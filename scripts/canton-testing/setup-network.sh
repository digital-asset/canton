#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Configures network namespaces and interfaces for network monitoring.
# The topology is documented in virtual-network-topology.png.
#
# IMPORTANT ASSUMPTIONS:
# - The script is executed on Linux.
# - The IP addresses assigned by this script (see virtual-network-topology.png)
#   are not assigned to other devices.
#   (Otherwise, we get non-unique ip addresses.)
# - The machine is not directly connected to the internet.
#   (The default FORWARD policy is set to ACCEPT.)
###############################################################################

# If source and target address are in the same network namespace, the kernel will inevitable route the traffic through 'lo'.
# Therefore, traffic from different connections will be mixed and is difficult to monitor.
#
# Consequently, we setup separate network namespaces for
# - all synchronizers,
# - all participants, and
# reuse the default namespace for all performance runner.
# Additionally, we setup one veth interface pair for every Canton api (ledger api, admin api, public api, ...).
# The veth interface pair connects the synchronizers or participants namespace with the default namespace.
#
# As a result, we can monitor the network traffic of an individual api by monitoring the corresponding veth interface.

set -eu -o pipefail

# set to non zero if you want to just get the commands written to the screen
PRINT_ONLY=0
TEST_ONLY=0

# If an interface name starts with '_', the interface is not intended to be monitored.
internet_bridge="_br-inet"
internet_gateway_suffix="0.1"

postgres_bridge="_br-postgres"
postgres_gateway_suffix="1.1"

public_api_bridge="_br-public-api"
public_api_gateway_suffix="99.1"

default_ping_size=56
ping_cycles=1
ping_timeout=3

on-exit() {
  echo "Exit code: $?"
}

trap on-exit EXIT

test_cases=()

run() {
  if [[ $PRINT_ONLY -eq 0 && $TEST_ONLY -eq 0 ]]; then
    $@
  else
    echo $@
  fi
}

tst() {
  if [[ $PRINT_ONLY -eq 0 ]]; then
    $@
  else
    echo $@
  fi
}

test-connection() {
  local source="$1"
  local dest="$2"
  local namespace="${3:-}"
  local ping_size="${4:-$default_ping_size}"

  echo "* Testing $source -> $dest (from namespace ${3:-<default>}, size $ping_size)"

  cmd=(ping -W "$ping_timeout" -c "$ping_cycles" -s "$ping_size" -I "$source" "$dest")

  if [[ -n $namespace ]]; then
    tst sudo ip netns exec "$namespace" "${cmd[@]}"
  else
    tst "${cmd[@]}"
  fi
  echo
}

setup-bridge() {
  local name="$1"
  local ip="192.168.$2"

  echo
  echo "*** Creating bridge '$name' with ip $ip"
  echo "***************************************************"

  run sudo ip link del "$name" &> /dev/null || true
  run sudo ip link add "$name" type bridge

  run sudo ip addr add "$ip/24" dev "$name"

  run sudo ip link set "$name" up

  test_cases+=("test-connection $ip $ip")
}

setup-bridge "$internet_bridge" "$internet_gateway_suffix"

# Setup NAT for the private IP ranges of the virtual ethernet devices.
# This makes sure that responses from the internet are routed back to the right device.
run sudo iptables -t nat -C POSTROUTING -s "192.168.0.0/24" -j MASQUERADE &> /dev/null ||
run sudo iptables -t nat -A POSTROUTING -s "192.168.0.0/24" -j MASQUERADE

run sudo iptables -t filter -P FORWARD ACCEPT

setup-bridge "$postgres_bridge" "$postgres_gateway_suffix"

setup-bridge "$public_api_bridge" "$public_api_gateway_suffix"

create-veth() {
  local namespace="$1"
  local name="$2"
  local peer_name="_$2"
  local ip="192.168.$3"
  local gateway="192.168.$4"
  local bridge="${5:-}"

  echo
  echo "*** Creating veth '$name' with ips '$ip@$namespace', gw '$gateway@${5:-<default>}, peer ${peer_name} "
  echo "***************************************************"


  # Create interface pair
  run sudo ip link add "$name" type veth peer name "$peer_name"
  run sudo ip link set "$peer_name" netns "$namespace"

  # Configure ip addresses / connect to bridge
  run sudo ip netns exec "$namespace" ip addr add "$ip/24" dev "$peer_name"
  if [[ -n $bridge ]]; then
    run sudo ip link set "$name" master "$bridge"
  else
    run sudo ip addr add "$gateway/24" dev "$name"
  fi

  # Activate interface pair
  run sudo ip link set "$name" up
  run sudo ip netns exec "$namespace" ip link set "$peer_name" up

  # Ping outside -> inside
  test_cases+=("test-connection $gateway $ip")
  # Ping inside -> inside
  test_cases+=("test-connection $ip $ip $namespace")
  # Ping outside -> outside
  test_cases+=("test-connection $gateway $gateway")
  # Ping inside -> outside
  test_cases+=("test-connection $ip $gateway $namespace")
}

create-namespace() {
  local name="$1"
  local veth_name="$2"
  local veth_ip="$3"

  echo
  echo "*** Creating namespace '$name'..."
  echo "***************************************************"

  # Create namespace
  run sudo ip netns delete "$name" &> /dev/null || true
  run sudo ip netns add "$name"

  # Enable ip traffic inside of ns.
  run sudo ip netns exec "$name" ip link set lo up

  # Enable traffic to the internet
  create-veth "$name" "$veth_name" "$veth_ip" "$internet_gateway_suffix" "$internet_bridge"

  run sudo ip netns exec "$name" ip route add default via "192.168.$internet_gateway_suffix"

  # Ping the internet
  test_cases+=("test-connection 192.168.$veth_ip 8.8.8.8 $name 56")
}

create-namespace "synchronizers" "sync-inet" "0.2"
create-veth "synchronizers" "sync-postgres" "1.2" "$postgres_gateway_suffix" "$postgres_bridge"
create-veth "synchronizers" "da-admin-api" "100.2" "100.1"
create-veth "synchronizers" "da-public-api" "99.2" "$public_api_gateway_suffix" "$public_api_bridge"

create-namespace "participants" "par-inet" "0.3"
create-veth "participants" "par-postgres" "1.3" "$postgres_gateway_suffix" "$postgres_bridge"
create-veth "participants" "p1-admin-api" "10.2" "10.1"
create-veth "participants" "p1-ledger-api" "11.2" "11.1"
create-veth "participants" "p1-public-api" "99.3" "$public_api_gateway_suffix" "$public_api_bridge"

echo
echo "*** Testing connectivity..."
echo "***************************************************"

for test_case in "${test_cases[@]}"; do
  $test_case
done
