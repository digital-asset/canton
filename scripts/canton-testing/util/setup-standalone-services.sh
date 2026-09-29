#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# When SBT kicks off a test that uses UsePostgres and UseToxiproxy
# we don't need to manage db replicas for performance testing
# but standalone barebone services.
#
# These setup functions can be used for:
# - creating a single postgres docker container without namespace  
# - creating a single toxiproxy server either in container or as an OS service
# - tearing down these services
#
# Note: These services are using their standard ports by default
###############################################################################

check-port-available() {
	local port="$1"
	local service_name="$2"

	if (echo > /dev/tcp/127.0.0.1/"$port") 2>/dev/null || \
	   command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 "$port" 2>/dev/null || \
	   command -v lsof >/dev/null 2>&1 && lsof -i :"$port" >/dev/null 2>&1; then
		echo -e " \033[31mFailed!\033[0m" >&2
		echo "[ERROR] Port $port is already in use by another process. Cannot start $service_name." >&2
		return 1
	fi
	return 0
}

teardown-postgres() {
	echo -n "Tearing down docker postgres service..."
	docker rm -f "${CURRENT_JOB_NAME:-canton}-postgres" 2>/dev/null || true
	echo -e " \033[32mdone\033[0m"
}

teardown-toxiproxy() {
	echo -n "Tearing down local toxiproxy service..."
	if [[ "$OSTYPE" == "darwin"* ]]; then
		killall toxiproxy-server 2>/dev/null || true
	else
		docker rm -f "${CURRENT_JOB_NAME:-canton}-toxiproxy" 2>/dev/null || true
	fi
	echo -e " \033[32mdone\033[0m"
}

setup-postgres() {
	local container_name="${CURRENT_JOB_NAME:-canton}-postgres"
	local port="${POSTGRES_PORT:-5432}"
	local max_retries=60 # 60 * 0.5s = 30s max timeout
	local count=0

	docker rm -f "$container_name" 2>/dev/null || true

	echo -n "Checking port $port availability "
	if ! check-port-available "$port" "Postgres"; then
		return 1
	fi
	echo -e " \033[32mdone\033[0m"

	echo "Start standalone postgres container on port $port..."
	docker run --rm -d \
	  --name "$container_name" \
	  -p "${port}:5432" \
	  -e POSTGRES_USER="${POSTGRES_USER:-postgres}" \
	  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-supersafe}" \
	  -e POSTGRES_DB="${POSTGRES_DB:-postgres}" \
	  postgres:17 >/dev/null 2>&1

	echo -n "Waiting for Postgres initialization "
	until docker exec "$container_name" pg_isready -U postgres >/dev/null 2>&1; do
		if ! docker ps -q --filter "name=^/${container_name}$" | grep -q . || [ "$count" -ge "$max_retries" ]; then
			echo -e " \033[31mfailed!\033[0m"
			echo "[ERROR] Postgres container '$container_name' failed to initialize." >&2
			echo "===== Container Logs =====" >&2
			docker logs "$container_name" 2>&1 || true
			echo "==========================" >&2
			return 1
		fi
		echo -n "."
		count=$((count + 1))
		sleep 0.5
	done
	echo -e " \033[32mdone\033[0m"
}

setup-toxiproxy() {
	local container_name="${CURRENT_JOB_NAME:-canton}-toxiproxy"
	local max_retries=100 # 100 * 0.2s = 20 seconds max timeout
	local count=0
	local TOXIPROXY_PORT="${TOXIPROXY_PORT:-8474}"

	docker rm -f "$container_name" 2>/dev/null || true

	echo -n "Checking port $TOXIPROXY_PORT availability "
    if ! check-port-available "$TOXIPROXY_PORT" "Toxiproxy"; then
        return 1
    fi
    echo -e " \033[32mdone\033[0m"

	if [[ "$OSTYPE" == "darwin"* ]]; then
		if ! brew list toxiproxy >/dev/null 2>&1; then
			echo "MacOS detected: please install toxiproxy (brew install toxiproxy)"
			return 1
		fi
		echo "MacOS detected: Start standalone toxiproxy-server on port ${TOXIPROXY_PORT}..."
		toxiproxy-server -host 0.0.0.0 -port "${TOXIPROXY_PORT}" >/dev/null 2>&1 &
	else
		echo "Start standalone toxiproxy-server container on port ${TOXIPROXY_PORT}..."
		docker run -d --rm \
		  --name "$container_name" \
		  --net=host \
		  ghcr.io/shopify/toxiproxy:2.1.5 \
		  -host 0.0.0.0 -port "${TOXIPROXY_PORT}" >/dev/null 2>&1
	fi

	echo -n "Waiting for Toxiproxy server initialization "
	until curl -s http://127.0.0.1:${TOXIPROXY_PORT}/version >/dev/null; do
		if [ "$count" -ge "$max_retries" ]; then
			echo -e " \033[31mfailed!\033[0m"
			echo "[ERROR] Toxiproxy server failed to respond on 127.0.0.1:${TOXIPROXY_PORT} after 20 seconds." >&2
			if [[ "$OSTYPE" != "darwin"* ]]; then
				echo "===== Container Logs =====" >&2
				docker logs "$container_name" 2>&1 || true
				echo "==========================" >&2
			fi
			return 1
		fi
		echo -n "."
		count=$((count + 1))
		sleep 0.2
	done
	echo -e " \033[32mdone\033[0m"
}

cleanup() {
	echo "Tearing down standalone containers..."
	teardown-postgres
	teardown-toxiproxy
}

setup-standalone-services() {
	setup-postgres
	setup-toxiproxy
}