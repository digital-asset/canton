..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _optimize-orderer:

Optimize the BFT orderer
========================

Background
----------

The BFT orderer is a byzantine fault-tolerant component of the Sequencer responsible to order and timestamp
submission requests from Sequencer clients.

Refer to :externalref:`BFT Sequencer<bft-orderer-arch>` for an overview of the BFT Sequencer architecture.

System resources
----------------

Available system resources can affect the performance of the BFT orderer in multiple ways:

- Node resources:
  - CPU: affects general processing and particularly cryptographic operations.
  - Memory: affects general processing and particularly the ability of a node to cope with bigger networks
  - Disk I/O: affects the performance of an orderer's database operations.
  - Network adapter performance: affects general network throughput and latency.
- Network resources:
  - Network bandwidth: affects message dissemination times and can directly affect ordering latency.
  - Network latency: affects message propagation times and can directly affect ordering latency.

Dimensions
----------

The size of the network and the characteristics of the ordering requests can also affect performance and resource usage:

- Number of Sequencer Nodes: the ISS protocol scales in the number of nodes up to node saturation.
- Request Size: larger requests can lead to increased processing times, however the dissemination sub-protocol,
  when running on a high-bandwidth network, reduces the overall effect of request size up to node saturation.

Synchronizer parameters
-----------------------

Some general Synchronizer parameters can also affect the performance of the BFT orderer:

- Topology change delay: as for the Sequencer in general, bigger values reduces the chance of waiting
  for the topology processor to complete computing the requested topology snapshot.

Refer to :externalref:`Parameters configuration<parameters-configuration>` for more information on Synchronizer
parameters.

Configuration
-------------

Some configuration parameters can affect performance and resource usage of the BFT orderer and are discussed
below.

Refer to :externalref:`Sequencer backend configuration <sequencer-backend>` for general information
on how to configure the orderer.

- Epoch length: epoch transitions' overhead may be significant, because all nodes need to wait for the whole
  epoch's blocks to be complete to detect potential topology changes. Increasing the epoch length reduces the frequency
  of these transitions, at the cost of slower reaction to topology changes.
  This value must be the same across all orderer nodes and cannot be changed after Synchronizer bootstrap.
- Minimum and maximum requests in batches, maximum batch creation interval: larger and less frequent batches increase
  throughput, at the cost of increased latency.
  This value must be the same across all orderer nodes and cannot be changed after Synchronizer bootstrap.
- Maximum batches in blocks: larger block sizes increase throughput, at the cost of increased latency.
  This value must be the same across all orderer nodes and cannot be changed after Synchronizer bootstrap.
- Leader selection policy: by default, failing leaders are blacklisted for a certain period.
  Disabling blacklisting may improve performance in stable networks, at the cost of reduced performance and
  resilience during node and network instability.
  This value must be the same across all orderer nodes and cannot be changed after Synchronizer bootstrap.
- Consensus empty block creation timeout: the orderer creates empty blocks after a certain timeout
  in the absence of ordering traffic so that sequencing time does not lag too much behind median wall clock time.
  Reducing this timeout may make sequencing timestamps closer to wall clock time, at the cost of increased
  resource usage.
- Maximum mempool queue size: larger values reduce backpressure on clients submitting requests, at the cost of increased
  memory usage, load, and ordering latency.
- Maximum number of unordered batches per node: larger values allow peers to disseminate more data before it must be
  ordered, at the cost of additional storage and memory usage.
- Storage: using fast, dedicated storage for the orderers' databases improves overall performance.
- Batch aggregator: by default batch aggregation is enabled with a default configuration for
  all database insert operations. Tuning the batch aggregator's parameters may improve performance
  depending on the workload.
- Dedicated execution context divisor: if set, a dedicated execution context is created for the orderer node,
  with a thread pool size equal to the number of available processors divided by the divisor.
  Setting this parameter may improve performance under high load, at the cost of increased resource usage.

Security
--------

The maximum ordering request size configuration parameter does not affect performance but it has security relevance,
as it prevents non-compliant nodes from exhausting memory and processing resources of peers.
It must be the same across all orderer nodes and cannot be changed after Synchronizer bootstrap.

The maximum decompress size Synchronizer parameter is also security-relevant to prevent "zip bomb" attacks and
is also observed in the BFT orderer.

Refer to :externalref:`Parameters configuration<parameters-configuration>` for more information on Synchronizer
parameters.
