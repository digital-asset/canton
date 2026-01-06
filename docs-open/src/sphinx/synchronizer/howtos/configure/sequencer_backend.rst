..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _sequencer-backend:

Configure Sequencer Backend
===========================

The following page describes the basics of configuring Sequencer backends. For more advanced configuration, refer to
the following sections:

    - :ref:`High availability<ha_sequencer>`
    - :ref:`Pruning<sequencer-pruning>`
    - :ref:`Optimization<optimize-sequencer>`

Database Sequencer
------------------

The Database Sequencer is currently unsupported and should not be configured.

BFT Sequencer
-------------

Minimal BFT Sequencer backend configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use the Byzantine Fault Tolerant (BFT) Sequencer, set the `type` parameter to `BFT` under `sequencer`:

.. literalinclude:: CANTON/community/app/src/pack/examples/11-bft-sequencer/minimal.conf
   :language: scala
   :start-after: user-manual-entry-begin: BftSequencerMinimalConfig
   :end-before: user-manual-entry-end: BftSequencerMinimalConfig
   :dedent:

Note that this configuration is for single-node networks; you cannot add peers.
For multi-node networks, refer to the :ref:`next<sequencer-backend-bft-initial-peers>` section.

.. _sequencer-backend-bft-initial-peers:

Configure initial peers (optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the network contains more than one node, configure the server endpoint under `initial-network`. There, you can also
configure peer endpoints, or do so :ref:`using admin commands<dynamically_adding_sequencers>` later.
Pre-configuring the peer endpoints reduces the number of manual configuration steps, which saves time, reduces errors,
and accelerates the deployment process.


.. literalinclude:: CANTON/community/app/src/pack/examples/11-bft-sequencer/two-peers.conf
   :language: scala
   :start-after: user-manual-entry-begin: BftSequencerInitialPeersConfig
   :end-before: user-manual-entry-end: BftSequencerInitialPeersConfig
   :dedent:

The `address` and `port` pair under `server-endpoint` make up an endpoint where the BFT Sequencer's gRPC server listens.
If the `port` is not specified, the operating system chooses one for you. The `external-address` and `external-port`
form an externally available endpoint that other peers can connect to. Usually, the `external-address` is the domain
name of a reverse proxy that points to the listening endpoint. The external endpoint must be configured correctly
for clients to successfully authenticate the server.

Transport Layer Security (TLS) is enabled by default for the external endpoint and peer endpoints.
For simplicity, it is disabled in the config example. However, it is recommended to configure it in
the :externalref:`standard way<tls-configuration>` with:

    - `tls` under `server-endpoint` for the internal server endpoint
    - `external-tls-config` under `server-endpoint` for the external endpoint
    - `tls-config` under `peer-endpoints` for peer endpoints

.. todo::
    #. Link the reference documentation. <https://github.com/DACH-NY/canton/issues/26126>

For more details on the `initial-network` configuration, check the reference documentation.

Configure authentication (optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Authentication is enabled by default. It can be configured with `endpoint-authentication` under `initial-network`
(at the same level as `server-endpoint`):

.. literalinclude:: CANTON/community/app/src/pack/examples/11-bft-sequencer/two-peers.conf
   :language: scala
   :start-after: user-manual-entry-begin: BftSequencerEndpointAuthentication
   :end-before: user-manual-entry-end: BftSequencerEndpointAuthentication
   :dedent:

For more details on authentication, visit the :ref:`secure Synchronizer page<secure-synchronizer-apis>`.

Configure dedicated storage (optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use dedicated storage, configure `storage` under `config` as for the top-level
:ref:`Sequencer storage configuration<sync-storage-config>`.

While the BFT Sequencer defaults to using the top-level Sequencer storage, configuring dedicated storage offers
greater flexibility. It allows you to:

    - Support distinct data read and write patterns with different database backends and settings
    - Separate :ref:`backups<synchronizer-backup-and-restore>`

Configure network-wide parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you change the defaults, keep the following parameters in sync across the network:

    - `epoch-length`: a length of all epochs
    - `max-requests-in-batch`: a maximum number of requests in a batch, validated at runtime
    - `max-batches-per-block-proposal`: a maximum number of batches per block proposal, validated at runtime

All the above parameters reside under `config` and must be the same across all BFT Orderer nodes.

For details on the related concepts, check the :ref:`BFT Orderer explanation page<bft-orderer-arch>`.

Configure other (local) parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are other (local) configuration parameters under `config` that can be changed.

.. todo::
    #. Link the reference documentation. <https://github.com/DACH-NY/canton/issues/26126>

For details, check the reference documentation.

External Sequencer
------------------

To use an external Sequencer (for example, CometBFT), configure the underlying Sequencer `type` under `config`
in the `sequencer` configuration:

.. code-block:: scala

    sequencer {
        config {
            cometbft-node-host = "127.0.0.1"
            cometbft-node-port = 26627
        }
        type = CometBFT
    }

Every external Sequencer requires a corresponding Sequencer Driver on the classpath.
