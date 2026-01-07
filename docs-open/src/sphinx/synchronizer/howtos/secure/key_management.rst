..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _synchronizer-key-management:

Crypto Key Management
=====================

For a Synchronizer, which includes both Sequencer and Mediator nodes, key management works similarly to that
of a :externalref:`Participant node <key-management>`. Just ensure that ``myNode`` refers to the correct Sequencer or
Mediator. **The root namespace key for Synchronizer nodes can only be rotated by deploying a new Synchronizer**.
Please note that Sequencers and Mediators do not have an encryption key, since they do not need to encrypt view
messages.

If you wish to use a Key Management Service (KMS) to encrypt a Sequencer or Mediator's private keys at rest or to store
and manage them directly in a KMS, you can follow the operational procedures described in the
:externalref:`participant KMS section <kms>`. The configuration process is similar for both Participant and
Synchronizer nodes.

