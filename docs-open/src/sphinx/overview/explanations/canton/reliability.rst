..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

###########
Reliability
###########

.. wip::

   Reliability = HA + DR

   Add links to subsites with configuration options and operational procedures

High Availability via Node Replication
**************************************

.. todo:: <https://github.com/DACH-NY/canton/issues/25669>

   Active-passive with shared DB for participant and mediator on centralized synchronizer

   Active-active for DB sequencer

   Note: BFT replication sufficient for decentralized synchronizers (no HA available right now for ordering service / block sequencer)

   Resilient sequencer connections (trust threshold + liveness margin)


Database replication
********************

.. todo:: <https://github.com/DACH-NY/canton/issues/25669>
   Backup and restore approaches for DR


High availability for Daml parties
**********************************

.. todo:: <https://github.com/DACH-NY/canton/issues/25669>
   Multi-host party for redundancy

   * Lower threshold = higher availability

   * Data recovery from any hosting participant
