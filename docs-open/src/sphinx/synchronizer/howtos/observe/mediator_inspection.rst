..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _mediator-inspect:

Mediator inspection
===================

The Mediator inspection provides access to metadata associated with finalized transactions, also known as verdicts,
to give insights on the transactions completed on a Synchronizer.
This page describes how to obtain the verdicts from the admin console.

Obtain verdicts from the mediator
---------------------------------

Use the `verdicts` admin command to inspect verdicts:

.. snippet:: mediator_inspection_admin_api
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1, mediator1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: participant1.health.ping(participant1)
    .. success(output=0):: import com.digitalasset.canton.data.CantonTimestamp
    .. success:: mediator1.inspection.verdicts(fromRecordTimeOfRequestExclusive = CantonTimestamp.MinValue, maxItems = 1)

The command requires a starting record time (exclusive) and a maximum number of verdicts to list.
`CantonTimestamp.MinValue` can be used to obtain all verdicts from the beginning.

The output is a list of verdicts. Each verdict contains, among others, its result (accepted, rejected, unspecified),
a submitting participant, submitting parties, a finalization time, a record time of the corresponding confirmation request,
and metadata of transaction views.

.. todo::
    #. Link the reference documentation. <https://github.com/DACH-NY/canton/issues/25832>
    #. Link the Admin API Protobuf reference docs once available. <https://github.com/DACH-NY/canton/issues/26126>

For more details on the `verdicts` admin command, check the reference documentation.

To learn more about related concepts, see :brokenref:`the Mediator overview<mediator-overview>`.
