..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-extension-synchronizers-linking-validator-multi-sync:

Canton Network Documentation Snippets: Extension Synchronizers Linking Validator Multi Sync
============================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_extension_synchronizers_linking_validator_multi_sync
    .. success:: bootstrap.synchronizer(synchronizerName = "private-sync", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
    .. hidden:: val participant = participant1
    .. hidden:: val participantReference: com.digitalasset.canton.console.ParticipantReference = participant1
    .. hidden:: val sequencerReference = sequencer1
    .. success:: participant.synchronizers.connect_local(sequencer1, "private-sync")

.. snippet:: cn_global_synchronizer_extension_synchronizers_linking_validator_multi_sync
    .. success:: participant.synchronizers.list_connected()

.. snippet:: cn_global_synchronizer_extension_synchronizers_linking_validator_multi_sync
    .. success:: participant.synchronizers.list_connected()
    .. success:: participant.synchronizers.list_registered()

.. snippet:: cn_global_synchronizer_extension_synchronizers_linking_validator_multi_sync
    .. success:: participant.synchronizers.disconnect("private-sync")

.. snippet:: cn_global_synchronizer_extension_synchronizers_linking_validator_multi_sync
    .. success:: participant.synchronizers.reconnect("private-sync")

.. snippet:: cn_global_synchronizer_extension_synchronizers_linking_validator_multi_sync
    .. hidden:: val synchronizerAlias = "private-sync"
    .. success:: val sequencerConnectionValidation = com.digitalasset.canton.admin.api.client.data.SequencerConnectionValidation.Active
    .. success:: participantReference.synchronizers.connect_local(sequencerReference, "private-sync", validation = sequencerConnectionValidation)
    .. hidden:: val port = sequencer1.config.publicApi.port
    .. hidden:: val sequencerAddress = "127.0.0.1"
    .. success:: val sequencerUrl = s"http://${sequencerAddress}:${port}"
    .. success:: participantReference.synchronizers.connect("private-sync", connection = sequencerUrl)
    .. hidden:: val sequencer1Address = sequencer1.config.publicApi.address
    .. hidden:: val sequencer2Address = sequencer1.config.publicApi.address
    .. hidden:: val sequencer3Address = sequencer1.config.publicApi.address
    .. hidden:: val sequencer1Port = sequencer1.config.publicApi.port
    .. hidden:: val sequencer2Port = sequencer1.config.publicApi.port
    .. hidden:: val sequencer3Port = sequencer1.config.publicApi.port
    .. success:: val sequencer1Url = s"http://${sequencer1Address}:${sequencer1Port}"
    .. success:: val sequencer2Url = s"http://${sequencer2Address}:${sequencer2Port}"
    .. success:: val sequencer3Url = s"http://${sequencer3Address}:${sequencer3Port}"
    .. success:: val connections = Seq(
         GrpcSequencerConnection.tryCreate(sequencer1Url, sequencerAlias = "sequencer1"),
         GrpcSequencerConnection.tryCreate(sequencer2Url, sequencerAlias = "sequencer2"),
         GrpcSequencerConnection.tryCreate(sequencer3Url, sequencerAlias = "sequencer3"),
       )
    .. success:: val sequencerTrustThreshold = 2
    .. success:: val sequencerLivenessMargin = 1
    .. success:: val submissionRequestAmplification = SubmissionRequestAmplification(factor = 2, patience = 0.seconds)
    .. success:: participantReference.synchronizers.connect_bft(
         connections,
         "private-sync",
         sequencerTrustThreshold = sequencerTrustThreshold,
         sequencerLivenessMargin = sequencerLivenessMargin,
         submissionRequestAmplification = submissionRequestAmplification,
       )
    .. success:: participantReference.synchronizers.list_registered()
    .. success:: participantReference.synchronizers.list_connected()
    .. success:: participantReference.synchronizers.config("private-sync")
    .. success:: participantReference.synchronizers.modify("private-sync", _.tryWithSequencerTrustThreshold(1))
    .. success:: participantReference.synchronizers.config("private-sync")
    .. success:: participantReference.synchronizers.modify("private-sync", _.withSequencerLivenessMargin(0))
    .. success:: participantReference.synchronizers.config("private-sync")
    .. success:: participantReference.synchronizers.modify("private-sync", _.withSubmissionRequestAmplification(SubmissionRequestAmplification.NoAmplification))
    .. success:: participantReference.synchronizers.modify("private-sync", _.withPriority(0))
    .. success:: val connection = connections.head
    .. success:: participantReference.synchronizers.modify("private-sync", _.copy(sequencerConnections=SequencerConnections.single(connection)))
    .. success:: participantReference.synchronizers.disconnect("private-sync")
    .. success:: participantReference.synchronizers.list_connected().isEmpty
    .. success:: participantReference.synchronizers.reconnect("private-sync")
    .. success:: participantReference.synchronizers.reconnect_all()
