..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _synchronizer-connections:

Synchronizer connections
========================

A Participant Node connects to the Synchronizer by connecting to one or many Sequencers of this Synchronizer.

A Participant Node can connect to multiple Synchronizers at once. The :ref:`synchronizer connectivity commands <participant_synchronizer_connectivity>`
allow the administrator of a Participant Node to manage connectivity to Synchronizers.

The following sections explain how to manage such Sequencer connections for a Participant Node.

.. snippet:: participant_connectivity
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1, sequencer2, sequencer3),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1, sequencer2, sequencer3),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )

Connect
-------

.. _connectivity_participant_connect_local:

Connect to a Sequencer by Reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To connect a Participant Node to a Sequencer by reference (local or :ref:`remote reference <canton_remote_console>`), follow the steps below.

1. Define a Synchronizer alias. An alias is a name chosen by the Participant's operator to manage the connection. For example:

.. snippet:: participant_connectivity
    .. success:: val synchronizerAlias = "mysynchronizer"


2. Set Optional Sequencer Connection Validation. This parameter determines how thoroughly the provided Sequencer connections
are validated before being persisted.

.. TODO(#25904): Add link to the reference doc of ``SequencerConnectionValidation``

.. snippet:: participant_connectivity
    .. success:: val sequencerConnectionValidation = SequencerConnectionValidation.Active

3. Execute the ``connect_local`` command:

.. snippet:: participant_connectivity
    .. hidden:: val sequencerReference = sequencer1
    .. hidden::  val participantReference: com.digitalasset.canton.console.ParticipantReference = participant1
    .. success:: participantReference.synchronizers.connect_local(sequencerReference, synchronizerAlias, validation = sequencerConnectionValidation)
    .. assert:: { participantReference.health.ping(participantReference); true }
    .. hidden::  participantReference.synchronizers.disconnect_all()
    .. hidden::  val participantReference: com.digitalasset.canton.console.ParticipantReference = participant2

4. List the connected Synchronizers to verify that the connection is listed. See :ref:`inspect connections <connectivity_participant_inspect_connections>`.

.. _connectivity_participant_connect_remote:

Connect to a Sequencer by URL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To connect to a remote Sequencer:

1. Obtain the Sequencer address and port from the Synchronizer Operator.

.. snippet:: participant_connectivity
    .. hidden:: val port = sequencer3.config.publicApi.port
    .. hidden:: val sequencerAddress = "127.0.0.1"
    .. success:: val sequencerUrl = s"https://${sequencerAddress}:${port}"

2. Provide custom certificates (if necessary). If the Sequencer uses TLS certificates
that cannot be automatically validated using your JVM's trust store (for example, self-signed certificates), you have to provide a custom certificate
such that the client can verify the Sequencer's public API TLS certificate. The operate must trust the custom root CA. Let's assume that this root certificate is given by:

.. snippet:: participant_connectivity
    .. success:: val certificatesPath = "tls/root-ca.crt"

3. Connect the Participant to the Sequencer using the ``connect`` console command.

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.connect("mysynchronizer", connection = sequencerUrl, certificatesPath = certificatesPath)
    .. assert:: { participantReference.health.ping(participantReference); true }

4. List the connected Synchronizers to verify that the connection is listed. See :ref:`inspect connections <connectivity_participant_inspect_connections>`.


.. _connectivity_participant_connect_bft:

Connect to decentralized Sequencers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enhance reliability and security, you can connect to multiple Sequencers of the same Synchronizer using the ``connect_bft`` command.

1. Create the URL for each Sequencer by referencing its public API address and port.

.. snippet:: participant_connectivity
    .. hidden:: participantReference.synchronizers.disconnect_all()
    .. hidden::  val participantReference: com.digitalasset.canton.console.ParticipantReference = participant3
    .. hidden:: val sequencer1Address = sequencer1.config.publicApi.address
    .. hidden:: val sequencer2Address = sequencer2.config.publicApi.address
    .. hidden:: val sequencer3Address = sequencer3.config.publicApi.address
    .. hidden:: val sequencer1Port = sequencer1.config.publicApi.port
    .. hidden:: val sequencer2Port = sequencer2.config.publicApi.port
    .. hidden:: val sequencer3Port = sequencer3.config.publicApi.port
    .. success:: val sequencer1Url = s"http://${sequencer1Address}:${sequencer1Port}"
    .. success:: val sequencer2Url = s"http://${sequencer2Address}:${sequencer2Port}"
    .. success:: val sequencer3Url = s"https://${sequencer3Address}:${sequencer3Port}"
    .. success:: val sequencer3Certificate = com.digitalasset.canton.util.BinaryFileUtil.tryReadByteStringFromFile(certificatesPath)

2. Create the Sequencer connections.

.. snippet:: participant_connectivity
    .. success:: val connections = Seq(
         GrpcSequencerConnection.tryCreate(sequencer1Url, sequencerAlias = "sequencer1"),
         GrpcSequencerConnection.tryCreate(sequencer2Url, sequencerAlias = "sequencer2"),
         GrpcSequencerConnection.tryCreate(sequencer3Url, sequencerAlias = "sequencer3", customTrustCertificates = Some(sequencer3Certificate)),
       )


3. Configure the Sequencer trust threshold by setting the minimum number of Sequencers that must agree before a message is considered valid.

- Default: 1
- Maximum: Number of connections

For example, setting the threshold to 2:

.. TODO(#25904) Add link to the explanation of the sequencer trust threshold.

.. snippet:: participant_connectivity
    .. success:: val sequencerTrustThreshold = 2

4. [Specific to the connection pool] Configure the Sequencer liveness margin; this sets the number of Sequencer connections, in addition to ``sequencerTrustThreshold``-many, from which we attempt to read messages.

- Default: 0
- Higher values increase the resilience to Sequencer delays or failures, but having (``sequencerTrustThreshold`` + ``sequencerLivenessMargin`` > number of connections) provides no benefit.

For example, setting the liveness margin to 1:

.. snippet:: participant_connectivity
    .. success:: val sequencerLivenessMargin = 1

.. _configure_request_amplification:

5. Configure submission request amplification: define how often the client should try to send a submission request that is eligible for deduplication.

- Higher values increase the chance of a submission request being accepted by the system, but also increase the load on the Sequencers.
- Default: ``SubmissionRequestAmplification.NoAmplification``

.. TODO(#25904) Add link to the refence doc of ``SubmissionRequestAmplification`` and the explanation.

In this example, I want to ensure that submission requests are sent to two Sequencers. Therefore, I set the amplification factor to 2 and the patience to 0 seconds.

.. snippet:: participant_connectivity
    .. success:: val submissionRequestAmplification = SubmissionRequestAmplification(factor = 2, patience = 0.seconds)

6. Connect using the ``connect_bft`` command.

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.connect_bft(
         connections,
         synchronizerAlias,
         sequencerTrustThreshold = sequencerTrustThreshold,
         sequencerLivenessMargin = sequencerLivenessMargin,
         submissionRequestAmplification = submissionRequestAmplification,
       )
    .. success:: participantReference.synchronizers.list_registered()
    .. assert:: { participantReference.health.ping(participantReference); true }

7. List the connected Synchronizers to verify that the connection is listed. See :ref:`inspect connections <connectivity_participant_inspect_connections>`.


.. _connectivity_participant_inspect_connections:

Inspect connections
-------------------

You can inspect the connected Synchronizer connections, as well as the configuration of a specific connection.

1. List connected Synchronizer connections. You can get the Synchronizer aliases and Synchronizer IDs of all connected Synchronizers:

.. snippet:: participant_connectivity
    .. hidden:: participantReference.synchronizers.connect_local(sequencerReference, synchronizerAlias)
    .. success:: participantReference.synchronizers.list_connected()
    .. assert::  participantReference.synchronizers.list_connected().length == 1

And you can inspect the configuration of a specific Synchronizer connection using:

2. Inspect the configuration of a specific connection:

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.config("mysynchronizer")


Modify
------

.. TODO(#25904) Add a section on how to modify the Sequencer connection.

Update Sequencer trust threshold
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You can modify the Sequencer trust threshold to control how many Sequencers must agree before a message is considered valid.

.. TODO(#25904) Add link to the explanation of the Sequencer trust threshold.

1. Define a valid threshold. Set the threshold value between 1 and the number of connected Sequencers.

.. snippet:: participant_connectivity
    .. success::  participantReference.synchronizers.modify("mysynchronizer", _.tryWithSequencerTrustThreshold(1))

2. Verify the updated configuration using:

.. snippet:: participant_connectivity
    .. success::  participantReference.synchronizers.config("mysynchronizer")


[Specific to the connection pool] Update Sequencer liveness margin
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You can modify the Sequencer liveness margin to control the resilience to faulty Sequencers.

1. Update the configuration with the new liveness margin:

.. snippet:: participant_connectivity
    .. success::  participantReference.synchronizers.modify("mysynchronizer", _.withSequencerLivenessMargin(0))

2. Verify the updated configuration using:

.. snippet:: participant_connectivity
    .. success::  participantReference.synchronizers.config("mysynchronizer")


Update request amplification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Configure submission request amplification. Amplification makes Sequencer clients send
eligible submission requests to multiple Sequencers to overcome message loss in faulty Sequencers.

.. snippet:: participant_connectivity
    .. success::  participantReference.synchronizers.modify("mysynchronizer", _.withSubmissionRequestAmplification(SubmissionRequestAmplification.NoAmplification))

2. Same as above, verify the updated configuration using ``config`` command.


Synchronizer priority
^^^^^^^^^^^^^^^^^^^^^
When connected to multiple Synchronizers, transaction routing will select the Synchronizer with the highest priority
for submitting transactions. You can adjust the priority of a Synchronizer to control which one is preferred
for submissions. For more details, refer to the :externalref:`multiple synchronizer documentation <multiple-synchronizers>`

1. Update the priority of the Synchronizer connection.

.. snippet:: participant_connectivity
    .. success::  participantReference.synchronizers.modify("mysynchronizer", _.withPriority(0))

2. Same as above, verify the updated configuration using ``config`` command.

.. _connectivity_participant_modify:


Update a custom TLS trust certificate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. TODO(#25904) Fix this section once the issue with the sequencer alias is resolved.

Whenever the root of trust changes, the clients need to update the custom root certificate accordingly.
To update a custom TLS trust certificate, particularly when using self-signed certificates as the root of trust for a TLS Sequencer connection, follow these steps:

1. Load the root certificate from a file:

.. snippet:: participant_connectivity
    .. success:: val certificate = com.digitalasset.canton.util.BinaryFileUtil.tryReadByteStringFromFile("tls/root-ca.crt")

2. Create a new connection instance and pass the certificate into this new connection.

.. snippet:: participant_connectivity
    .. success:: val connection = GrpcSequencerConnection.tryCreate(sequencerUrl, customTrustCertificates = Some(certificate))

3. Update the Sequencer connection settings on the Participant Node:

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.modify("mysynchronizer", _.copy(sequencerConnections=SequencerConnections.single(connection)))

For Mediators, update the certificate settings using a similar approach.


.. _connectivity_participant_reconnect:

Disconnect and reconnect
------------------------

1. Disconnect from the Synchronizer by using the ``disconnect`` command with the Synchronizer alias:

.. snippet:: participant_connectivity
    .. assert:: participantReference.synchronizers.list_connected().length == 1
    .. success:: participantReference.synchronizers.disconnect("mysynchronizer")
    .. assert:: participantReference.synchronizers.list_connected().isEmpty

2. Verify the disconnection:

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.list_connected().isEmpty

3. Reconnect to a specific Synchronizer using the ``reconnect`` command with the Synchronizer alias:

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.reconnect("mysynchronizer")

4. To reconnect all Synchronizers that are not configured to require a manual connection,
use the following command:

.. snippet:: participant_connectivity
    .. success:: participantReference.synchronizers.reconnect_all()
