..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Link to participant docs for general troubleshooting guidelines.

.. _synchronizer-troubleshoot:

Synchronizer Troubleshooting
============================

.. _synchronizer-troubleshoot-new-sequencer-onboarding:

Sequencer subscriptions fail for newly onboarded sequencers
-----------------------------------------------------------

Newly onboarded Sequencers only serve events more recent than the "onboarding snapshot" taken during the onboarding. In addition, some events may belong to transactions initiated before
a Sequencer was onboarded, but the Sequencer is not in a position to sign such events and replaces them with "tombstones". If a participant (or mediator)
connects to a newly onboarded Sequencer too soon and the subscription encounters a tombstone, the Sequencer subscription aborts with a `FAILED_PRECONDITION` error
specifying `InvalidCounter` or `SEQUENCER_TOMBSTONE_ENCOUNTERED`. If this occurs, the participant or mediator should connect to another Sequencer with a longer
history of sequenced events before switching to the newly onboarded Sequencer.

.. _synchronizer-troubleshoot-generate-member-auth-token:

Generating authentication token for a member
--------------------------------------------

Most services exposed by the sequencer require an authentication token to be provided with the request. Participant and mediator nodes manage this token internally
and transparently, but it may be useful to obtain such token to perform troubleshooting tasks. Authentication tokens can be generated on the sequencer admin API for such purpose.
As a prerequisite, the ``enable-testing-commands`` config flag must be enabled:

.. code-block::

    canton.features.enable-testing-commands = yes

.. important::

    This feature allows to generate authentication tokens for any member of the synchronizer and use them to authenticate on the API on behalf of this member on the sequencer.
    Use with care and do NOT share this token.

To generate the token, we'll need the ID of the node for which the token will be valid. For example:

.. snippet:: synchronizer_troubleshoot_auth_token
    .. hidden:: bootstrap.synchronizer(
      synchronizerName = "global",
      sequencers = Seq(sequencer1),
      mediators = Seq(mediator1),
      synchronizerOwners = Seq(sequencer1),
      synchronizerThreshold = 1,
      staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
    )
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. success:: val memberId = participant1.id.toProtoPrimitive
    .. success:: val token = sequencer1.authentication.generate_authentication_token(participant1, expiresIn = Some(1.minute))
    .. success:: val tokenAsBase64 = token.tokenAsBase64

The expiration argument is optional, and will default to the maximum validity duration configured on the sequencer.

With the token in hand one can make a call to the gRPC sequencer API directly.
Before that we'll also need the synchronizer ID to which the sequencer belongs to:

.. snippet:: synchronizer_troubleshoot_auth_token
    .. success:: val synchronizerId = sequencer1.physical_synchronizer_id.toProtoPrimitive

Write the data down to temporary files to use from a shell CLI:

.. snippet:: synchronizer_troubleshoot_auth_token
    .. success:: val tmpDir = better.files.File("/tmp/troubleshoot/auth_token").createDirectories()
    .. success:: (tmpDir / "memberId").createFileIfNotExists().write(memberId)
    .. success:: (tmpDir / "synchronizerId").createFileIfNotExists().write(synchronizerId)
    .. success:: (tmpDir / "token").createFileIfNotExists().write(tokenAsBase64)
    .. success:: (tmpDir / "sequencer_address").createFileIfNotExists().write(sequencer1.config.publicApi.address)
    .. success:: (tmpDir / "sequencer_port").createFileIfNotExists().write(sequencer1.config.publicApi.port.unwrap.toString)

Then for example, calling the ``DownloadTopologyStateForInit`` endpoint with grpcurl (replace the token, member ID and synchronizer ID with the appropriate values):

.. snippet:: synchronizer_troubleshoot_auth_token
    .. shell:: TMPDIR=$(echo "/tmp/troubleshoot/auth_token")
    .. shell:: grpcurl -plaintext -H "authToken-bin: $(cat $TMPDIR/token)" -H "memberId: $(cat $TMPDIR/memberId)" -H "synchronizerId: $(cat $TMPDIR/synchronizerId)" -d '{ "member": "'$(cat $TMPDIR/memberId)'"}' $(cat $TMPDIR/sequencer_address):$(cat $TMPDIR/sequencer_port) com.digitalasset.canton.sequencer.api.v30.SequencerService/DownloadTopologyStateForInit

A few things to note:

    * The token needs to be set as a ``authToken-bin`` header in Base64 encoding
    * The ID of the member needs to be set as a ``memberId`` header
    * The ID of the synchronizer needs to be set as a ``synchronizerId`` header

Finally, to invalidate the token, call the ``com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationService/Logout`` RPC or use the following command on the sequencer.

.. snippet:: synchronizer_troubleshoot_auth_token
    .. success:: sequencer1.authentication.logout(token.token)

Note that this will disconnect the member from the sequencer and may cause temporary failures while the sequencer connection / authentication is re-established.
