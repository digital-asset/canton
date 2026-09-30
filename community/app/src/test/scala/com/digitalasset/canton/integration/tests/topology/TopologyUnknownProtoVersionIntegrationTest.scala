// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MessageId,
  Recipients,
  SignedContent,
  SubmissionRequest,
  TopologyBroadcastAddress,
}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{Namespace, PartyId}
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.nonempty.NonEmpty
import org.slf4j.event.Level

/** In versions prior to 3.6, we had the following bug: if an UntypedVersionedMessage specified an
  * unknown proto version, then the versioning tooling picked the highest known deserializer instead
  * of failing.
  *
  * Because the topology state of MainNet includes a topology transaction with proto version 29, we
  * cannot fail in all cases. Hence, we do:
  *   - Pick the smallest known deserializer for topology transaction if the proto version is lower
  *     than the lowest known proto version.
  *   - Fail otherwise.
  */
final class TopologyUnknownProtoVersionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, daName)
    }

  /** Creates a party-to-participant transaction for a new party hosted on participant1, whose
    * serialization uses `protoVersion` in the version wrapper.
    */
  private def partyToParticipant(partyName: String, protoVersion: Int)(implicit
      env: TestConsoleEnvironment
  ): (PartyId, TopologyTransaction[TopologyChangeOp, TopologyMapping]) = {
    import env.*

    val party = PartyId.tryCreate(partyName, Namespace(participant1.fingerprint))
    val mapping = PartyToParticipant.tryCreate(
      party,
      PositiveInt.one,
      Seq(HostingParticipant(participant1.id, ParticipantPermission.Submission)),
      isOffline = false,
    )
    val transactionP = TopologyTransaction
      .tryCreate(TopologyChangeOp.Replace, PositiveInt.one, mapping, testedProtocolVersion)
      .toProtoV30
      .value

    val originalBytes = UntypedVersionedMessage(
      UntypedVersionedMessage.Wrapper.Data(transactionP.toByteString),
      protoVersion,
    ).toByteString

    // Use the deserializer of proto version 30 directly: the resulting transaction memoizes
    // `originalBytes`, even if no deserializer exists for `protoVersion`
    val transaction = TopologyTransaction.versioningTable
      .deserializerFor(ProtoVersion(30))
      .value(ProtocolVersionValidation.NoValidation, (), originalBytes, transactionP.toByteString)
      .value

    (party, transaction)
  }

  /** Signs the transaction with participant1's namespace key and sends it to the sequencer as a
    * topology broadcast.
    */
  private def broadcast(transaction: TopologyTransaction[TopologyChangeOp, TopologyMapping])(
      implicit env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val signedTransaction = SignedTopologyTransaction
      .signAndCreate(
        transaction,
        NonEmpty.mk(Set, participant1.fingerprint),
        isProposal = false,
        participant1.crypto.privateCrypto,
        testedProtocolVersion,
      )
      .futureValueUS
      .value

    val batch = Batch.closeEnvelopes(
      Batch.of(
        testedProtocolVersion,
        (
          TopologyTransactionsBroadcast(daId, Seq(signedTransaction)),
          Recipients.cc(TopologyBroadcastAddress.recipient),
        ),
      )
    )

    val request = SubmissionRequest.tryCreate(
      sender = participant1.member,
      messageId = MessageId.randomMessageId(),
      batch = batch,
      maxSequencingTime = CantonTimestamp.MaxValue,
      topologyTimestamp = None,
      aggregationRule = None,
      submissionCost = None,
      protocolVersion = testedProtocolVersion,
    )

    val cryptoSnapshot: SyncCryptoApi =
      participant1.underlying.value.sync.syncCrypto
        .forSynchronizer(daId, staticSynchronizerParameters1)
        .value
        .currentSnapshotApproximation
        .futureValueUS
    val signedRequest = SignedContent
      .create(
        cryptoApi = cryptoSnapshot.pureCrypto,
        cryptoPrivateApi = cryptoSnapshot,
        content = request,
        timestampOfSigningKey = Some(cryptoSnapshot.ipsSnapshot.timestamp),
        signingTimestampOverrides = None,
        purpose = HashPurpose.SubmissionRequestSignature,
        protocolVersion = testedProtocolVersion,
      )
      .futureValueUS
      .value

    sequencer1.underlying.value.sequencer.sequencer
      .sendAsyncSigned(signedRequest)
      .futureValueUS
      .value shouldBe ()
  }

  private def isHosting(party: PartyId)(implicit env: TestConsoleEnvironment): Option[PartyId] = {
    import env.*
    participant1.topology.party_to_participant_mappings
      .list(daId, filterParty = party.toProtoPrimitive)
      .headOption
      .map(_.item.partyId)
  }

  "Topology transactions with wrong proto version" should {
    // TODO(#35499): Switch to stable PV
    "be accepted with the legacy proto version 29" onlyRunLessThan ProtocolVersion.dev in {
      implicit env =>
        import env.*

        val (party, transaction) = partyToParticipant("legacy-proto-version", protoVersion = 29)
        broadcast(transaction)

        eventually() {
          isHosting(party).value shouldBe party
        }

        val stored = participant1.topology.transactions
          .list(store = daId, filterMappings = Seq(TopologyMapping.Code.PartyToParticipant))
          .result
          .map(_.transaction.transaction)
          .filter(_.mapping.select[PartyToParticipant].exists(_.partyId == party))
          .loneElement

        // Legacy proto versions are read with the lowest supported proto version
        stored.representativeProtocolVersion shouldBe
          TopologyTransaction.protocolVersionRepresentativeFor(ProtoVersion(30)).value

        // The original proto version is kept
        UntypedVersionedMessage
          .parseFrom(stored.getCryptographicEvidence.toByteArray)
          .version shouldBe 29
    }

    "be dropped by the receiving nodes with an unknown proto version" in { implicit env =>
      import env.*

      val unknownProtoVersion = 999
      val (party, transaction) =
        partyToParticipant("unknown-proto-version", protoVersion = unknownProtoVersion)

      // The sequencer accepts the submission because it does not deserialize the topology
      // transactions. Every receiving node fails to open the envelope and raises an alarm.
      val expectedError =
        s"Unable to find deserializer for version ${ProtoVersion(unknownProtoVersion)}"

      def logAssertion(entry: LogEntry, node: String) = {
        entry.loggerName should include(node)
        entry.warningMessage should include(expectedError)
      }

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        broadcast(transaction),
        LogEntry.assertLogSeq(
          Seq(
            (logAssertion(_, "participant1"), "alert from P1"),
            (logAssertion(_, "sequencer1"), "alert from S1"),
            (logAssertion(_, "mediator1"), "alert from M1"),
          )
        ),
      )

      participant1.parties.enable("alice")

      // participant1 raised the alarm above and the transaction was dropped
      isHosting(party) shouldBe empty

      // nodes are still healthy
      participant1.health.ping(participant1)
    }
  }
}
