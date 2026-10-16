// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.Id
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{SymmetricKey, TestHash}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.participant.GeneratorsParticipant
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.acsreplication.{
  AcsReplicationSourceParticipantMessage,
  AcsReplicationTargetParticipantMessage,
}
import com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData
import com.digitalasset.canton.participant.synchronizer.{
  PendingLsuOperation,
  PendingOnboardingTransactions,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.pruning.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.channel.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelConnectedToAllEndpoints,
  SequencerChannelMetadata,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.GeneratorsBftOrdering
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistLeaderSelectionPolicyState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  data as bft,
  modules as bftmodules,
}
import com.digitalasset.canton.synchronizer.sequencer.store.VersionedStatus
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.synchronizer.sequencer.{
  GeneratorsSequencer,
  OnboardingStateForSequencerV2,
  SequencerSnapshot,
}
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.DbSequencerStateManagerStore.AggregatedSignaturesOfSender
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  SignedTopologyTransactions,
  TopologyTransaction,
}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString
import org.scalacheck.Arbitrary
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Because we do exhaustiveness check via class listing, this test needs to be in the project which
  * has as many dependencies as possible.
  */
final class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {

  forAll(Table("protocol version", ProtocolVersion.supported.forgetNE*)) { version =>
    val generators = new CommonGenerators(version)
    val pvv = ProtocolVersionValidation(version)
    val sequencerGenerators =
      new GeneratorsSequencer(
        version,
        generators.topology,
        generators.transaction,
        generators.generatorsProtocolSeq,
        generators.protocol,
      )
    val participantGenerators =
      new GeneratorsParticipant(
        generators.topology,
        generators.transaction,
        generators.lf,
        version,
      )
    val bftOrderingGenerators = new GeneratorsBftOrdering(version)

    import com.digitalasset.canton.crypto.GeneratorsCrypto.*
    import generators.data.*
    import generators.generatorsMessages.*
    import generators.generatorsProtocolSeq.*
    import generators.generatorsSequencing.*
    import generators.localVerdict.*
    import generators.protocol.*
    import generators.trafficData.*
    import generators.transaction.*
    import generators.verdict.*
    import participantGenerators.*
    import sequencerGenerators.*
    import bftOrderingGenerators.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        implicit val getByteStringId: Id[ByteString] => ByteString = identity
        implicit val getByteStringE: Either[String, ByteString] => ByteString = _.value

        val synchronizerLimits = SynchronizerLimits.default

        testVersioned(SymmetricKey, version)

        test(StaticSynchronizerParameters, version)
        test(DynamicSynchronizerParameters, version)
        test(SequencingParameters, version)

        if (version >= ProtocolVersion.v36) {
          test(AcsCommitment, version)
          testContext(
            AcsCommitmentProtocolMessage,
            AcsCommitmentDeserializationContext(synchronizerLimits),
            version,
          )

          testContext(
            AcsCommitmentSummary,
            AcsCommitmentDeserializationContext(synchronizerLimits),
            version,
          )
          testContext(
            AcsCommitmentSummaryProtocolMessage,
            AcsCommitmentDeserializationContext(synchronizerLimits),
            version,
          )
        }

        test(LegacyAcsCommitmentProtocolMessage, version)

        test(LegacyAcsCommitment, version)
        test(LsuSequencingTestMessage, version)
        test(LsuSequencingTestMessageContent, version)
        test(Verdict, version)
        test(ConfirmationResponses, version)
        test(TypedSignedProtocolMessageContent, version)
        test(SignedProtocolMessage, version)
        test(ProtocolSymmetricKey, version)
        test(LocalVerdict, version)
        testContext(
          EnvelopeContent,
          (EnvelopeContentDeserializationContext(TestHash, synchronizerLimits), version),
          version,
        )
        test(ConfirmationResultMessage, version)
        test(PendingLsuOperation, version)
        test(PendingOnboardingTransactions, version)
        test(OnboardingClearanceOperation, version)

        test(AcknowledgeRequest, version)
        test(AggregationRule, version)
        test(SequencingSubmissionCost, version)

        // Merkle tree leaves
        testContext(CommonMetadata, TestHash, version)
        testContext(ParticipantMetadata, TestHash, version)
        testContext(
          SubmitterMetadata,
          SubmitterMetadataDeserializationContext(TestHash, synchronizerLimits),
          version,
        )
        testContext(AssignmentCommonData, TestHash, version)
        testContext(AssignmentView, TestHash, version)
        testContext(UnassignmentCommonData, TestHash, version)
        testContext(UnassignmentView, TestHash, version)
        test(UnassignmentData, version)
        testContext(
          ViewCommonData,
          ViewCommonDataDeserializationContext(
            TestHash,
            SynchronizerLimits.default.transactionProtocolLimits,
          ),
          version,
        )

        testContext(RootHashMessage, SerializedRootHashMessagePayload.fromByteString, version)
        testContext(AssignmentMediatorMessage, (TestHash, Target(pvv)), version)(
          assignmentMediatorMessageArb,
          getByteStringId,
        )
        testContext(
          UnassignmentMediatorMessage,
          (TestHash, Source(pvv)),
          version,
        )(
          unassignmentMediatorMessageArb,
          getByteStringId,
        )
        // InformeeMessage become large due to the embedded ExternalAuthorization (quadratic list)
        // on top of transaction view trees, so give this test more time.
        testContext(
          InformeeMessage,
          (GenTransactionTreeDeserializationContext(TestHash, synchronizerLimits), version),
          version,
        )(
          informeeMessageArb,
          getByteStringId,
        )

        test(EncryptedViewMessage, version)(
          encryptedViewMessageArb,
          getByteStringId,
        )

        testContext(TopologyTransaction, testedTopologyTransactionContext, version)
        testContext(TopologyTransactionsBroadcast, testedTopologyTransactionContext, version)
        testContext(SignedTopologyTransaction, testedTopologyTransactionContext, version)
        testContext(SignedTopologyTransactions, testedTopologyTransactionContext, version)
        testContext(SequencerSnapshot, version, version)
        test(OnboardingStateForSequencerV2, version)
        test(LsuTrafficState, version)
        test(SynchronizerPredecessor, version)

        testContext(ViewParticipantData, (TestHash, version), version)
        // the generated recipient trees can be quite big, even they are already limited
        testContext(
          Batch,
          BatchDeserializationContext(
            defaultDecompressionPolicy,
            SynchronizerLimits.default,
          ),
          version,
        )
        test(SetTrafficPurchasedMessage, version)
        testContext(
          SubmissionRequest,
          SubmissionRequestDeserializationContext(
            defaultDecompressionPolicy,
            SynchronizerLimits.default,
          ),
          version,
        )
        testVersioned(SequencerConnections, version)
        testVersioned(CounterParticipantIntervalsBehind, version)
        test(GetTrafficStateForMemberRequest, version)
        // This fails, which is expected, because PartySignatures serialization is only defined on PV.dev
        // We do this on purpose to make clear that this is a work in progress and should **NOT** be merged to 3.1
        test(ExternalAuthorization, version)
        test(GetTrafficStateForMemberResponse, version)
        test(TopologyStateForInitRequest, version)
        test(SubscriptionRequest, version)
        if (version.isDev) {
          test(ConnectToSequencerChannelRequest, version)
          test(ConnectToSequencerChannelResponse, version)
          test(SequencerChannelMetadata, version)
          test(SequencerChannelConnectedToAllEndpoints, version)
          test(SequencerChannelSessionKey, version)
          test(SequencerChannelSessionKeyAck, version)

          test(PartyReplicationStatus, version)
          test(AcsReplicationStatus, version)
          test(AcsReplicationSourceParticipantMessage.AcsDigest, version)
          testContext(AcsReplicationSourceParticipantMessage, version, version)
          test(AcsReplicationTargetParticipantMessage, version)
        }

        // Generated sequenced events get quite big because each batched envelope has recipient trees
        // of quadratic size breadth * depth, so this test takes longer than other tests.
        testContext(
          SequencedEvent,
          SequencedEventDeserializationContext(
            defaultDecompressionPolicy,
            SynchronizerLimits.default,
          ),
          version,
        )
        // Also cover the deferred-decompression path: parse keeping the batch compressed, then
        // decompress separately.
        testProtocolVersionedCommon[Id, SequencedEvent[GenBatch[?]], SequencedEvent[
          Batch[ClosedEnvelope]
        ]](
          SequencedEvent,
          bytes =>
            SequencedEvent
              .fromTrustedByteStringCompressed(bytes)
              .flatMap(
                SequencedEvent
                  .decompress(
                    _,
                    pvv,
                    defaultDecompressionPolicy,
                    SynchronizerLimits.default,
                  )
              ),
        )
        test(SignedContent, version)
        testContext(
          TransactionView,
          TransactionViewDeserializationContext(
            TestHash,
            DepthCounter.NoLimit,
            version,
            SynchronizerLimits.default.transactionProtocolLimits,
          ),
          version,
        )
        testContext(
          FullInformeeTree,
          (GenTransactionTreeDeserializationContext(TestHash, synchronizerLimits), version),
          version,
        )
        // testing MerkleSeq structure with specific VersionedMerkleTree: SubmitterMetadata.
        testContext(
          MerkleSeq,
          (
            TestHash,
            (bytes: ByteString, _: DepthCounter) =>
              SubmitterMetadata.fromTrustedByteString(
                SubmitterMetadataDeserializationContext(TestHash, synchronizerLimits)
              )(bytes),
            DepthCounter.NoLimit,
          ),
          version,
        )
        val randomnessLength =
          EncryptedViewMessage.computeRandomnessLength(ExampleTransactionFactory.pureCrypto)
        testContext(
          LightTransactionViewTree,
          (
            LightTransactionViewTreeDeserializationContext(
              TestHash,
              randomnessLength,
              synchronizerLimits,
            ),
            version,
          ),
          version,
        )
        testContext(
          AssignmentViewTree,
          (TestHash, Target(pvv)),
          version,
        )
        testContext(
          UnassignmentViewTree,
          (TestHash, Source(pvv)),
          version,
        )

        test(SubmissionTrackingData, version)

        test(VersionedStatus, version)
        test(AggregatedSignaturesOfSender, version)

        // CantonBFT messages
        test(bft.topology.SequencingParameters, version)
        test(BlacklistLeaderSelectionPolicyState, version)
        test(OrderingRequestBatch, version)
        testContext(bftmodules.ConsensusStatus.EpochStatus, actualSender, version)

        testContext(bftmodules.Availability.RemoteDissemination.RemoteBatch, actualSender, version)
        testContext(
          bftmodules.Availability.RemoteDissemination.RemoteBatchAcknowledged,
          actualSender,
          version,
        )
        testContext(
          bftmodules.Availability.RemoteOutputFetch.RemoteBatchDataFetched,
          actualSender,
          version,
        )
        testContext(
          bftmodules.Availability.RemoteOutputFetch.FetchRemoteBatchData,
          actualSender,
          version,
        )

        testContext(
          bftmodules.Consensus.StateTransferMessage.BlockTransferRequest,
          actualSender,
          version,
        )
        testContext(
          bftmodules.ConsensusSegment.ConsensusMessage.Commit,
          Some(actualSender),
          version,
        )(arb = commitArb(Some(actualSender)), getByteStringValue = getByteStringId)
        testContext(
          bftmodules.ConsensusSegment.ConsensusMessage.Prepare,
          Some(actualSender),
          version,
        )
        testContext(
          bftmodules.ConsensusSegment.ConsensusMessage.PrePrepare,
          Some(actualSender),
          version,
        )
        testContext(
          bftmodules.ConsensusSegment.ConsensusMessage.ViewChange,
          Some(actualSender),
          version,
        )
        testContext(
          bftmodules.ConsensusSegment.ConsensusMessage.NewView,
          Some(actualSender),
          version,
        )
        if (version < ProtocolVersion.v37) {
          testContext(
            bftmodules.Consensus.StateTransferMessage.BlockTransferResponse30,
            actualSender,
            version,
          )
        } else {
          testContext(
            bftmodules.Consensus.StateTransferMessage.BlockTransferResponse31,
            actualSender,
            version,
          )
        }
      }
    }

    forAll(Table("contract ID version", CantonContractIdVersion.all*)) { contractIdVersion =>
      val generatorContract = new GeneratorsContract(contractIdVersion)
      import generatorContract.*

      s"Serialization and deserialization methods for contract ID version $contractIdVersion and protocol version $version" should {
        "compose to the identity" in {
          testContractVersion[ContractAuthenticationData](
            "ContractAuthenticationData to LfBytes",
            _.toLfBytes.toByteString,
            bytes =>
              ContractAuthenticationData.fromLfBytes(
                pvv,
                contractIdVersion,
                Bytes.fromByteString(bytes),
              ),
          )

          testContractVersion[ContractAuthenticationData](
            "ContractAuthenticationData to v30.SerializableContractId",
            _.toSerializableContractProtoV30,
            ContractAuthenticationData
              .fromSerializableContractProtoV30(
                pvv,
                contractIdVersion,
                _,
              ),
          )

          testContractVersion[ContractAuthenticationData](
            "ContractAuthenticationData to admin.SerializableContractId",
            _.toSerializableContractAdminProtoV30,
            ContractAuthenticationData
              .fromSerializableContractAdminProtoV30(
                pvv,
                contractIdVersion,
                _,
              ),
          )
        }
      }
    }
  }

  "be exhaustive" in {
    val unfilteredRequiredTests = findBaseVersioningCompanionSubClasses()

    val ignoreAnnotation = classOf[IgnoreInSerializationTestExhaustivenessCheck]
    val requiredTests =
      unfilteredRequiredTests.filterNot(ignoreAnnotation.isAssignableFrom).map(_.getName).toSet

    val exceptions = Set(
      // Parallel deserializer for the same wire format as SequencedEvent, keeping the batch
      // compressed. The wire format is already covered by the SequencedEvent round-trip above, and
      // CompressedSequencedEventTest checks that the two versioning tables stay in sync.
      "com.digitalasset.canton.sequencing.protocol.CompressedSequencedEvent$",

      // TODO(#26599) Remove this exception
      "com.digitalasset.canton.participant.admin.data.ActiveContract$",

      // TODO(#26726) Remove this exception
      "com.digitalasset.canton.data.MerkleSeq$MerkleSeqElement$",
    )

    val untestedClasses = requiredTests.diff(testedClasses)
    val missingTests = untestedClasses
      .diff(exceptions)

    /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
     */
    clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)

    // Check that all exceptions are actually needed
    val unnecessaryExceptions = exceptions.diff(untestedClasses)
    clue(s"Unnecessary exceptions should be removed") {
      unnecessaryExceptions shouldBe empty
    }

  }

  // Test that it is not possible to create ViewParticipantData containing contracts
  // serialized with a LF serialization version that is not supported by the protocol version.
  // In practice this means that pv=ProtocolVersion.latest should not allow VDev contracts, while pv=dev should allow them.
  s"ViewParticipantData.tryCreate with protocol version ${ProtocolVersion.latest}" should {

    val generators = new CommonGenerators(ProtocolVersion.latest)

    def recreateWith(
        extraCoreInput: Option[InputContract],
        extraCreatedCore: Option[CreatedContract],
    ): Either[String, ViewParticipantData] = {
      val vpd = generators.data.viewParticipantDataArb.arbitrary.sample.value
      ViewParticipantData.create(vpd.hashOps)(
        coreInputs = vpd.coreInputs ++ extraCoreInput.map(i => i.contract.contractId -> i),
        createdCore = vpd.createdCore ++ extraCreatedCore,
        createdInSubviewArchivedInCore = vpd.createdInSubviewArchivedInCore,
        keyResolution = vpd.keyResolution,
        actionDescription = vpd.actionDescription,
        rollbackContext = vpd.rollbackContext,
        salt = vpd.salt,
        externalCallResults = vpd.externalCallResults,
        protocolVersion = ProtocolVersion.latest,
      )
    }

    val vDevContract = ExampleContractFactory.build(
      keyOpt = Some(ExampleContractFactory.buildKeyWithMaintainers()),
      overrideSerializationVersion = Some(LfSerializationVersion.VDev),
    )
    vDevContract.inst.version shouldBe LfSerializationVersion.VDev

    s"allow ${LfSerializationVersion.V3} contracts" in {
      recreateWith(None, None).isRight shouldBe true
    }

    s"disallow ${LfSerializationVersion.VDev} input contracts" in {
      inside(
        recreateWith(
          Some(InputContract(vDevContract, consumed = false)),
          None,
        )
      ) { case Left(err) =>
        err should include(
          "ViewParticipantData contains contract serialization versions not supported by protocol version"
        )
      }
    }

    s"disallow ${LfSerializationVersion.VDev} created contracts" in {
      inside(
        recreateWith(
          None,
          Some(CreatedContract.tryCreate(vDevContract, consumedInCore = false, rolledBack = false)),
        )
      ) { case Left(err) =>
        err should include(
          "ViewParticipantData contains contract serialization versions not supported by protocol version"
        )
      }
    }

  }

  private def testContractVersion[T](
      name: String,
      toByteString: T => ByteString,
      fromByteString: ByteString => ParsingResult[T],
  )(implicit arb: Arbitrary[T]): Unit =
    forAll { (instance: T) =>
      val byteString = toByteString(instance)

      val deserializedInstance = clue(s"Deserializing serialized $name")(
        fromByteString(byteString).value
      )

      withClue(s"Comparing $name with (de)serialization") {
        instance shouldBe deserializedInstance
      }
    }

}
