// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.SignatureCheckError.SignerHasNoValidKeys
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Hash, Signature, SignatureCheckError, TestHash}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.{
  AuthenticatedMessageType,
  BftOrderingSigningKeyUsage,
  hashForMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferTestHelpers
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  FailingCryptoProvider,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.{
  RetransmissionRequest,
  RetransmissionResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Prepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.{
  EpochStatus,
  SegmentStatus,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class IssConsensusSignatureVerifierTest extends AnyWordSpec with BftSequencerBaseTest {
  val myId: BftNodeId = BftNodeId("self")
  val otherIds: IndexedSeq[BftNodeId] = (1 to 3).map(index => BftNodeId(s"node$index"))
  val allIds: Seq[BftNodeId] = (myId +: otherIds).sorted
  val anOrderingTopology = OrderingTopology.forTesting(allIds.toSet)
  val aMembership =
    Membership(myId, anOrderingTopology, leaders = allIds, blacklistedNodes = Seq.empty)
  def aTopologyInfo(
      currentCryptoProvider: CryptoProvider[ProgrammableUnitTestEnv] = new FailingCryptoProvider()
  ) =
    OrderingTopologyInfo[ProgrammableUnitTestEnv](
      myId,
      currentTopology = anOrderingTopology,
      currentCryptoProvider = currentCryptoProvider,
      currentLeaders = allIds,
      currentBlacklistedNodes = Seq.empty,
      previousTopology = anOrderingTopology, // not relevant
      previousCryptoProvider = currentCryptoProvider,
      previousLeaders = allIds,
      previousBlacklistedNodes = Seq.empty,
    )

  val blockMetadata = BlockMetadata(EpochNumber.First, BlockNumber.First)

  "IssConsensusSignatureVerifier" should {
    "reject cross-synchronizer retransmissions and state-transfer certificates" in {
      implicit val context: ProgrammableUnitTestContext[Prepare] = new ProgrammableUnitTestContext
      val pv = ProtocolVersion.dev
      val localPsid = testPsid.copy(protocolVersion = pv)
      val foreignPsid = localPsid.incrementSerial.value
      val crypto = SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
      val keys = crypto.newSymbolicSigningKeyPair(BftOrderingSigningKeyUsage)
      val provider = new FailingCryptoProvider[ProgrammableUnitTestEnv] {
        override val psid = localPsid
        override def verifySignature(
            hash: Hash,
            node: BftNodeId,
            signature: Signature,
            operationId: String,
        )(implicit
            traceContext: TraceContext,
            metricsContext: MetricsContext,
        ): () => Either[SignatureCheckError, Unit] =
          () =>
            crypto.pureCrypto.verifySignature(
              hash,
              keys.publicKey,
              signature,
              BftOrderingSigningKeyUsage,
            )
      }
      val sender = StateTransferTestHelpers.otherId
      val topology = OrderingTopology.forTesting(Set(myId, sender))
      val topologyInfo = aTopologyInfo(provider).copy(
        currentTopology = topology,
        previousTopology = topology,
      )
      val verifier = new IssConsensusSignatureVerifier[ProgrammableUnitTestEnv](
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
      )(pv)
      def sign[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](
          message: A,
          kind: AuthenticatedMessageType,
          psid: com.digitalasset.canton.topology.PhysicalSynchronizerId,
      ): SignedMessage[A] =
        SignedMessage(
          message,
          crypto.pureCrypto
            .sign(
              hashForMessage(message, message.from, kind, psid),
              keys.privateKey,
              BftOrderingSigningKeyUsage,
            )
            .value,
        )
      def check(
          psid: com.digitalasset.canton.topology.PhysicalSynchronizerId,
          expected: Boolean,
      ): Unit = {
        val status =
          EpochStatus.create(sender, EpochNumber.First, NonEmpty(Seq, SegmentStatus.Complete))(pv)
        val request = RetransmissionRequest(
          sign(status, AuthenticatedMessageType.BftSignedRetransmissionMessage, psid)
        )
        val template = StateTransferTestHelpers.aCommitCert()(pv)
        val certificate = CommitCertificate(
          sign(
            template.prePrepare.message,
            AuthenticatedMessageType.BftSignedConsensusMessage,
            psid,
          ),
          template.commits.map(c =>
            sign(c.message, AuthenticatedMessageType.BftSignedConsensusMessage, psid)
          ),
        )
        val requestResult = verifier.verifyRetransmissionMessage(request, topologyInfo)
        val responseResult = verifier.verifyRetransmissionMessage(
          RetransmissionResponse(sender, Seq(certificate)),
          topologyInfo,
        )
        val certificateResult = verifier.verifyConsensusCertificate(certificate, topologyInfo)
        requestResult().isRight shouldBe expected
        responseResult().isRight shouldBe expected
        certificateResult().isRight shouldBe expected
      }
      check(localPsid, expected = true)
      check(foreignPsid, expected = false)
    }

    "verify signature of message" in {
      implicit val context: ProgrammableUnitTestContext[Prepare] = new ProgrammableUnitTestContext
      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

      val verifier = new IssConsensusSignatureVerifier[ProgrammableUnitTestEnv](
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
      )
      val hash = TestHash.digest("pre-prepare")
      val prepare = Prepare
        .create(
          blockMetadata,
          ViewNumber.First,
          hash,
          myId,
        )
        .fakeSign
      val result = verifier.verify(prepare, aTopologyInfo(cryptoProvider))

      verify(cryptoProvider, times(1)).verifySignedMessage(
        prepare,
        AuthenticatedMessageType.BftSignedConsensusMessage,
      )

      result() shouldBe Right(())
    }

    "check if the node is a member before checking signature" in {
      implicit val context: ProgrammableUnitTestContext[Prepare] = new ProgrammableUnitTestContext
      val verifier = new IssConsensusSignatureVerifier[ProgrammableUnitTestEnv](
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
      )
      val prepare = Prepare
        .create(
          blockMetadata,
          ViewNumber.First,
          TestHash.digest("pre-prepare"),
          BftNodeId("InvalidNode"),
        )
        .fakeSign
      val result = verifier.verify(prepare, aTopologyInfo())
      result() shouldBe Left(
        List(
          SignerHasNoValidKeys(
            "Cannot verify signature from node InvalidNode, because it is not currently a valid member"
          )
        )
      )
    }
  }
}
