// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.protocol.BftOrdererLimitsView
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.standalone.crypto.FixedKeysCryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.{
  AuthenticatedMessageType,
  BftOrderingSigningKeyUsage,
  hashForMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.RetransmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  BlockTransferRequest,
  StateTransferNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.{
  EpochStatus,
  SegmentStatus,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.{v30, v31}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

final class SenderBoundRequestTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with HasExecutionContext {

  private val sender = BftNodeId("sender")
  private val other = BftNodeId("other")
  private val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
  private val senderKeys = crypto.newSymbolicSigningKeyPair(BftOrderingSigningKeyUsage)
  private val otherKeys = crypto.newSymbolicSigningKeyPair(BftOrderingSigningKeyUsage)
  private val verifier = new FixedKeysCryptoProvider(
    senderKeys.privateKey,
    Map(sender -> senderKeys.publicKey, other -> otherKeys.publicKey),
    crypto.pureCrypto,
    SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
    _ => None,
    testPsid,
  )

  private def checkSenderBinding[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      request: A,
      messageType: AuthenticatedMessageType,
      parse: (BftNodeId, v30.SignedMessage) => ParsingResult[SignedMessage[A]],
  ): Future[Assertion] = {
    val signature = crypto.pureCrypto
      .sign(
        hashForMessage(request, sender, messageType, testPsid),
        senderKeys.privateKey,
        BftOrderingSigningKeyUsage,
      )
      .value
    val wire = SignedMessage(request, signature).toProtoV1
    val direct = parse(sender, wire).value
    val alteredWrapper = parse(sender, wire.copy(from = other)).value
    val forwarded = parse(other, wire).value
    val forwardedAlteredWrapper = parse(other, wire.copy(from = other)).value

    direct.from shouldBe sender
    alteredWrapper.from shouldBe sender
    forwarded.from shouldBe other
    forwardedAlteredWrapper.from shouldBe other
    alteredWrapper.message.getCryptographicEvidence shouldBe request.getCryptographicEvidence
    forwarded.signature shouldBe signature

    for {
      directResult <- toFuture(verifier.verifySignedMessage(direct, messageType))
      alteredWrapperResult <- toFuture(verifier.verifySignedMessage(alteredWrapper, messageType))
      forwardedResult <- toFuture(verifier.verifySignedMessage(forwarded, messageType))
      forwardedAlteredWrapperResult <- toFuture(
        verifier.verifySignedMessage(forwardedAlteredWrapper, messageType)
      )
    } yield {
      directResult shouldBe Right(())
      alteredWrapperResult shouldBe Right(())
      forwardedResult.isLeft shouldBe true
      forwardedAlteredWrapperResult.isLeft shouldBe true
    }
  }

  "Requests that cannot be forwarded" should {

    "bind signed messages and availability acknowledgements to the PSID only after PV 37" in {
      val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
      val batchId = BatchId.createForTesting("psid-binding")
      val messageTypes = Seq[(AuthenticatedMessageType, HashPurpose, HashPurpose)](
        (
          AuthenticatedMessageType.BftSignedAvailabilityMessage,
          HashPurpose.BftSignedAvailabilityMessage,
          HashPurpose.BftSignedAvailabilityMessageV2,
        ),
        (
          AuthenticatedMessageType.BftSignedConsensusMessage,
          HashPurpose.BftSignedConsensusMessage,
          HashPurpose.BftSignedConsensusMessageV2,
        ),
        (
          AuthenticatedMessageType.BftSignedStateTransferMessage,
          HashPurpose.BftSignedStateTransferMessage,
          HashPurpose.BftSignedStateTransferMessageV2,
        ),
        (
          AuthenticatedMessageType.BftSignedRetransmissionMessage,
          HashPurpose.BftSignedRetransmissionMessage,
          HashPurpose.BftSignedRetransmissionMessageV2,
        ),
      )

      Future
        .traverse(Seq(ProtocolVersion.v36, ProtocolVersion.v37, ProtocolVersion.dev)) { pv =>
          val psid = testPsid.copy(protocolVersion = pv)
          val otherPsid = psid.incrementSerial.value
          val request = BlockTransferRequest.create(EpochNumber.First, sender)(pv)
          val localVerifier = new FixedKeysCryptoProvider(
            senderKeys.privateKey,
            Map(sender -> senderKeys.publicKey),
            crypto.pureCrypto,
            metrics,
            _ => None,
            psid,
          )
          val otherVerifier = new FixedKeysCryptoProvider(
            senderKeys.privateKey,
            Map(sender -> senderKeys.publicKey),
            crypto.pureCrypto,
            metrics,
            _ => None,
            otherPsid,
          )
          val ackHash =
            AvailabilityAck.hashFor(batchId, EpochNumber.First, sender, metrics, psid, pv)
          val otherAckHash =
            AvailabilityAck.hashFor(batchId, EpochNumber.First, sender, metrics, otherPsid, pv)
          val ackSignature = crypto.pureCrypto
            .sign(ackHash, senderKeys.privateKey, BftOrderingSigningKeyUsage)
            .value
          val ackResult = crypto.pureCrypto.verifySignature(
            otherAckHash,
            senderKeys.publicKey,
            ackSignature,
            BftOrderingSigningKeyUsage,
          )
          crypto.pureCrypto.verifySignature(
            ackHash,
            senderKeys.publicKey,
            ackSignature,
            BftOrderingSigningKeyUsage,
          ) shouldBe Right(())
          ackResult.isRight shouldBe (pv <= ProtocolVersion.v37)
          if (pv <= ProtocolVersion.v37) {
            ackHash shouldBe Hash
              .build(HashPurpose.BftAvailabilityAck, HashAlgorithm.Sha256)
              .addByteString(batchId.getCryptographicEvidence)
              .addLong(EpochNumber.First)
              .addString(sender)
              .finish()
          } else {
            ackHash shouldBe Hash
              .build(HashPurpose.BftAvailabilityAckV2, HashAlgorithm.Sha256)
              .addString(psid.toProtoPrimitive)
              .addByteString(batchId.getCryptographicEvidence)
              .addLong(EpochNumber.First)
              .addString(sender)
              .finish()
          }

          Future.traverse(messageTypes) { case (messageType, oldPurpose, newPurpose) =>
            val hash = hashForMessage(request, sender, messageType, psid)
            if (pv <= ProtocolVersion.v37)
              hash shouldBe Hash
                .build(oldPurpose, HashAlgorithm.Sha256)
                .addString(sender)
                .addByteString(request.getCryptographicEvidence)
                .finish()
            else
              hash shouldBe Hash
                .build(newPurpose, HashAlgorithm.Sha256)
                .addString(psid.toProtoPrimitive)
                .addString(sender)
                .addByteString(request.getCryptographicEvidence)
                .finish()
            for {
              signed <- toFuture(localVerifier.signMessage(request, messageType))
              local <- toFuture(localVerifier.verifySignedMessage(signed.value, messageType))
              other <- toFuture(otherVerifier.verifySignedMessage(signed.value, messageType))
            } yield {
              local shouldBe Right(())
              other.isRight shouldBe (pv <= ProtocolVersion.v37)
            }
          }
        }
        .map(_ => succeed)
    }

    "bind retransmission requests to the authenticated sender rather than the signed wrapper" in {
      val status = EpochStatus.create(
        sender,
        EpochNumber.First,
        NonEmpty(Seq, SegmentStatus.Complete),
      )
      checkSenderBinding(
        status,
        AuthenticatedMessageType.BftSignedRetransmissionMessage,
        (sentBy, wire) =>
          RetransmissionRequest
            .fromProto(
              ProtocolVersionValidation.AlwaysValidation,
              BftOrdererLimitsView.default,
              sentBy,
              wire,
            )
            .map(_.signedEpochStatus),
      )
    }

    "bind v30 block transfer requests to the authenticated sender rather than the signed wrapper" in {
      checkSenderBinding[StateTransferNetworkMessage](
        BlockTransferRequest.create(EpochNumber.First, sender)(ProtocolVersion.v36),
        AuthenticatedMessageType.BftSignedStateTransferMessage,
        (sentBy, wire) =>
          SignedMessage.fromProto(
            ProtocolVersionValidation.AlwaysValidation,
            v30.StateTransferMessage,
            SupportedVersions.ProtoData30,
          )(
            IssConsensusModule.parseStateTransferMessage(
              ProtocolVersionValidation.AlwaysValidation,
              BftOrdererLimitsView.default,
              sentBy,
              _,
            )
          )(wire),
      )
    }

    "bind v31 block transfer requests to the authenticated sender rather than the signed wrapper" in {
      checkSenderBinding[StateTransferNetworkMessage](
        BlockTransferRequest.create(EpochNumber.First, sender)(ProtocolVersion.v37),
        AuthenticatedMessageType.BftSignedStateTransferMessage,
        (sentBy, wire) =>
          SignedMessage.fromProto(
            ProtocolVersionValidation.AlwaysValidation,
            v31.StateTransferMessage,
            SupportedVersions.ProtoData31,
          )(
            IssConsensusModule.parseStateTransferMessage31(
              ProtocolVersionValidation.AlwaysValidation,
              BftOrdererLimitsView.default,
              sentBy,
              _,
            )
          )(wire),
      )
    }
  }
}
