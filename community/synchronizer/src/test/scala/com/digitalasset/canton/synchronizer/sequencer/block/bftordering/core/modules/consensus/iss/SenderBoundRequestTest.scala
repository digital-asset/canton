// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
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
import com.digitalasset.canton.version.ProtocolVersion
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
  )

  private def checkSenderBinding[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      request: A,
      messageType: AuthenticatedMessageType,
      parse: (BftNodeId, v30.SignedMessage) => ParsingResult[SignedMessage[A]],
  ): Future[Assertion] = {
    val signature = crypto.pureCrypto
      .sign(
        hashForMessage(request, sender, messageType),
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

    "bind retransmission requests to the authenticated sender rather than the signed wrapper" in {
      val status = EpochStatus.create(
        sender,
        EpochNumber.First,
        NonEmpty(Seq, SegmentStatus.Complete),
      )
      checkSenderBinding(
        status,
        AuthenticatedMessageType.BftSignedRetransmissionMessage,
        (sentBy, wire) => RetransmissionRequest.fromProto(sentBy, wire).map(_.signedEpochStatus),
      )
    }

    "bind v30 block transfer requests to the authenticated sender rather than the signed wrapper" in {
      checkSenderBinding[StateTransferNetworkMessage](
        BlockTransferRequest.create(EpochNumber.First, sender)(ProtocolVersion.v36),
        AuthenticatedMessageType.BftSignedStateTransferMessage,
        (sentBy, wire) =>
          SignedMessage.fromProto(v30.StateTransferMessage, SupportedVersions.ProtoData30)(
            IssConsensusModule.parseStateTransferMessage(sentBy, _)
          )(wire),
      )
    }

    "bind v31 block transfer requests to the authenticated sender rather than the signed wrapper" in {
      checkSenderBinding[StateTransferNetworkMessage](
        BlockTransferRequest.create(EpochNumber.First, sender)(ProtocolVersion.v37),
        AuthenticatedMessageType.BftSignedStateTransferMessage,
        (sentBy, wire) =>
          SignedMessage.fromProto(v31.StateTransferMessage, SupportedVersions.ProtoData31)(
            IssConsensusModule.parseStateTransferMessage31(sentBy, _)
          )(wire),
      )
    }
  }
}
