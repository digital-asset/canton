// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PMetrics.{
  emitReceiveStats,
  receiveMetricsContext,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  P2PNetworkIn,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessageBody.Message
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.{v30, v31}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import java.time.{Duration, Instant}

class P2PNetworkInModule[E <: Env[E]](
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val consensus: ModuleRef[Consensus.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit synchronizerProtocolVersion: ProtocolVersion, mc: MetricsContext)
    extends P2PNetworkIn[E] {

  private type OutcomeType = metrics.p2p.receive.labels.source.values.SourceValue

  override def receiveInternal(
      message: BftOrderingMessage
  )(implicit
      context: E#ActorContextT[BftOrderingMessage],
      traceContext: TraceContext,
  ): Unit = {
    val sentBy = BftNodeId(message.sentBy)
    logger.trace(s"Received network message from $sentBy: $message")
    val start = Instant.now
    parseAndForwardBody(
      sentBy,
      message.body,
      start,
    )(TraceContext.fromW3CTraceParent(message.traceContext))
  }

  private def parseAndForwardBody(
      sentBy: BftNodeId,
      body: Option[BftOrderingMessageBody],
      start: Instant,
  )(implicit traceContext: TraceContext): Unit = {
    val outcome: OutcomeType =
      body.fold[OutcomeType]({
        logger.info(s"Received empty message body from $sentBy, dropping")
        metrics.p2p.receive.labels.source.values.Empty(sentBy)
      })(body => handleMessage(sentBy, body.message))

    val end = Instant.now
    val mc1 = receiveMetricsContext(metrics)(outcome)
    locally {
      implicit val mc: MetricsContext = mc1
      updateTimer(metrics.p2p.receive.processingLatency, Duration.between(start, end))
      emitReceiveStats(metrics, size = body.map(_.serializedSize.toLong).getOrElse(0L))
    }
  }

  private def handleMessage(
      sentBy: BftNodeId,
      message: Message,
  )(implicit
      traceContext: TraceContext
  ): OutcomeType =
    message match {

      case Message.Empty =>
        logger.info(s"Received empty message from $sentBy, dropping")
        metrics.p2p.receive.labels.source.values.Empty(sentBy)

      case Message.AvailabilityMessage(availabilityMessage) =>
        AvailabilityModule
          .parseNetworkMessage(availabilityMessage)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping availability message from $sentBy as it couldn't be parsed: $errorMessage"
              ),
            msg => availability.asyncSend(msg),
          )
        metrics.p2p.receive.labels.source.values.Availability(sentBy)

      case Message.ConsensusMessage(consensusMessage) =>
        IssConsensusModule
          .parseNetworkMessage(consensusMessage, actualSender = sentBy)
          .map(msg => Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage(msg))
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping consensus message from $sentBy as it couldn't be parsed: $errorMessage"
              ),
            unverifiedNetworkMessage => {
              val claimedOriginator = unverifiedNetworkMessage.underlyingNetworkMessage.message.from
              if (claimedOriginator != sentBy) {
                val epoch =
                  unverifiedNetworkMessage.underlyingNetworkMessage.message.blockMetadata.epochNumber
                logger.debug(
                  s"Received retransmitted message at epoch $epoch sent by $sentBy originally created by $claimedOriginator"
                )
              }
              consensus.asyncSend(unverifiedNetworkMessage)
            },
          )
        metrics.p2p.receive.labels.source.values.Consensus(sentBy)

      // For retransmission and state transfer messages, we assume that the authenticated sender
      //  is the signer and this is functionally enough because we don't support forwarding
      //  for these messages.
      //  This doesn't allow the sender to impersonate an arbitrary originator because
      //  the signed message is verified against the sender; the proto signed message's from
      //  is just ignored. See `SenderBoundRequestTest`.

      case Message.RetransmissionMessage(message) =>
        IssConsensusModule
          .parseRetransmissionMessage(sentBy, message)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping retransmission message from $sentBy as it couldn't be parsed: $errorMessage"
              ),
            message => consensus.asyncSend(message),
          )
        metrics.p2p.receive.labels.source.values.Retransmissions(sentBy)

      case Message.StateTransferMessage(message) =>
        val parsedSignedMessage =
          if (synchronizerProtocolVersion >= ProtocolVersion.v37)
            SignedMessage
              .fromProto(v31.StateTransferMessage, SupportedVersions.ProtoData31)(
                IssConsensusModule
                  .parseStateTransferMessage31(sentBy, _)
              )(message)
          else
            SignedMessage
              .fromProto(v30.StateTransferMessage, SupportedVersions.ProtoData30)(
                IssConsensusModule
                  .parseStateTransferMessage(sentBy, _)
              )(message)
        parsedSignedMessage
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping state transfer message from $sentBy as it couldn't be parsed: $errorMessage"
              ),
            signedMessage =>
              consensus.asyncSend(
                Consensus.StateTransferMessage.UnverifiedStateTransferMessage(signedMessage)
              ),
          )
        metrics.p2p.receive.labels.source.values.StateTransfer(sentBy)

      case Message.ConnectionOpened(_) =>
        logger.debug(s"Received connection opener from $sentBy")
        metrics.p2p.receive.labels.source.values.ConnectionOpener(sentBy)
    }
}
