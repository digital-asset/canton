// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer

import cats.syntax.functorFilter.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  LsuSequencingTestMessage,
  ProtocolMessage,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError, OpenEnvelope}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.InvalidLsuSequencingTestSignature
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext

/** Handle [[com.digitalasset.canton.protocol.messages.LsuSequencingTestMessage]]
  */
final class LsuSequencingTestMessageHandler(
    metrics: MediatorMetrics,
    cryptoApi: SynchronizerCryptoClient,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends UnsignedProtocolEventHandler
    with NamedLogging {
  override def name: String = "LsuTestSequencingMessagesHandler"

  private val psid = cryptoApi.psid

  override def subscriptionStartsAt(
      start: SubscriptionStart,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  override def apply(
      tracedBatch: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
  ): HandlerResult =
    MonadUtil.sequentialTraverseMonoid(tracedBatch.value) { tracedEvent =>
      implicit val tracContext: TraceContext = tracedEvent.traceContext

      tracedEvent.value match {
        case Deliver(_, ts, _, _, batch, _, _) =>
          logger.debug(s"Processing sequenced event with timestamp $ts")

          val lsuSequencingTestMessages = batch.envelopes.mapFilter(
            ProtocolMessage.select[LsuSequencingTestMessage]
          )

          val withCorrectPSId: Seq[OpenEnvelope[LsuSequencingTestMessage]] = ProtocolMessage
            .filterSynchronizerEnvelopes(
              lsuSequencingTestMessages,
              psid,
            ) { wrongMessages =>
              val wrongPSIds = wrongMessages.map(_.protocolMessage.psid)
              logger.error(
                s"Received test lsu sequencing messages with wrong synchronizer ids: $wrongPSIds"
              )
            }

          val resF = NonEmpty
            .from(withCorrectPSId)
            .fold(FutureUnlessShutdown.unit)(handleInternal(ts, _))

          // This can be processed asynchronously
          HandlerResult.asynchronous(resF)

        case _: DeliverError => HandlerResult.done
      }
    }

  /** @param ts
    *   Sequencing timestamps
    * @param withCorrectPSId
    *   Open envelopes, with psid checked.
    */
  private def handleInternal(
      ts: CantonTimestamp,
      withCorrectPSId: NonEmpty[Seq[OpenEnvelope[LsuSequencingTestMessage]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = for {
    snapshot <- cryptoApi.ips.awaitSnapshot(ts)
    withCorrectPSIdAndSignature: Seq[LsuSequencingTestMessage] <- withCorrectPSId.forgetNE
      .parTraverseFilter { msg =>
        LsuSequencingTestMessage
          .verifySignature(cryptoApi.syncCryptoVerifier, snapshot)(msg.protocolMessage)
          .value
          .map {
            case Left(err) =>
              // This is logged on creation
              InvalidLsuSequencingTestSignature.Error(msg.protocolMessage, err).discard
              Option.empty[LsuSequencingTestMessage]
            case Right(()) => msg.protocolMessage.some
          }
      }

    // We just count the number of such messages.
    _ = withCorrectPSIdAndSignature.foreach { msg =>
      val sender = msg.content.sender
      logger.info(
        s"Received LsuSequencingTest message from $sender with sequencing time $ts"
      )

      metrics.receivedTestingLsuSequencingMessages.mark()(
        MetricsContext(
          "sender" -> msg.content.sender.toProtoPrimitive
        )
      )
    }
  } yield ()
}
