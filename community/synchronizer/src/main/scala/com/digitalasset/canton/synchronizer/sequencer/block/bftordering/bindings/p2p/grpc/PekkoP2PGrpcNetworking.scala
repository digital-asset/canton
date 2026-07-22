// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionManager.PeerSender
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  failGrpcStreamObserver,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoActorContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.JitterGenerator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.abort
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Jitter
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop}

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration

private sealed trait PekkoP2PGrpcConnectionManagerActorMessage

private sealed trait ResendablePekkoP2PGrpcConnectionManagerActorMessage
    extends PekkoP2PGrpcConnectionManagerActorMessage {
  private[grpc] def config: BftBlockOrdererConfig

  // Note that `Jitter.full.apply` produces a timeout value between 0 and the exponential (we use
  // base 2) as `initialValue*math.pow(base.toDouble, attempt.toDouble)`, the unit of the initial
  // delay is important because the exp is on the non-converted value, the cap is converted to the
  // same unit of the initial delay with ceiling, and what guarantees that the jitter does not
  // yield 0 is the minimum delay.
  private[grpc] lazy val jitterStream =
    JitterGenerator(
      Jitter.full(
        cap = config.networkSendRetryJitterCap.underlying,
        Jitter.randomSource(ThreadLocalRandom.current()),
      ),
      initialDelay = config.networkSendRetryMinimumDelay.underlying,
      minimumDelay = config.networkSendRetryMinimumDelay.underlying,
    )
}

/** Asks the connection-managing actor to initialize the connection without performing a Send; used
  * to create a connection eagerly rather than the first time a message is sent.
  */
private final case class Initialize(
    override val config: BftBlockOrdererConfig,
    attemptNumber: Int,
) extends ResendablePekkoP2PGrpcConnectionManagerActorMessage

private final case class SendMessage(
    override val config: BftBlockOrdererConfig,
    createMessage: Option[Instant] => BftOrderingMessage,
    metricsContext: MetricsContext,
    attemptNumber: Int,
    traceContext: TraceContext,
    sendInstant: Instant = Instant.now,
    maybeDelay: Option[FiniteDuration] = None,
) extends ResendablePekkoP2PGrpcConnectionManagerActorMessage

/** Closes the connection-managing actor. Sent as part of disconnecting an endpoint.
  */
private case object Close extends PekkoP2PGrpcConnectionManagerActorMessage

final class PekkoP2PNetworkRef(
    connectionManagingActorRef: ActorRef[PekkoP2PGrpcConnectionManagerActorMessage],
    val actorName: String,
    outstandingMessages: AtomicInteger,
    config: BftBlockOrdererConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends P2PNetworkRef[BftOrderingMessage]
    with NamedLogging {

  outstandingMessages.incrementAndGet().discard
  connectionManagingActorRef ! Initialize(config, attemptNumber = 1)

  override def toString: String = this.getClass.getSimpleName + s"($actorName)"

  override def asyncP2PSend(
      createMessage: Option[Instant] => BftOrderingMessage
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit = {
    outstandingMessages.incrementAndGet().discard
    synchronizeWithClosingSync("send-message") {
      connectionManagingActorRef ! SendMessage(
        config,
        createMessage,
        metricsContext,
        attemptNumber = 1,
        traceContext,
      )
    }.discard
  }

  override def onClosed(): Unit = {
    implicit val traceContext: TraceContext = TraceContext.empty
    logger.debug(s"Sending Close message to connection managing actor for ref $this")
    connectionManagingActorRef ! Close
  }
}

object PekkoP2PGrpcNetworking {

  final class PekkoP2PGrpcNetworkManager(
      val connectionManager: P2PGrpcConnectionManager,
      config: BftBlockOrdererConfig,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ) extends P2PNetworkManager[PekkoModuleSystem.PekkoEnv, BftOrderingMessage]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        context: PekkoActorContext[ActorContextT],
        p2pAddress: P2PAddress,
    )(implicit traceContext: TraceContext): P2PNetworkRef[BftOrderingMessage] = {

      val bftNodeIdActorNameComponent =
        p2pAddress.maybeBftNodeId.getOrElse("unknown-bft-node-id")
      val p2pEndpointActorNameComponent = p2pAddress.maybeP2PEndpoint
        .map { p2pEndpoint =>
          val security = if (p2pEndpoint.transportSecurity) "tls" else "plaintext"
          s"${p2pEndpoint.address}-${p2pEndpoint.port}-$security"
        }
        .getOrElse("unknown-p2p-endpoint")

      // The Pekko actor name must be unique within the actor system; for each endpoint and node ID we always have
      //  at most one active P2P gRPC connection-managing actor but network ref consolidation could stop and
      //  re-create one for the same endpoint and node ID, so we ensure unicity by appending a UUID.
      //
      //  An example of that situation follows:
      //
      //  - A is configured with an endpoint to B but B is not configured with an endpoint to A
      //  - A connects and authenticates successfully to B
      //  - B wants to send to A and thus it creates a network ref to A
      //  - The connection crashes and B cleans up the network ref
      //  - A reconnects and authenticates successfully to B
      val actorName =
        s"pekko-p2p-grpc-connection-managing-actor-$p2pEndpointActorNameComponent-$bftNodeIdActorNameComponent-${UUID.randomUUID()}"

      val outstandingMessages = new AtomicInteger()

      logger.debug(s"Spawning P2P gRPC connection-managing actor '$actorName'")
      val result =
        new PekkoP2PNetworkRef(
          context.underlying.spawn(
            createGrpcP2PConnectionManagerPekkoBehavior(
              p2pAddress,
              actorName,
              outstandingMessages,
              metrics,
            ),
            actorName,
          ),
          actorName,
          outstandingMessages,
          config,
          timeouts,
          loggerFactory,
        )
      logger.debug(s"Spawned P2P gRPC connection-managing actor '$actorName'")
      result
    }

    override def onClosed(): Unit = {
      logger.info("Closing P2P gRPC network manager")(TraceContext.empty)
      connectionManager.close()
    }

    override def shutdownOutgoingConnection(
        p2pEndpointId: P2PEndpoint.Id
    )(implicit traceContext: TraceContext): Unit =
      connectionManager.shutdownConnection(
        Left(p2pEndpointId),
        clearNetworkRefAssociations = true,
        closeNetworkRefs = true,
      )

    private def createGrpcP2PConnectionManagerPekkoBehavior(
        p2pAddress: P2PAddress,
        actorName: String,
        outstandingMessages: AtomicInteger,
        metrics: BftOrderingMetrics,
    )(implicit traceContext: TraceContext): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {

      // Reschedules an Initialize or Send if the connection is not available yet
      def scheduleMessageIfNotConnectedBehavior(
          message: PekkoP2PGrpcConnectionManagerActorMessage
      )(whenConnected: PeerSender => Unit)(implicit
          context: ActorContext[
            PekkoP2PGrpcConnectionManagerActorMessage
          ]
      ): Unit = {

        def emitModuleQueueStats(): Unit =
          // Emit actor queue latency for the message
          message match {

            case SendMessage(_, _, metricsContext, _, _, sendInstant, maybeDelay) =>
              // Emit actor queue metrics
              metrics.performance.orderingStageLatency.emitModuleQueueLatency(
                "PekkoP2PGrpcConnectionManagingActor",
                sendInstant,
                maybeDelay,
              )(metricsContext)
              metrics.performance.orderingStageLatency.emitModuleQueueSize(
                "PekkoP2PGrpcConnectionManagingActor",
                outstandingMessages.decrementAndGet(),
              )(metricsContext)

            case Initialize(_, _) =>
              outstandingMessages.decrementAndGet()
              logger.debug(s"Connection-managing actor $actorName received `Initialize`")

            case _ =>
          }

        emitModuleQueueStats()

        connectionManager.getPeerSenderOrStartConnection(p2pAddress) match {

          case Some(peerSender) =>
            logger.debug(
              s"Connection-managing actor $actorName found connection available $peerSender for Send"
            )
            whenConnected(peerSender)

          case _ =>
            val maxAttempts = config.networkSendAttempts.value
            message match {
              case sm @ SendMessage(_, grpcMessage, metricsContext, attemptNumber, tc, _, _) =>
                implicit val traceContext: TraceContext = tc
                val newAttemptNumber = attemptNumber + 1
                val delay = sm.jitterStream.next(newAttemptNumber)
                if (newAttemptNumber <= maxAttempts) {
                  logger.debug(
                    s"Connection-managing actor $actorName " +
                      s"couldn't yet obtain connection for `Send`, retrying it in $delay, " +
                      s"attempt $newAttemptNumber out of $maxAttempts"
                  )
                  // Retrying after a delay due to not being connected:
                  //  record the send instant and delay to emit the actor queue latency when processing the message
                  val delayedMessage =
                    sm.copy(
                      attemptNumber = newAttemptNumber,
                      sendInstant = Instant.now,
                      maybeDelay = Some(delay),
                    )
                  outstandingMessages.incrementAndGet().discard
                  context
                    .scheduleOnce(delay, target = context.self, delayedMessage)
                    .discard
                  metrics.p2p.send.sendsRetried.inc()(metricsContext)
                } else
                  logger.info(
                    s"Connection-managing actor $actorName " +
                      s"couldn't yet obtain connection for `Send`, $maxAttempts retries exhausted, " +
                      s"not retrying anymore"
                  )

              case i @ Initialize(_, attemptNumber) =>
                // Initialize must always be retried, since there are modules that wait for a quorum of
                // connections to be established before being initialized. So if we stopped retrying too soon,
                // some nodes could get stuck, which could easily happen in a network where nodes are starting
                // up simultaneously and are not immediately reachable to one another.
                val newAttemptNumber = attemptNumber + 1
                val delay = i.jitterStream.next(newAttemptNumber)
                logger.debug(
                  s"Connection-managing actor $actorName " +
                    s"couldn't yet obtain connection for `Initialize`, retrying it in $delay"
                )
                outstandingMessages.incrementAndGet().discard
                context
                  .scheduleOnce(
                    delay,
                    target = context.self,
                    i.copy(attemptNumber = newAttemptNumber),
                  )
                  .discard
                metrics.p2p.send.sendsRetried.inc()(MetricsContext.Empty)

              case Close =>
                abort(
                  logger,
                  s"Connection-managing actor $actorName is unexpectedly processing " +
                    "Close messages with retries",
                )
            }
        }
      }

      def closeBehavior(): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {
        logger.info(s"Closing connection-managing actor $actorName")
        connectionManager.shutdownConnection(
          p2pAddress.id,
          clearNetworkRefAssociations = true,
          closeNetworkRefs = false, // Because we're already closing the actor
        )
        Behaviors.stopped
      }

      Behaviors.setup { implicit pekkoActorContext =>
        Behaviors
          .receiveMessage[PekkoP2PGrpcConnectionManagerActorMessage] {

            case i: Initialize =>
              logger.info(s"Connection-managing actor $actorName initializing")
              scheduleMessageIfNotConnectedBehavior(i)((_) => ())
              Behaviors.same

            case sendMsg: SendMessage =>
              implicit val traceContext: TraceContext = sendMsg.traceContext
              scheduleMessageIfNotConnectedBehavior(sendMsg) { peerSender =>
                val now = Instant.now
                val msg = sendMsg.createMessage(Some(now))
                try {
                  logger.debug(
                    s"Connection-managing actor $actorName sending message to sender $peerSender"
                  )
                  peerSender.onNext(msg)
                  // Network send succeeded (but it may still be lost)
                  updateTimer(
                    metrics.p2p.send.networkWriteLatency,
                    Duration.between(sendMsg.sendInstant, now),
                  )(sendMsg.metricsContext)
                } catch {
                  case exception: Exception =>
                    logger.debug(
                      s"Connection-managing actor $actorName failed sending message $msg to sender $peerSender",
                      exception,
                    )
                    // Failing the stream in case of an exception when sending is required by the gRPC streaming API
                    failGrpcStreamObserver(peerSender, exception, logger)
                    // gRPC requires onError to be the last event, so the connection must be invalidated even though
                    //  the send operation will be retried.
                    connectionManager.shutdownConnection(
                      p2pAddress.id,
                      clearNetworkRefAssociations = false,
                      closeNetworkRefs = false,
                    )
                    val maxAttempts = config.networkSendAttempts.value
                    // Retrying after a delay due to an exception:
                    //  record the send instant and delay to emit the actor queue latency when processing the message
                    val newAttemptNumber = sendMsg.attemptNumber + 1
                    val delay = sendMsg.jitterStream.next(newAttemptNumber)
                    if (newAttemptNumber <= maxAttempts) {
                      logger.debug(
                        s"Connection-managing actor $actorName couldn't send a message to sender $peerSender, " +
                          s"invalidating the connection and retrying in $delay, " +
                          s"attempt $newAttemptNumber out of $maxAttempts",
                        exception,
                      )
                      outstandingMessages.incrementAndGet().discard
                      pekkoActorContext
                        .scheduleOnce(
                          delay,
                          target = pekkoActorContext.self,
                          sendMsg.copy(
                            attemptNumber = newAttemptNumber,
                            sendInstant = Instant.now,
                            maybeDelay = Some(delay),
                          ),
                        )
                        .discard
                      metrics.p2p.send.sendsRetried.inc()(sendMsg.metricsContext)
                    } else
                      logger.info(
                        s"Connection-managing actor $actorName couldn't send $msg, " +
                          s"invalidating the connection. No more retries left.",
                        exception,
                      )
                }
              }
              Behaviors.same

            case Close =>
              logger.info(
                s"Connection-managing actor $actorName is stopping, closing"
              )
              closeBehavior()
          }
          .receiveSignal { case (_, PostStop) =>
            logger.info(s"Connection-managing actor $actorName stopped, closing")
            closeBehavior()
          }
      }
    }
  }
}
