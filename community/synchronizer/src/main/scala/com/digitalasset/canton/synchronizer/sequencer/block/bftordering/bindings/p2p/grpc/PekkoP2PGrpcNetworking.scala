// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.{MetricHandle, MetricsContext}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionManager.PeerSender
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoActorContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
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

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.FiniteDuration

private sealed trait PekkoP2PGrpcConnectionManagerActorMessage {
  def traceContext: TraceContext
}

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
    override val traceContext: TraceContext,
) extends ResendablePekkoP2PGrpcConnectionManagerActorMessage

private final case class SendMessage(
    override val config: BftBlockOrdererConfig,
    recipientBftNodeId: BftNodeId,
    createMessage: Option[Instant] => BftOrderingMessage,
    metricsContext: MetricsContext,
    attemptNumber: Int,
    override val traceContext: TraceContext,
    sendInstantForQueueLatencyMetrics: Instant = Instant.now,
    retryDelayO: Option[FiniteDuration] = None,
    supposedSendInstantForLatencyTestingO: Option[Instant] = None,
) extends ResendablePekkoP2PGrpcConnectionManagerActorMessage

/** Closes the connection-managing actor. Sent as part of disconnecting an endpoint.
  */
private final case class Close(override val traceContext: TraceContext)
    extends PekkoP2PGrpcConnectionManagerActorMessage

final class PekkoP2PNetworkRef(
    connectionManagingActorRef: ActorRef[PekkoP2PGrpcConnectionManagerActorMessage],
    val actorName: String,
    outstandingMessages: AtomicInteger,
    config: BftBlockOrdererConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val traceContext: TraceContext)
    extends P2PNetworkRef[BftOrderingMessage]
    with NamedLogging {

  import PekkoP2PNetworkRef.*

  // Coordinates the two-phase start with closing: the connection-managing actor is spawned parked
  //  and only activated (sent Initialize) by `startConnection` once the ref has been published. If a
  //  concurrent shutdown closes this ref (e.g. because it was superseded or its entry was removed)
  //  before it is started, we must not activate its actor, since an activated actor runs
  //  address-based cleanup on teardown and could corrupt the connection state now owned by another
  //  ref for the same address.
  //
  //  Start-vs-close is coordinated through the inherited closing synchronization (see
  //  `startConnection`); this atomic only makes the start idempotent across repeated calls and, as a
  //  secondary guard, records the terminal `Closed` state.
  private val lifecycle = new AtomicReference[Lifecycle](Parked)

  override def toString: String = this.getClass.getSimpleName + s"($actorName)"

  override def startConnection()(implicit traceContext: TraceContext): Unit =
    // Guard the start with the inherited closing synchronization so that closing either prevents the
    //  start (the body does not run once closing has begun, leaving the parked actor inert) or waits
    //  until the body has finished, i.e. until `Initialize` has been enqueued ahead of the `Close`
    //  that `onClosed` enqueues. Without this, `close` marks the ref closing *before* `onClosed` sets
    //  `Closed`, leaving a gap in which the CAS below could still start an already-removed ref. The
    //  CAS additionally makes the start idempotent across repeated calls.
    synchronizeWithClosingSync("start-connection") {
      if (lifecycle.compareAndSet(Parked, Started)) {
        outstandingMessages.incrementAndGet().discard
        connectionManagingActorRef ! Initialize(config, attemptNumber = 1, traceContext)
      } else
        logger.info(
          s"Not starting connection for ref $this as it is already ${lifecycle.get()}"
        )
    }.discard

  override def asyncP2PSend(
      recipientBftNodeId: BftNodeId,
      createMessage: Option[Instant] => BftOrderingMessage,
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit = {
    outstandingMessages.incrementAndGet().discard
    synchronizeWithClosingSync("send-message") {
      connectionManagingActorRef ! SendMessage(
        config,
        recipientBftNodeId,
        createMessage,
        metricsContext,
        attemptNumber = 1,
        traceContext,
      )
    }.discard
  }

  override def onClosed(): Unit = {
    // By the time `onClosed` runs, the closing synchronization has already prevented (or drained) any
    //  concurrent `startConnection`, so no start can still be in flight. Record the terminal state
    //  (a secondary guard for the CAS in `startConnection`) and send Close so the actor stops,
    //  whether it was left parked or already activated.
    lifecycle.set(Closed)
    logger.debug(s"Sending Close message to connection managing actor for ref $this")
    connectionManagingActorRef ! Close(traceContext)
  }
}

object PekkoP2PNetworkRef {

  private sealed trait Lifecycle
  private case object Parked extends Lifecycle
  private case object Started extends Lifecycle
  private case object Closed extends Lifecycle
}

object PekkoP2PGrpcNetworking {

  final class SimulatedSendFailureException
      extends RuntimeException("Simulated send failure for testing")
      with scala.util.control.NoStackTrace

  final class PekkoP2PGrpcNetworkManager(
      val connectionManager: P2PGrpcConnectionManager,
      config: BftBlockOrdererConfig,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  )(implicit traceContext: TraceContext)
      extends P2PNetworkManager[PekkoModuleSystem.PekkoEnv, BftOrderingMessage]
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
      //  - B wants to send to A, and thus it creates a network ref to A
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
      logger.info("Closing P2P gRPC network manager")
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

            case SendMessage(
                  _,
                  _,
                  _,
                  metricsContext,
                  _,
                  _,
                  sendInstantForQueueLatencyMetrics,
                  retryDelayO,
                  supposedSendInstantForLatencyTestingO,
                ) =>
              // Emit actor queue metrics
              metrics.performance.orderingStageLatency.emitModuleQueueLatency(
                "PekkoP2PGrpcConnectionManagingActor",
                sendInstantForQueueLatencyMetrics,
                retryDelayO,
              )(metricsContext)
              // Do not count sends delayed for latency testing in the actor queue size metrics
              if (
                supposedSendInstantForLatencyTestingO.isEmpty || config.standalone
                  .flatMap(_.testSlowdown)
                  .flatMap(_.sendDelay)
                  .isEmpty
              )
                metrics.performance.orderingStageLatency.emitModuleQueueSize(
                  "PekkoP2PGrpcConnectionManagingActor",
                  outstandingMessages.decrementAndGet(),
                )(metricsContext)

            case Initialize(_, _, traceContext) =>
              outstandingMessages.decrementAndGet()
              logger.debug(s"Connection-managing actor $actorName received `Initialize`")(
                message.traceContext
              )

            case _ =>
          }

        emitModuleQueueStats()

        implicit val traceContext: TraceContext = message.traceContext

        connectionManager.getPeerSenderOrStartConnection(p2pAddress) match {

          case Some(peerSender) =>
            logger.debug(
              s"Connection-managing actor $actorName found connection available $peerSender for Send"
            )
            whenConnected(peerSender)

          case _ =>
            val maxAttempts = config.networkSendAttempts.value
            message match {
              case sendMsg @ SendMessage(_, _, _, metricsContext, attemptNumber, _, _, _, _) =>
                val newAttemptNumber = attemptNumber + 1
                val retryDelay = sendMsg.jitterStream.next(newAttemptNumber)
                if (newAttemptNumber <= maxAttempts) {
                  emitSendFailureMetrics(metrics, metrics.p2p.send.sendsRetried, sendMsg)(
                    metrics.p2p.send.failure.labels.reason.values.ConnectionInitError
                  )
                  logger.debug(
                    s"Connection-managing actor $actorName " +
                      s"couldn't yet obtain connection for `Send`, retrying it in $retryDelay, " +
                      s"attempt $newAttemptNumber out of $maxAttempts"
                  )
                  // Retrying after a delay due to not being connected:
                  //  record the send instant and delay to emit the actor queue latency when processing the message
                  val delayedMessage =
                    sendMsg.copy(
                      attemptNumber = newAttemptNumber,
                      sendInstantForQueueLatencyMetrics = Instant.now,
                      retryDelayO = Some(retryDelay),
                    )
                  outstandingMessages.incrementAndGet().discard
                  context
                    .scheduleOnce(retryDelay, target = context.self, delayedMessage)
                    .discard
                } else {
                  emitSendFailureMetrics(metrics, metrics.p2p.send.sendsDropped, sendMsg)(
                    metrics.p2p.send.failure.labels.reason.values.ConnectionInitError
                  )
                  logger.info(
                    s"Connection-managing actor $actorName " +
                      s"couldn't yet obtain connection for `Send`, $maxAttempts retries exhausted, " +
                      s"not retrying anymore"
                  )
                }

              case i @ Initialize(_, attemptNumber, _) =>
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
                metrics.p2p.send.sendsRetried.inc()(
                  MetricsContext.Empty.withExtraLabels(
                    metrics.p2p.send.failure.labels.reason.Key -> metrics.p2p.send.failure.labels.reason.values.ConnectionInitError
                  )
                )

              case Close(tc) =>
                implicit val traceContext: TraceContext = tc
                abort(
                  logger,
                  s"Connection-managing actor $actorName is unexpectedly processing " +
                    "Close messages with retries",
                )
            }
        }
      }

      def createCloseBehavior()(implicit
          traceContext: TraceContext
      ): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {
        logger.info(s"Closing connection-managing actor $actorName")
        connectionManager.shutdownConnection(
          p2pAddress.id,
          clearNetworkRefAssociations = true,
          closeNetworkRefs = false, // Because we're already closing the actor
        )
        Behaviors.stopped
      }

      Behaviors.setup { implicit pekkoActorContext =>
        // Two-phase start: until the actor has processed at least one Initialize (sent by
        //  `PekkoP2PNetworkRef.startConnection()` once the ref has been published) or an actual
        //  send, it stays parked and inert. A parked actor has not touched the connection state
        //  associated with its address, so it must NOT run the address-based cleanup on teardown;
        //  otherwise a speculative or superseded ref could clean up state now owned by the winning
        //  ref for the same address.
        //
        //  The flag is set *before* any shared-state-mutating work runs, so that if such work fails
        //  and the actor stops, PostStop still runs the address-based cleanup, as in the original
        //  unconditional behavior. It is only ever accessed from the actor's own thread (messages
        //  and the PostStop signal are processed one at a time), so a plain `var` is safe.
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var connectionInitialized = false
        Behaviors
          .receiveMessage[PekkoP2PGrpcConnectionManagerActorMessage] {

            case i: Initialize =>
              connectionInitialized = true
              logger.info(s"Connection-managing actor $actorName initializing")
              scheduleMessageIfNotConnectedBehavior(i)(_ => ())
              Behaviors.same

            case sendMsg: SendMessage =>
              // If the network ref contract is observed, a Send won't reach the actor before an Initialize,
              //  but the actor is nevertheless robust against that.
              connectionInitialized = true
              implicit val traceContext: TraceContext = sendMsg.traceContext

              def grpcSend(
                  peerSender: PeerSender,
                  delayConfO: Option[BftBlockOrdererConfig.BftBlockOrderingP2PSendDelayConfig],
                  messageSendInstant: Instant = Instant.now,
              ): Unit = {
                val msg = sendMsg.createMessage(Some(messageSendInstant))

                def sendAndRecordDiscard(): Unit =
                  try {
                    peerSender.onNext(msg)
                  } catch {
                    case exception: PeerSender.FlowControlException =>
                      logger.debug(
                        s"Connection-managing actor $actorName failed sending message $msg to sender $peerSender due to flow control",
                        exception,
                      )
                      emitSendFailureMetrics(metrics, metrics.p2p.send.sendsDropped, sendMsg)(
                        metrics.p2p.send.failure.labels.reason.values.FlowControl
                      )
                  }

                logger.debug(
                  s"Connection-managing actor $actorName sending message to sender $peerSender"
                )
                try {
                  val recipientBftNodeId = sendMsg.recipientBftNodeId
                  val recipientInstanceNameO = instanceName(recipientBftNodeId)
                  val recipientInstanceNameAndDelayConfO =
                    recipientInstanceNameO.flatMap(recipientInstanceName =>
                      delayConfO.map(delayConf => recipientInstanceName -> delayConf)
                    )
                  recipientInstanceNameAndDelayConfO.fold {
                    sendAndRecordDiscard()
                  } { case (recipientInstanceName, delayConf) =>
                    if (delayConf.nextGrpcSendAcceptedByFlowControl(recipientInstanceName)) {
                      if (delayConf.nextGrpcSendSucceeds(recipientInstanceName)) {
                        sendAndRecordDiscard()
                      } else {
                        throw new SimulatedSendFailureException
                      }
                    } else {
                      emitSendFailureMetrics(metrics, metrics.p2p.send.sendsDropped, sendMsg)(
                        metrics.p2p.send.failure.labels.reason.values.FlowControl
                      )
                    }
                  }
                } catch {
                  case exception: Exception =>
                    logger.debug(
                      s"Connection-managing actor $actorName failed sending message $msg to sender $peerSender",
                      exception,
                    )
                    connectionManager.handleSendFailure(p2pAddress.id, peerSender, exception)
                    val SendErrorFailure = metrics.p2p.send.failure.labels.reason.values.SendError
                    val maxAttempts = config.networkSendAttempts.value
                    // Retrying after a delay due to an exception:
                    //  record the send instant and delay to emit the actor queue latency when processing the message
                    val newAttemptNumber = sendMsg.attemptNumber + 1
                    val delay = sendMsg.jitterStream.next(newAttemptNumber)
                    if (newAttemptNumber <= maxAttempts) {
                      emitSendFailureMetrics(metrics, metrics.p2p.send.sendsRetried, sendMsg)(
                        SendErrorFailure
                      )
                      logger.debug(
                        s"Connection-managing actor $actorName couldn't send a message to sender $peerSender, " +
                          s"invalidating the connection and retrying in $delay, " +
                          s"attempt $newAttemptNumber out of $maxAttempts",
                        exception,
                      )
                      outstandingMessages.incrementAndGet().discard
                      // Retrying after a delay due to send exception:
                      //  record the send instant and delay to emit the actor queue latency when processing the message
                      val delayedMessage =
                        sendMsg.copy(
                          attemptNumber = newAttemptNumber,
                          sendInstantForQueueLatencyMetrics = Instant.now,
                          retryDelayO = Some(delay),
                        )
                      pekkoActorContext
                        .scheduleOnce(
                          delay,
                          target = pekkoActorContext.self,
                          delayedMessage,
                        )
                        .discard
                    } else {
                      emitSendFailureMetrics(metrics, metrics.p2p.send.sendsDropped, sendMsg)(
                        SendErrorFailure
                      )
                      logger.info(
                        s"Connection-managing actor $actorName couldn't send $msg" +
                          ", invalidating the connection. No more retries left.",
                        exception,
                      )
                    }
                }
              }

              scheduleMessageIfNotConnectedBehavior(sendMsg) { peerSender =>
                (
                  sendMsg.supposedSendInstantForLatencyTestingO,
                  config.standalone.flatMap(_.testSlowdown).flatMap(_.sendDelay),
                ) match {
                  case (None, Some(delayConf)) =>
                    val recipientBftNodeId = sendMsg.recipientBftNodeId
                    val recipientInstanceNameO = instanceName(recipientBftNodeId)
                    val delayO = recipientInstanceNameO.flatMap(delayConf.nextSendDelay)
                    logger.debug(
                      s"Connection-managing actor $actorName delaying send of message to $recipientBftNodeId, " +
                        s"instance name $recipientInstanceNameO, sender $peerSender by $delayO"
                    )
                    delayO.fold(grpcSend(peerSender, Some(delayConf))) { delay =>
                      // Not setting retry relay and not incrementing the queue size as we want this send to appear
                      //  as already performed in metrics, rather than queued.
                      val now = Instant.now
                      pekkoActorContext
                        .scheduleOnce(
                          delay,
                          target = pekkoActorContext.self,
                          sendMsg.copy(
                            // Do not count the artificial send delay in the actor queue latency metrics
                            sendInstantForQueueLatencyMetrics = now.plusNanos(delay.toNanos),
                            // Use the current instant as the supposed send instant for latency testing, so that we see
                            //  the artificial latency in the gRPC latency metrics
                            supposedSendInstantForLatencyTestingO = Some(now),
                          ),
                        )
                        .discard
                    }

                  case (Some(supposedSendInstantForLatencyTesting), delayConfO) =>
                    // Fake the network send instant to be the supposed send instant,
                    //  before waiting the artificial delay for latency testing, so that we see
                    //  the artificial latency in the gRPC latency metrics
                    grpcSend(peerSender, delayConfO, supposedSendInstantForLatencyTesting)

                  case (None, None) => grpcSend(peerSender, delayConfO = None)
                }
              }
              Behaviors.same

            case Close(tc) =>
              implicit val traceContext: TraceContext = tc
              if (connectionInitialized) {
                logger.info(
                  s"Connection-managing actor $actorName is stopping, closing"
                )
                createCloseBehavior()
              } else {
                logger.info(
                  s"Connection-managing actor $actorName is stopping before initialization, " +
                    "no connection-state cleanup needed"
                )
                Behaviors.stopped
              }
          }
          .receiveSignal { case (_, PostStop) =>
            if (connectionInitialized) {
              logger.info(s"Connection-managing actor $actorName stopped, closing")
              createCloseBehavior()
            } else {
              logger.info(
                s"Connection-managing actor $actorName stopped before initialization, " +
                  "no connection-state cleanup needed"
              )
              Behaviors.stopped
            }
          }
      }
    }

    private def instanceName(recipientBftNodeId: BftNodeId): Option[String] =
      recipientBftNodeId.split("::").view.zipWithIndex.map(_.swap).toMap.get(1)

    private def emitSendFailureMetrics(
        metrics: BftOrderingMetrics,
        counter: MetricHandle.Counter,
        sendMsg: SendMessage,
    )(
        failureReason: metrics.p2p.send.failure.labels.reason.values.SendFailureReasonValue
    ): Unit = {
      import metrics.p2p.send.failure.labels.reason
      counter.inc()(
        sendMsg.metricsContext.withExtraLabels(
          metrics.p2p.send.labels.TargetSequencer -> sendMsg.recipientBftNodeId,
          reason.Key -> failureReason,
        )
      )
    }
  }
}
