// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcStreamingReceiver.{
  AuthenticationTimeout,
  UnauthenticatedPeer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.{
  abort,
  objIdC,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DelayUtil, FutureUnlessShutdownUtil}
import io.grpc.Status
import io.grpc.stub.StreamObserver

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Random}

abstract class P2PGrpcStreamingReceiver(
    maybeP2PEndpointId: Option[P2PEndpoint.Id],
    inputModule: ModuleRef[BftOrderingMessage],
    sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
    isAuthenticationEnabled: Boolean,
    metrics: BftOrderingMetrics,
    // Interval between application-level RTT echo probes; probing is disabled when non-positive.
    rttProbeInterval: FiniteDuration,
    // Sends a message to the peer over this connection's stream (wraps the associated `PeerSender`).
    sendToPeer: BftOrderingMessage => Unit,
    // Builds a fresh `ConnectionOpened` message (with this node's `sentBy` and a current timestamp),
    //  reused as both the RTT probe and its reflection.
    makeConnectionOpener: () => BftOrderingMessage,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    traceContext: TraceContext,
    executionContext: ExecutionContext,
    metricsContext: MetricsContext,
) extends StreamObserver[BftOrderingMessage]
    with NamedLogging
    with AutoCloseable {

  override def toString: String = objIdC(this)

  def shutdown(): Unit

  private[grpc] val remotePeerId: String =
    maybeP2PEndpointId
      .map(_.toString)
      .getOrElse(s"<unknown (incoming connection, receiver: $this)>")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var closed = false

  // Ensures the terminal handling (teardown metric, log, and channel shutdown) runs at most once.
  //  A single teardown can otherwise surface as several terminal callbacks to the same observer:
  //  validation failures and the authentication timeout call `onError` directly, and the `close()`
  //  they trigger shuts down the outgoing channel, which can deliver another `onError` (potentially
  //  with a different status). Without this guard, one teardown would be counted more than once.
  private val terminated = new AtomicBoolean(false)

  // Best-effort peer node ID, captured once the sequencer ID is known, for teardown metrics.
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var resolvedPeerIdO: Option[BftNodeId] = None

  // Direction of this stream from this node's point of view: outgoing when we dialed the peer
  //  (a P2P endpoint is known), incoming when the peer dialed us.
  private val direction: String =
    if (maybeP2PEndpointId.isDefined) metrics.p2p.stream.labels.direction.Outgoing
    else metrics.p2p.stream.labels.direction.Incoming

  // Application-level RTT probing (connection-direction- and transport-independent). The dialer
  //  (outgoing) side is the prober: it periodically sends a `ConnectionOpened` echo probe and
  //  measures the round-trip time when the peer reflects it back. The acceptor (incoming) side is
  //  the reflector: it echoes every `ConnectionOpened` it receives back and never measures.
  //  Splitting the roles by connection direction ensures probes never loop (the prober never
  //  reflects, the reflector never originates), so exactly one reflection flows back per probe.
  private val isRttProber: Boolean = maybeP2PEndpointId.isDefined
  // RTT is enabled network-wide by the (uniform) configuration: the prober probes and the reflector
  //  reflects only when it is on, so it is fully inert (a single boolean check per message) otherwise.
  private val rttEnabled: Boolean =
    rttProbeInterval > scala.concurrent.duration.Duration.Zero
  private val outstandingRttProbeSentAt = new AtomicReference[Option[Instant]](None)

  setupAuthenticationTimeout()
  captureResolvedPeerId()
  maybeStartRttProbing()

  override final def close(): Unit = {
    closed = true
    shutdown()
  }

  final override def onNext(message: BftOrderingMessage): Unit = {
    implicit val traceContext: TraceContext = TraceContext.fromW3CTraceParent(message.traceContext)
    logger.trace(s"$this: Received gRPC message from '$remotePeerId': $message")
    message.sentAt.foreach(sendInstant =>
      updateTimer(
        metrics.p2p.send.grpcLatency,
        Duration.between(sendInstant.asJavaInstant, Instant.now),
      )(
        metricsContext.withExtraLabels(
          metrics.p2p.send.labels.SourceSequencer -> message.sentBy
        )
      )
    )
    // Reuse the `ConnectionOpened` message as a low-frequency application-level RTT echo: the prober
    //  measures the round-trip time and the reflector echoes the probe back (see `isRttProber`).
    if (rttEnabled && message.body.exists(_.message.isConnectionOpened))
      handleConnectionOpenedForRtt()
    if (!sequencerIdPromiseUS.isCompleted) {
      if (isAuthenticationEnabled) {
        val errorMsg =
          s"Received gRPC message before authentication was completed"
        sequencerIdPromiseUS.complete(Failure(new RuntimeException(errorMsg)))
        abort(logger, errorMsg)
      } else {
        logger.debug(
          s"Authentication disabled: received first gRPC message from '$remotePeerId'"
        )
        // Authentication is disabled: backfill the sequencer ID promise with the first message's sentBy
        SequencerId.fromProtoPrimitive(message.sentBy, "sentBy") match {
          case Left(e) =>
            val msg = s"$this: Received unparseable sequencer ID from '$remotePeerId': ${e.message}"
            logger.warn(msg)
            val error = new RuntimeException(msg)
            sequencerIdPromiseUS.complete(Failure(error))
            onError(error)
          case Right(sequencerId) =>
            logger.debug(
              s"Providing the sequencer ID ${sequencerId.toProtoPrimitive} " +
                s"in 'sentBy' of in first gRPC message from '$remotePeerId'"
            )
            sequencerIdPromiseUS.outcome_(sequencerId)
        }
      }
    } else {
      logger.trace(
        s"$this: Received sequencer ID for '$remotePeerId' already from authentication or first message"
      )
    }

    sequencerIdPromiseUS.futureUS
      .map { sequencerId =>
        if (validateNodeId(message, sequencerId)) {
          logger.trace(
            s"Forwarding gRPC message from '$remotePeerId' to p2p network input module: $message"
          )
          inputModule.asyncSend(message)
        }
      }
      .onShutdown(
        logger.debug(s"Dropping gRPC message from '$remotePeerId' on shutdown")
      )
      .discard
  }

  private def validateNodeId(message: BftOrderingMessage, sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): Boolean = {
    def emitNonCompliance(
        metrics: BftOrderingMetrics
    )(from: BftNodeId)(implicit mc: MetricsContext): Unit = {
      val mcWithLabels = mc.withExtraLabels(
        metrics.security.noncompliant.labels.Sequencer -> from,
        metrics.security.noncompliant.labels.violationType.Key ->
          metrics.security.noncompliant.labels.violationType.values.WrongGrpcMessageSentByBftNodeId,
      )
      metrics.security.noncompliant.behavior.mark()(mcWithLabels)
    }

    val bftNodeId = SequencerNodeId.toBftNodeId(sequencerId)

    if (bftNodeId != message.sentBy) {
      // Signature verification uses the `from` field (a.k.a. original sender) from the underlying (signed) message,
      //  so validating the `sentBy` field from the top-level message is fine.
      val msg =
        s"$this: Sequencer ID (`$sequencerId`) from authentication and `sentBy` (`${message.sentBy}`) " +
          s"do not match in gRPC message from '$remotePeerId'"
      logger.warn(msg)
      emitNonCompliance(metrics)(bftNodeId)
      onError(new RuntimeException(msg))
      false
    } else true
  }

  final override def onError(t: Throwable): Unit =
    if (terminated.compareAndSet(false, true)) {
      closed = true
      emitStreamTeardown(t)
      logger.info(
        s"$this: Received error (${t.getMessage}) from '$remotePeerId', " +
          "invalidating connection and shutting down the gRPC channel",
        t,
      )
      close()
    } else {
      logger.debug(
        s"$this: Ignoring repeated terminal callback (onError: ${t.getMessage}) from '$remotePeerId'",
        t,
      )
    }

  final override def onCompleted(): Unit =
    if (terminated.compareAndSet(false, true)) {
      closed = true
      logger.info(
        s"$this: Received completion from '$remotePeerId', " +
          "invalidating connection and shutting down the gRPC channel"
      )
      close()
    } else {
      logger.debug(
        s"$this: Ignoring repeated terminal callback (onCompleted) from '$remotePeerId'"
      )
    }

  // Bump a peer-, direction- and gRPC-status-labeled counter so remote- or infrastructure-initiated
  //  stream resets (e.g. a proxy GOAWAY/RST_STREAM surfaced as UNAVAILABLE) are observable as metrics,
  //  complementing the INFO log emitted on teardown.
  private def emitStreamTeardown(t: Throwable): Unit = {
    val stream = metrics.p2p.stream
    val peer: String = resolvedPeerIdO.map(id => id: String).getOrElse(UnauthenticatedPeer)
    stream.teardowns.mark(1L)(
      metricsContext.withExtraLabels(
        stream.labels.Peer -> peer,
        stream.labels.Direction -> direction,
        stream.labels.GrpcStatus -> Status.fromThrowable(t).getCode.name,
      )
    )
  }

  private def captureResolvedPeerId(): Unit =
    sequencerIdPromiseUS.futureUS
      .map(sequencerId => resolvedPeerIdO = Some(SequencerNodeId.toBftNodeId(sequencerId)))
      .discard

  // Handles every received `ConnectionOpened` message for RTT measurement. The prober measures the
  //  round-trip time against its outstanding probe (if any) and does not reflect; the reflector
  //  echoes the opener back and does not measure.
  private def handleConnectionOpenedForRtt()(implicit traceContext: TraceContext): Unit =
    if (isRttProber)
      // `getAndSet(None)` guards against measuring twice, and against a stale reflection with no
      //  outstanding probe (e.g. the peer's own initial handshake opener received at startup).
      outstandingRttProbeSentAt.getAndSet(None).foreach { sentAt =>
        recordRtt(Duration.between(sentAt, Instant.now()))
      }
    else
      // Reflect off the gRPC inbound thread. That thread delivers all received messages for this
      //  connection in order (the `StreamObserver` contract), and the (flow-controlled) send drains
      //  the whole send queue under a lock; doing it inline would block in-order delivery of
      //  subsequent real messages behind the drain and inflate ordering latency under load.
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
        FutureUnlessShutdown.outcomeF(Future {
          try sendToPeer(makeConnectionOpener())
          catch {
            case NonFatal(e) =>
              logger.debug(s"$this: Failed to reflect RTT echo to '$remotePeerId'", e)
          }
        }),
        s"$this: Failed to reflect RTT echo to '$remotePeerId'",
      )

  private def recordRtt(rtt: Duration): Unit = {
    val stream = metrics.p2p.stream
    val peer = resolvedPeerIdO.map(id => id: String).getOrElse(UnauthenticatedPeer)
    // Only the prober (the side that dialed, i.e. the outgoing connection) ever records RTT, so a
    //  direction label would always be "outgoing" and carry no information; hence it is omitted.
    updateTimer(stream.roundTripLatency, rtt)(
      metricsContext.withExtraLabels(
        stream.labels.Peer -> peer
      )
    )
    logger.trace(s"$this: Measured application-level RTT to '$remotePeerId': $rtt")
  }

  // Starts the periodic RTT probe loop on the prober (outgoing) side, once the peer's identity is
  //  known so that RTT samples can be attributed to it. No-op on the reflector side or when disabled.
  private def maybeStartRttProbing(): Unit =
    if (isRttProber && rttEnabled)
      sequencerIdPromiseUS.futureUS
        .map(_ => scheduleNextRttProbe())
        .discard

  private def scheduleNextRttProbe(): Unit =
    if (!closed)
      DelayUtil.delay(nextRttProbeDelay()).onComplete { _ =>
        if (!closed) {
          sendRttProbe()
          scheduleNextRttProbe()
        }
      }

  // Apply +/-50% uniform jitter to the probe interval, so that probes across many peers and
  //  connections don't synchronize into periodic bursts of traffic and metric updates.
  private def nextRttProbeDelay(): FiniteDuration = {
    val jitterFactor = 0.5 + Random.nextDouble() // in [0.5, 1.5)
    FiniteDuration(
      math.max(1L, (rttProbeInterval.toNanos * jitterFactor).toLong),
      java.util.concurrent.TimeUnit.NANOSECONDS,
    )
  }

  private def sendRttProbe(): Unit =
    // Keep at most one probe in flight. If the previous probe hasn't been reflected back yet (exactly
    //  the kind of application stall this metric should expose), don't overwrite its send time:
    //  overwriting would make the delayed reflection be measured from the newer timestamp and
    //  substantially under-report the RTT, and the real (large) sample would be lost. Skip this round
    //  instead; the outstanding probe stays pending and is measured whenever its reflection arrives.
    //  The stream is a reliable, ordered, per-connection channel, so a reflection is only ever lost if
    //  the connection breaks, which tears down and recreates this receiver (resetting the outstanding
    //  probe), so probing cannot stall forever. The send time is recorded (via the CAS) before sending,
    //  so a fast reflection cannot race ahead of the timestamp.
    if (outstandingRttProbeSentAt.compareAndSet(None, Some(Instant.now())))
      try sendToPeer(makeConnectionOpener())
      catch {
        case NonFatal(e) =>
          // The probe never went out; clear the reservation so the next tick can probe again.
          outstandingRttProbeSentAt.set(None)
          logger.debug(s"$this: Failed to send RTT probe to '$remotePeerId'", e)
      }

  // Don't wait forever for the sequencer ID to be provided:
  //
  // - If authentication is disabled, it comes from the first message but the peer could misbehave and not send it.
  // - Even if authentication is enabled, during shutdown the Canton member authenticator could be closed
  //   before we have a chance to obtain the sequencer ID, so if we don't complete the promise, we
  //   could potentially prevent the shutdown.
  private def setupAuthenticationTimeout(): Unit =
    DelayUtil.delay(AuthenticationTimeout).onComplete { _ =>
      if (!sequencerIdPromiseUS.isCompleted) {
        val msg =
          s"$this: Did not receive sequencer ID from '$remotePeerId' within $AuthenticationTimeout"
        val error = new RuntimeException(msg)
        sequencerIdPromiseUS.complete(Failure(error))
        if (!closed) {
          logger.info(msg + ", terminating the gRPC stream")
          onError(error)
        } else {
          logger.info(msg + ", not terminating the gRPC stream as it's already closed")
        }
      } else {
        logger.debug(
          s"$this: Received sequencer ID from '$remotePeerId' before the authentication timeout"
        )
      }
    }
}

object P2PGrpcStreamingReceiver {

  // Peer label used for teardowns observed before the peer's sequencer ID is known.
  private val UnauthenticatedPeer = "<unauthenticated>"

  // The timeout for receiving the sequencer ID from the server, after which
  //  the promise is completed with an error.
  //  It must be short enough to not block the shutdown of the node for too long,
  //  but long enough to allow the authentication to complete in a reasonable time.
  private val AuthenticationTimeout: FiniteDuration = 5.seconds
}
