// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.data.EitherT
import cats.syntax.option._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.{SendAsyncClientError, SequencerClient}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.time.admin.v0
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil.logOnFailure
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.retry.{Backoff, Success}
import com.digitalasset.canton.util.{HasFlushFuture, HasProtoV0}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** @param initialRetryDelay The initial retry delay if the request to send a sequenced event fails
  * @param maxRetryDelay The max retry delay if the request to send a sequenced event fails
  * @param maxSequencingDelay If our request for a sequenced event was successful, how long should we wait
  *                                      to observe it from the sequencer before starting a new request.
  */
case class TimeProofRequestConfig(
    initialRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(200),
    maxRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
    maxSequencingDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
) extends HasProtoV0[v0.TimeProofRequestConfig] {
  override def toProtoV0: v0.TimeProofRequestConfig = v0.TimeProofRequestConfig(
    initialRetryDelay.toProtoPrimitive.some,
    maxRetryDelay.toProtoPrimitive.some,
    maxSequencingDelay.toProtoPrimitive.some,
  )
}

object TimeProofRequestConfig {
  def fromProto(
      configP: v0.TimeProofRequestConfig
  ): ParsingResult[TimeProofRequestConfig] =
    for {
      initialRetryDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay"),
        "initialRetryDelay",
        configP.initialRetryDelay,
      )
      maxRetryDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxRetryDelay"),
        "maxRetryDelay",
        configP.maxRetryDelay,
      )
      maxSequencingDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxSequencingDelay"),
        "maxSequencingDelay",
        configP.maxSequencingDelay,
      )
    } yield TimeProofRequestConfig(initialRetryDelay, maxRetryDelay, maxSequencingDelay)
}

/** Use [[fetchTimeProof]] to fetch a time proof we observe from the sequencer via [[handle]].
  * Will batch fetch calls so there is only a single request occurring at any point.
  *
  * The submission of this request to the sequencer is slightly more involved than usual as we do not rely at all
  * on domain time as this component is primarily used when the domain time is likely unknown or stale.
  * Instead we use the the local node clock for retries.
  *
  * Future optimizations:
  *  - Most scenarios don't need a time event specifically and instead just need any event to cause a "tick".
  *    In these cases we could short circuit and cancel a pending request when receiving any event with a timestamp.
  *    However this would only optimize our retry loop so the distinction doesn't currently feel anywhere near worthwhile.
  */
trait TimeProofRequestSubmitter extends AutoCloseable {

  /** Returns a future that will be completed with the next received time proof received by the handler.
    * The [[TimeProofRequestSubmitter]] will attempt to produce a time event by calling send on the domain sequencer
    * however will resolve the returned future with the first time proof it witnesses (not necessarily the one
    * it requested).
    * Ensures that only a single request is in progress at a time regardless of how many times it is called.
    * Is safe to call frequently without causing many requests to the sequencer.
    * If the component is shutdown the returned future will currently remain pending.
    */
  def fetchTimeProof()(implicit traceContext: TraceContext): Future[TimeProof]

  /** Update state based on events observed from the sequencer */
  def handle(event: OrdinarySequencedEvent[_]): Unit
}

private[time] class TimeProofRequestSubmitterImpl(
    config: TimeProofRequestConfig,
    sendRequest: TraceContext => EitherT[Future, SendAsyncClientError, Unit],
    clock: Clock,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TimeProofRequestSubmitter
    with NamedLogging
    with FlagCloseable
    with HasFlushFuture {

  private val lock: AnyRef = new Object
  private def withLock[A](fn: => A): A = blocking { lock.synchronized { fn } }
  // updates must be coordinated with the above lock
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var currentRequest: Option[Promise[TimeProof]] = None

  override def fetchTimeProof()(implicit traceContext: TraceContext): Future[TimeProof] =
    withLock {
      currentRequest.fold({
        val newRequest = Promise[TimeProof]()
        currentRequest = newRequest.some
        requestTime(newRequest)
        newRequest.future
      })(_.future)
    }

  override def handle(event: OrdinarySequencedEvent[_]): Unit = {
    implicit val traceContext: TraceContext = event.traceContext

    // we only need to update the request if we have a time event that could change its state
    TimeProof.fromEventO(event).foreach { proof =>
      withLock {
        currentRequest.fold(()) { request =>
          logger.debug(s"Received $proof")
          // trySuccess as may have already been resolved due to shutdown
          request.trySuccess(proof)
          currentRequest = None
        }
      }
    }
  }

  private def requestTime(
      requestP: Promise[TimeProof]
  )(implicit traceContext: TraceContext): Unit = {
    def stillPending: Boolean =
      !isClosing && withLock {
        // are we still the active request
        currentRequest.contains(requestP)
      }

    /* Make the request or short circuit if we're no longer waiting a time event */
    def mkRequest(): Future[Either[SendAsyncClientError, Unit]] =
      performUnlessClosingF {
        if (stillPending) {
          logger.debug("Sending time request")
          sendRequest(traceContext).value
        } else Future.successful(Right(()))
      }.onShutdown(Right(()))

    /* Keep retrying sending the request until it's sent or we no longer need to */
    def eventuallySendRequest(): Future[Unit] = {
      import Success.either
      val retrySendTimeRequest = Backoff(
        logger,
        this,
        Int.MaxValue,
        config.initialRetryDelay.toScala,
        config.maxRetryDelay.toScala,
        "request current time",
      )

      retrySendTimeRequest(mkRequest(), AllExnRetryable) flatMap { _ =>
        // if we still care about the outcome (we could have witnessed a recent time while sending the request),
        // then schedule retrying a new request.
        // this will short circuit if a new timestamp is not needed at that point.
        if (stillPending) {
          // intentionally don't wait for future - error logging is added inside addRunRequest
          val _ = clock.scheduleAfter(
            _ => {
              addRunRequest()
            },
            config.maxSequencingDelay.unwrap,
          )
        }

        Future.unit
      }
    }

    def addRunRequest(): Unit =
      addToFlush(s"requestTime scheduled ${config.maxSequencingDelay} after ${clock.now}") {
        logOnFailure(
          eventuallySendRequest(),
          "requesting current domain time",
        )
      }

    // initial kick off
    addRunRequest()
  }

  @VisibleForTesting
  protected[time] def flush(): Future[Unit] = doFlush()
}

object TimeProofRequestSubmitter {
  def apply(
      config: TimeProofRequestConfig,
      clock: Clock,
      sequencerClient: SequencerClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): TimeProofRequestSubmitter =
    new TimeProofRequestSubmitterImpl(
      config,
      TimeProof.sendRequest(sequencerClient)(_),
      clock,
      sequencerClient.timeouts,
      loggerFactory,
    )
}
