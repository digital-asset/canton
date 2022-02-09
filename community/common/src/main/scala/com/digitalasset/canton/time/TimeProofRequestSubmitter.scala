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
import com.digitalasset.canton.time.admin.v0
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.retry.{Backoff, Success}
import com.digitalasset.canton.util.{FutureUtil, HasFlushFuture, HasProtoV0, retry}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

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

/** Use [[fetchTimeProof]] to fetch a time proof we observe from the sequencer via [[handleTimeProof]].
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

  /** The [[TimeProofRequestSubmitter]] will attempt to produce a time proof by calling send on the domain sequencer.
    * It will stop requesting a time proof with the first time proof it witnesses (not necessarily the one
    * it requested).
    * Ensures that only a single request is in progress at a time regardless of how many times it is called.
    * Is safe to call frequently without causing many requests to the sequencer.
    * If the component is shutdown it stops requesting a time proof.
    */
  def fetchTimeProof()(implicit traceContext: TraceContext): Unit

  /** Update state based on time proof events observed from the sequencer */
  def handleTimeProof(proof: TimeProof): Unit
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
  import com.digitalasset.canton.time.TimeProofRequestSubmitterImpl._

  private val currentRequestToken: AtomicReference[Token] =
    new AtomicReference[Token](NoCurrentRequest)

  override def fetchTimeProof()(implicit traceContext: TraceContext): Unit = {
    val newToken = new Object
    if (currentRequestToken.compareAndSet(NoCurrentRequest, newToken)) {
      sendRequestIfPending(newToken)
    }
  }

  override def handleTimeProof(proof: TimeProof): Unit = {
    val token = currentRequestToken.getAndSet(NoCurrentRequest)
    if (token != NoCurrentRequest) {
      logger.debug(s"Received $proof")(proof.traceContext)
    }
  }

  private def sendRequestIfPending(token: Token)(implicit traceContext: TraceContext): Unit = {
    def stillPending: Boolean = !isClosing && currentRequestToken.get() == token

    /* Make the request or short circuit if we're no longer waiting a time event */
    def mkRequest(): Future[Either[SendAsyncClientError, Unit]] =
      performUnlessClosingF {
        if (stillPending) {
          logger.debug("Sending time request")
          sendRequest(traceContext).value
        } else Future.successful(Right(()))
      }.onShutdown(Right(()))

    def eventuallySendRequest(): Unit = addToFlush(
      s"sendRequestIfPending scheduled ${config.maxSequencingDelay} after ${clock.now}"
    ) {
      {
        import Success.either
        val retrySendTimeRequest = Backoff(
          logger,
          this,
          retry.Forever,
          config.initialRetryDelay.toScala,
          config.maxRetryDelay.toScala,
          "request current time",
        )

        retrySendTimeRequest(mkRequest(), AllExnRetryable) map { _ =>
          // if we still care about the outcome (we could have witnessed a recent time while sending the request),
          // then schedule retrying a new request.
          // this will short circuit if a new timestamp is not needed at that point.
          if (stillPending) {
            // intentionally don't wait for future
            FutureUtil.doNotAwait(
              clock
                .scheduleAfter(
                  _ => eventuallySendRequest(),
                  config.maxSequencingDelay.unwrap,
                )
                .onShutdown(()),
              "requesting current domain time",
            )
          }
        }
      }
    }

    // initial kick off
    eventuallySendRequest()
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

object TimeProofRequestSubmitterImpl {
  private[TimeProofRequestSubmitterImpl] type Token = AnyRef
  private[TimeProofRequestSubmitterImpl] val NoCurrentRequest: Token = new Object
}
