// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import cats.syntax.option.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.client.{SendAsyncClientError, SequencerClient}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.UUID

/** Wrapper for a sequenced event that has the correct properties to act as a time proof:
  *   - a deliver event with no envelopes
  *   - has a message id that suggests it was requested as a time proof (this is practically
  *     unnecessary but will act as a safeguard against future sequenced event changes)
  * @param event
  *   the signed content wrapper containing the event
  * @param deliver
  *   the time proof event itself. this must be the event content signedEvent wrapper.
  */
final case class TimeProof private (
    private val event: OrdinarySequencedEvent[Batch[Envelope[?]]],
    private val deliver: Deliver[Nothing],
) extends PrettyPrinting
    with HasCryptographicEvidence {

  require(
    event.signedEvent.content eq deliver,
    "Time proof event must be the content of the provided signed sequencer event",
  )

  def timestamp: CantonTimestamp = deliver.timestamp

  def traceContext: TraceContext = event.traceContext

  override protected def pretty: Pretty[TimeProof.this.type] = prettyOfClass(
    unnamedParam(_.timestamp)
  )

  override def getCryptographicEvidence: ByteString = deliver.getCryptographicEvidence
}

object TimeProof {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def fromEvent(event: OrdinarySequencedEvent[Batch[Envelope[?]]]): Either[String, TimeProof] =
    for {
      deliver <- PartialFunction
        .condOpt(event.signedEvent.content) { case deliver: Deliver[Batch[Envelope[?]]] =>
          deliver
        }
        .toRight("Time Proof must be a deliver event")
      _ <- validateDeliver(deliver)
      // is now safe to cast to a `Deliver[Nothing]` as we've validated it has no envelopes
      emptyDeliver = deliver.asInstanceOf[Deliver[Nothing]]
    } yield new TimeProof(event, emptyDeliver)

  private def validateDeliver(deliver: Deliver[Batch[Envelope[?]]]): Either[String, Unit] =
    for {
      _ <- Either.cond(
        isTimeEventBatch(deliver.batch),
        (),
        "Time Proof event should have no envelopes",
      )
      _ <- Either.cond(
        deliver.messageIdO.exists(isTimeEventMessageId),
        (),
        "Time Proof event should have an expected message id",
      )
    } yield ()

  /** Return a wrapped [[TimeProof]] if the given `event` has the correct properties. */
  def fromEventO(event: OrdinarySequencedEvent[Batch[Envelope[?]]]): Option[TimeProof] =
    fromEvent(event).toOption

  /** Is the event a time proof */
  def isTimeProofDeliver(deliver: Deliver[Batch[Envelope[?]]]): Boolean =
    validateDeliver(deliver).isRight

  /** Does the submission request look like a request to create a time event */
  def isTimeProofSubmission(submission: SubmissionRequest): Boolean =
    isTimeEventMessageId(submission.messageId) && isTimeEventBatch(submission.batch)

  /** Send placed alongside the validation logic for a time proof to help ensure it remains
    * consistent
    */
  def sendRequest(
      client: SequencerClient,
      timeProofRequestExpiry: NonNegativeFiniteDuration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = {
    implicit val metricsContext: MetricsContext = MetricsContext("type" -> "time-proof")
    client.send(
      // we intentionally ask for an empty event to be sequenced to observe the time.
      // this means we can safely share this event without mentioning other recipients.
      batch = Batch.empty(client.protocolVersion),
      timestamps = SendRequestTimestamps(
        topologyTimestamp = None,
        approximateTimestampForSigning = client.clock.now,
        maxSequencingTime = client.clock.now.plus(timeProofRequestExpiry.unwrap),
      ),
      messageId = mkTimeProofRequestMessageId,
      // Do not amplify as there is anyway an outer retry mechanism in TimeProofRequestSubmitter
      amplify = false,
    )
  }

  /** Use a constant prefix for a message which would permit the sequencer to track how many time
    * request events it is receiving.
    */
  val timeEventMessageIdPrefix = "tick-"
  private def isTimeEventMessageId(messageId: MessageId): Boolean =
    messageId.unwrap.startsWith(timeEventMessageIdPrefix)
  private def isTimeEventBatch(batch: Batch[?]): Boolean =
    batch.envelopes.isEmpty // should be entirely empty

  /** Make a unique message id for a time event submission request. Currently adding a short prefix
    * for debugging at the sequencer so floods of time requests will be observable.
    */
  @VisibleForTesting
  def mkTimeProofRequestMessageId: MessageId =
    MessageId(
      String73.tryCreate(
        s"$timeEventMessageIdPrefix${UUID.randomUUID()}",
        "time-proof-message-id".some,
      )
    )
}
