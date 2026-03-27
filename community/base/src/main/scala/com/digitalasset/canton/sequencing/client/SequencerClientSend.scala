// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.protocol.{AggregationRule, Batch, MessageId}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait SequencerClientSend {
  def psid: PhysicalSynchronizerId
  final def protocolVersion: ProtocolVersion = psid.protocolVersion

  implicit protected def executionContext: ExecutionContext

  /** Sends a request to sequence a deliver event to the sequencer. If we fail to make the request
    * to the sequencer and are certain that it was not received by the sequencer an error is
    * returned. In this circumstance it is safe for the caller to retry the request without causing
    * a duplicate request. A successful response however does not mean that the request will be
    * successfully sequenced. Instead the caller must subscribe to the sequencer and can observe one
    * of the following outcomes:
    *   1. A deliver event is sequenced with a messageId matching this send.
    *   1. A deliver error is sequenced with a messageId matching this send.
    *   1. The sequencing time progresses beyond the provided max-sequencing-time. The caller can
    *      assume that the send will now never be sequenced. Callers should be aware that a
    *      message-id can be reused once one of these outcomes is observed so cannot assume that an
    *      event with a matching message-id at any point in the future matches their send. Use the
    *      `sendTracker` to aid tracking timeouts for events (if useful this could be enriched in
    *      the future to provide send completion callbacks alongside the existing timeout
    *      notifications). For convenience callers can provide a callback that the SendTracker will
    *      invoke when the outcome of the send is known. However this convenience comes with
    *      significant limitations that a caller must understand:
    *      - the callback has no ability to be persisted so will be lost after a restart or
    *        recreation of the SequencerClient
    *      - the callback is called by the send tracker while handling an event from a
    *        SequencerSubscription. If the callback returns an error this will be returned to the
    *        underlying subscription handler and shutdown the sequencer client. If handlers do not
    *        want to halt the sequencer subscription errors should be appropriately handled
    *        (particularly logged) and a successful value returned from the callback.
    *      - If witnessing an event causes many prior sends to timeout there is no guaranteed order
    *        in which the callbacks of these sends will be notified.
    *      - If replay is enabled, the callback will be called immediately with a fake `SendResult`.
    *      - When the send tracker is closed, the callback will be called immediately with
    *        AbortedDueToShutdown.
    *      - the closing of objects should not synchronize with the completion of the callback via
    *        performUnlessClosing unless the synchronized object is responsible for closing the
    *        sequencer client itself (possibly transitively). Otherwise shutdown deadlocks are to be
    *        expected between the synchronized object and the send tracker or sequencer client. For
    *        more robust send result tracking callers should persist metadata about the send they
    *        will make and monitor the sequenced events when read, so actions can be taken even if
    *        in-memory state is lost.
    *
    * @param amplify
    *   Amplification sends the submission request to multiple sequencers according to the
    *   [[com.digitalasset.canton.sequencing.SubmissionRequestAmplification]] configured in the
    *   [[com.digitalasset.canton.sequencing.SequencerConnections]]. If the sequencer client plans
    *   to send the submission request to multiple sequencers, it adds a suitable
    *   [[com.digitalasset.canton.sequencing.protocol.AggregationRule]] to the request for
    *   deduplication, unless one is already present. False disables amplification for this request
    *   independent of the configuration.
    * @param timestamps
    *   Aggregated timestamps needed for sending a request.
    */
  def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      timestamps: SendRequestTimestamps = defaultSendRequestTimestamps,
      messageId: MessageId = generateMessageId,
      aggregationRule: Option[AggregationRule] = None,
      callback: SendCallback = SendCallback.empty,
      amplify: Boolean = false,
      useConfirmationResponseAmplificationParameters: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): SendAsyncResult

  protected[canton] def clock: Clock

  /** Flattened version of [[sendAsync]]
    */
  def send(
      batch: Batch[DefaultOpenEnvelope],
      timestamps: SendRequestTimestamps = defaultSendRequestTimestamps,
      messageId: MessageId = generateMessageId,
      aggregationRule: Option[AggregationRule] = None,
      callback: SendCallback = SendCallback.empty,
      amplify: Boolean = false,
      useConfirmationResponseAmplificationParameters: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = sendAsync(
    batch = batch,
    timestamps = timestamps,
    messageId = messageId,
    aggregationRule = aggregationRule,
    callback = callback,
    amplify = amplify,
    useConfirmationResponseAmplificationParameters = useConfirmationResponseAmplificationParameters,
  ).value.flatMap(identity)

  private def defaultSendRequestTimestamps: SendRequestTimestamps = {
    val now = clock.now
    SendRequestTimestamps(
      topologyTimestamp = None,
      approximateTimestampForSigning = now,
      maxSequencingTime = generateMaxSequencingTime(now),
    )
  }

  /** Provides a value for max-sequencing-time to use with `sendAsync` if no better application-
    * provided timeout is available. Currently, this is a configurable offset from our clock. If
    * `referenceTimestamp` is provided, it is used as the reference timestamp to calculate the max
    * sequencing time.
    */
  def generateMaxSequencingTime(referenceTimestamp: CantonTimestamp): CantonTimestamp

  /** Generates a message id. The message id is only for correlation within this client and does not
    * need to be globally unique.
    */
  def generateMessageId: MessageId = MessageId.randomMessageId()

}

object SequencerClientSend {

  /** Aggregates all the timestamps needed when sending a request.
    *
    * @param topologyTimestamp
    *   An optional timestamp specifying the topology state to reference for this request.
    * @param approximateTimestampForSigning
    *   Defines a timestamp used for signing a request, overriding the timestamp of the selected
    *   topology. For submission requests (e.g., encrypted view messages), the topology is not yet
    *   fixed and only an approximate snapshot is available. Therefore, we use an approximate
    *   timestamp to better reflect the signer’s current time when determining which session signing
    *   key to use. The current local clock is typically a suitable choice.
    * @param maxSequencingTime
    *   The max sequencing time for the request.
    */
  final case class SendRequestTimestamps(
      topologyTimestamp: Option[CantonTimestamp],
      approximateTimestampForSigning: CantonTimestamp,
      maxSequencingTime: CantonTimestamp,
  )

}
