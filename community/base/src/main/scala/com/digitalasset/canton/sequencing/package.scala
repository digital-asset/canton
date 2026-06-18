// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.SequencedEventError
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  DecompressedSequencedEvent,
  Envelope,
  GenBatch,
  SignedContent,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  ProcessingSequencedEvent,
  SequencedEventWithTraceContext,
}
import com.digitalasset.canton.tracing.Traced

package object sequencing {

  /** It is convenient to consider the envelopes and all the structure around the envelopes (the
    * box). [[EnvelopeBox]] defines type class operations to manipulate
    */
  type BoxedEnvelope[+Box[+_ <: Envelope[?]], +Env <: Envelope[?]] = Box[Env]

  /** A handler processes an event synchronously in the [[scala.concurrent.Future]] and returns an
    * [[AsyncResult]] that may be computed asynchronously by the contained future. Asynchronous
    * processing may run concurrently with later events' synchronous processing and with
    * asynchronous processing of other events.
    */
  type GenericHandlerResult[+A] = FutureUnlessShutdown[AsyncResult[A]]

  /** A specific GenericHandlerResult that returns [[UnthrottledAsync]] in the async computation.
    * The [[UnthrottledAsync]] is not subject to throttling in the sequencer client, but can be used
    * to hold additional async computation that happens after the [[AsyncResult]] completes.
    */
  type HandlerResult = FutureUnlessShutdown[AsyncResult[UnthrottledAsync]]

  /** An application handler that returns a HandlerResult
    */
  type UnthrottledApplicationHandler[-Box[+_ <: Envelope[?]], -E <: Envelope[?]] =
    ApplicationHandler[Box, E, UnthrottledAsync]

  ///////////////////////////////
  // The boxes and their handlers
  ///////////////////////////////

  /** Default box for signed batches of events The outer `Traced` contains a trace context for the
    * entire batch.
    */
  type OrdinaryEnvelopeBox[+E <: Envelope[?]] = Traced[Seq[EnvelopeBox.OrdinarySequencedEventOf[E]]]
  type SequencedEnvelopeBox[+E <: Envelope[?]] =
    Traced[Seq[SequencedEventWithTraceContext[Batch[E]]]]
  type OrdinaryApplicationHandler[-E <: Envelope[?]] =
    UnthrottledApplicationHandler[OrdinaryEnvelopeBox, E]
  type SequencedApplicationHandler[-E <: Envelope[?]] =
    UnthrottledApplicationHandler[SequencedEnvelopeBox, E]

  /** Just a signature around the [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]] The
    * term "raw" indicates that the trace context is missing. Try to use the box
    * [[OrdinarySerializedEvent]] instead.
    */
  type RawSignedContentEnvelopeBox[+Env <: Envelope[?]] =
    SignedContent[DecompressedSequencedEvent[Env]]

  /** A batch of traced protocol events (without a signature) with the assigned counter. The outer
    * `Traced` contains a trace context for the entire batch.
    */
  type UnsignedEnvelopeBox[+E <: Envelope[?]] =
    Traced[Seq[WithCounter[Traced[DecompressedSequencedEvent[E]]]]]
  type UnsignedApplicationHandler[-E <: Envelope[?]] =
    UnthrottledApplicationHandler[UnsignedEnvelopeBox, E]
  type UnsignedProtocolEventHandler = UnsignedApplicationHandler[DefaultOpenEnvelope]

  /** Default box for `PossiblyIgnoredProtocolEvents`. The outer `Traced` contains a trace context
    * for the entire batch.
    */
  type PossiblyIgnoredEnvelopeBox[+E <: Envelope[?]] =
    Traced[Seq[EnvelopeBox.PossiblyIgnoredSequencedEventOf[E]]]
  type PossiblyIgnoredApplicationHandler[-E <: Envelope[?]] =
    UnthrottledApplicationHandler[PossiblyIgnoredEnvelopeBox, E]

  ///////////////////////////////////
  // Serialized events in their boxes
  ///////////////////////////////////

  /** Default type for serialized events. Contains trace context and signature.
    */

  type ProcessingSerializedEvent = ProcessingSequencedEvent[Batch[ClosedEnvelope]]

  type SequencedSerializedEvent = SequencedEventWithTraceContext[Batch[ClosedEnvelope]]

  type MaybeCompressedSerializedEvent = SequencedEventWithTraceContext[GenBatch[ClosedEnvelope]]

  type OrdinarySerializedEvent = EnvelopeBox.OrdinarySequencedEventOf[ClosedEnvelope]

  type PossiblyIgnoredSerializedEvent =
    EnvelopeBox.PossiblyIgnoredSequencedEventOf[ClosedEnvelope]

  type OrdinaryEventOrError = Either[SequencedEventError, OrdinarySerializedEvent]

  type SequencedEventOrError = Either[SequencedEventError, SequencedSerializedEvent]

  /////////////////////////////////
  // Protocol events (deserialized)
  /////////////////////////////////

  /** Default type for deserialized events. Includes a signature and a trace context.
    */
  type SequencedProtocolEvent = SequencedEventWithTraceContext[Batch[DefaultOpenEnvelope]]

  type OrdinaryProtocolEvent = EnvelopeBox.OrdinarySequencedEventOf[DefaultOpenEnvelope]

  /** Deserialized event with optional payload. */
  type PossiblyIgnoredProtocolEvent =
    EnvelopeBox.PossiblyIgnoredSequencedEventOf[DefaultOpenEnvelope]

  /** Default type for deserialized events. The term "raw" indicates that the trace context is
    * missing. Try to use `TracedProtocolEvent` instead.
    */
  type RawProtocolEvent = BoxedEnvelope[DecompressedSequencedEvent, DefaultOpenEnvelope]

  /** Deserialized event with a trace context. Use this when you are really sure that a signature
    * will never be needed.
    */
  type TracedProtocolEvent = WithCounter[Traced[RawProtocolEvent]]

  //////////////////////////////
  // Non-standard event handlers
  //////////////////////////////

  // Non-standard handlers do not return a `HandlerResult`

  /** Default type for handlers on serialized events with error reporting
    */
  type OrdinaryEventHandler[Err] =
    OrdinarySerializedEvent => FutureUnlessShutdown[Either[Err, Unit]]
  type OrdinaryEventOrErrorHandler[Err] =
    OrdinaryEventOrError => FutureUnlessShutdown[Either[Err, Unit]]

  type SequencedEventHandler[Err] =
    SequencedSerializedEvent => FutureUnlessShutdown[Either[Err, Unit]]

  type MaybeCompressedSequencedEventHandler[Err] =
    MaybeCompressedSerializedEvent => FutureUnlessShutdown[Either[Err, Unit]]
  type SequencedEventOrErrorHandler[Err] =
    SequencedEventOrError => FutureUnlessShutdown[Either[Err, Unit]]
}
