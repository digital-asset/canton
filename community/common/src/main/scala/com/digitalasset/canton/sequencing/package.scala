// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.store.SequencedEventStore.{
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.tracing.Traced

import scala.concurrent.Future

package object sequencing {

  /** It is convenient to consider the envelopes and all the structure around the envelopes (the box).
    * [[EnvelopeBox]] defines type class operations to manipulate
    */
  type BoxedEnvelope[+Box[+_], +Env] = Box[Env]

  /** An application handler processes boxed envelopes and returns a [[HandlerResult]] */
  type ApplicationHandler[-Box[+_], -Env] = BoxedEnvelope[Box, Env] => HandlerResult

  /** A handler processes an event synchronously in the [[scala.concurrent.Future]]
    * and returns an [[AsyncResult]] that may be computed asynchronously by the contained future.
    * Asynchronous processing may run concurrently with later events' synchronous processing
    * and with asynchronous processing of other events.
    */
  type HandlerResult = FutureUnlessShutdown[AsyncResult]

  ///////////////////////////////
  // The boxes and their handlers
  ///////////////////////////////

  /** Default box for signed batches of events
    * The outer `Traced` contains a trace context for the entire batch.
    */
  type OrdinaryEnvelopeBox[+E] = Traced[Seq[OrdinarySequencedEvent[E]]]
  type OrdinaryApplicationHandler[-E] = ApplicationHandler[OrdinaryEnvelopeBox, E]

  /** Just a signature around the [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]
    * The term "raw" indicates that the trace context is missing.
    * Try to use the box [[OrdinarySerializedEvent]] instead.
    */
  type RawSignedContentEnvelopeBox[+Env] = SignedContent[SequencedEvent[Env]]

  /** A batch of traced protocol events (without a signature).
    * The outer `Traced` contains a trace context for the entire batch.
    */
  type UnsignedEnvelopeBox[+E] = Traced[Seq[Traced[SequencedEvent[E]]]]
  type UnsignedApplicationHandler[-E] = ApplicationHandler[UnsignedEnvelopeBox, E]
  type UnsignedProtocolEventHandler = UnsignedApplicationHandler[DefaultOpenEnvelope]

  /** Default box for `PossiblyIgnoredProtocolEvents`.
    * The outer `Traced` contains a trace context for the entire batch.
    */
  type PossiblyIgnoredEnvelopeBox[+E] = Traced[Seq[PossiblyIgnoredSequencedEvent[E]]]
  type PossiblyIgnoredApplicationHandler[-E] = ApplicationHandler[PossiblyIgnoredEnvelopeBox, E]

  ///////////////////////////////////
  // Serialized events in their boxes
  ///////////////////////////////////

  /** Default type for serialized events.
    * Contains trace context and signature.
    */
  type OrdinarySerializedEvent = BoxedEnvelope[OrdinarySequencedEvent, ClosedEnvelope]

  type PossiblyIgnoredSerializedEvent = BoxedEnvelope[PossiblyIgnoredSequencedEvent, ClosedEnvelope]

  type RawSignedContentSerializedEvent = BoxedEnvelope[RawSignedContentEnvelopeBox, ClosedEnvelope]

  /////////////////////////////////
  // Protocol events (deserialized)
  /////////////////////////////////

  /** Default type for deserialized events.
    * Includes a signature and a trace context.
    */
  type OrdinaryProtocolEvent = BoxedEnvelope[OrdinarySequencedEvent, DefaultOpenEnvelope]

  /** Deserialized event with optional payload. */
  type PossiblyIgnoredProtocolEvent =
    BoxedEnvelope[PossiblyIgnoredSequencedEvent, DefaultOpenEnvelope]

  /** Default type for deserialized events.
    * The term "raw" indicates that the trace context is missing.
    * Try to use `TracedProtocolEvent` instead.
    */
  type RawProtocolEvent = BoxedEnvelope[SequencedEvent, DefaultOpenEnvelope]

  /** Deserialized event with a trace context.
    * Use this when you are really sure that a signature will never be needed.
    */
  type TracedProtocolEvent = Traced[RawProtocolEvent]

  //////////////////////////////
  // Non-standard event handlers
  //////////////////////////////

  // Non-standard handlers do not return a `HandlerResult`

  /** Default type for handlers on serialized events with error reporting
    */
  type SerializedEventHandler[Err] = OrdinarySerializedEvent => Future[Either[Err, Unit]]
}
