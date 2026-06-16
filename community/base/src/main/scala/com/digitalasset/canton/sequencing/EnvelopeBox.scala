// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.{Applicative, Traverse}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  DecompressedSequencedEvent,
  Envelope,
  GenBatch,
  SequencedEvent,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.tracing.Traced

/** Type class to manipulate envelopes inside their box. Specializes [[cats.Traverse]] to
  * [[protocol.Envelope]] arguments.
  */
trait EnvelopeBox[Box[+_ <: Envelope[?]]] {

  /** Make this private so that we don't arbitrarily change the contents of a
    * [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]] that has its serialization
    * memoized as cryptographic evidence.
    */
  private[sequencing] def traverse[G[_], A <: Envelope[?], B <: Envelope[?]](boxedEnvelope: Box[A])(
      f: A => G[B]
  )(implicit G: Applicative[G]): G[Box[B]]

  /** We can compose a [[cats.Traverse]] with an [[EnvelopeBox]], but not several [[EnvelopeBox]]es
    * due to the restriction to [[protocol.Envelope]]s in the type arguments.
    */
  type ComposedBox[Outer[+_], +A <: Envelope[?]] = Outer[Box[A]]

  def revCompose[OuterBox[+_]](implicit
      OuterBox: Traverse[OuterBox]
  ): EnvelopeBox[Lambda[`+A <: Envelope[_]` => ComposedBox[OuterBox, A]]] =
    new EnvelopeBox[Lambda[`+A <: Envelope[_]` => ComposedBox[OuterBox, A]]] {
      override private[sequencing] def traverse[G[_], A <: Envelope[?], B <: Envelope[?]](
          boxedEnvelope: OuterBox[Box[A]]
      )(f: A => G[B])(implicit G: Applicative[G]): G[OuterBox[Box[B]]] =
        OuterBox.traverse(boxedEnvelope)(innerBox => EnvelopeBox.this.traverse(innerBox)(f))
    }
}

object EnvelopeBox {

  def apply[Box[+_ <: Envelope[?]]](implicit Box: EnvelopeBox[Box]): EnvelopeBox[Box] = Box

  implicit val unsignedEnvelopeBox: EnvelopeBox[UnsignedEnvelopeBox] = {
    type TracedSeqWithCounterTraced[+A] = Traced[Seq[WithCounter[Traced[A]]]]
    EnvelopeBox[DecompressedSequencedEvent].revCompose(
      Traverse[Traced]
        .compose[Seq]
        .compose[WithCounter]
        .compose[Traced]: Traverse[TracedSeqWithCounterTraced]
    )
  }

  type OrdinarySequencedEventOf[+E <: Envelope[?]] = OrdinarySequencedEvent[Batch[E]]
  type IgnoredSequencedEventOf[+E <: Envelope[?]] = IgnoredSequencedEvent[Batch[E]]
  type PossiblyIgnoredSequencedEventOf[+E <: Envelope[?]] =
    PossiblyIgnoredSequencedEvent[Batch[E]]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def traverseOrdinarySequencedEvent[G[_], B <: GenBatch[?], C <: GenBatch[?]](
      ordinaryEvent: OrdinarySequencedEvent[B]
  )(f: B => G[C])(implicit G: Applicative[G]): G[OrdinarySequencedEvent[C]] = {
    val oldSignedEvent = ordinaryEvent.signedEvent
    G.map(oldSignedEvent.traverse(seqEvent => SequencedEvent.traverseBatch(seqEvent)(f))) {
      newSignedEvent =>
        if (newSignedEvent eq oldSignedEvent)
          ordinaryEvent.asInstanceOf[OrdinarySequencedEvent[C]]
        else
          OrdinarySequencedEvent(ordinaryEvent.counter, newSignedEvent)(ordinaryEvent.traceContext)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def traverseIgnoredSequencedEvent[G[_], B <: GenBatch[?], C <: GenBatch[?]](
      event: IgnoredSequencedEvent[B]
  )(f: B => G[C])(implicit G: Applicative[G]): G[IgnoredSequencedEvent[C]] =
    event.underlying match {
      case None => G.pure(event.asInstanceOf[IgnoredSequencedEvent[C]])
      case Some(signedEvent) =>
        G.map(signedEvent.traverse(seqEvent => SequencedEvent.traverseBatch(seqEvent)(f))) {
          newSignedEvent =>
            if (newSignedEvent eq signedEvent) event.asInstanceOf[IgnoredSequencedEvent[C]]
            else
              IgnoredSequencedEvent(
                event.timestamp,
                event.counter,
                Some(newSignedEvent),
                event.previousTimestamp,
              )(event.traceContext)
        }
    }

  implicit val ordinarySequencedEventEnvelopeBox: EnvelopeBox[OrdinarySequencedEventOf] =
    new EnvelopeBox[OrdinarySequencedEventOf] {
      override private[sequencing] def traverse[G[_], A <: Envelope[?], B <: Envelope[?]](
          ordinaryEvent: OrdinarySequencedEvent[Batch[A]]
      )(f: A => G[B])(implicit G: Applicative[G]): G[OrdinarySequencedEvent[Batch[B]]] =
        traverseOrdinarySequencedEvent[G, Batch[A], Batch[B]](ordinaryEvent)(_.traverse(f))
    }

  implicit val ignoredSequencedEventEnvelopeBox: EnvelopeBox[IgnoredSequencedEventOf] =
    new EnvelopeBox[IgnoredSequencedEventOf] {
      override private[sequencing] def traverse[G[_], A <: Envelope[?], B <: Envelope[?]](
          ignoredEvent: IgnoredSequencedEvent[Batch[A]]
      )(f: A => G[B])(implicit G: Applicative[G]): G[IgnoredSequencedEvent[Batch[B]]] =
        traverseIgnoredSequencedEvent[G, Batch[A], Batch[B]](ignoredEvent)(_.traverse(f))
    }

  implicit val possiblyIgnoredSequencedEventEnvelopeBox
      : EnvelopeBox[PossiblyIgnoredSequencedEventOf] =
    new EnvelopeBox[PossiblyIgnoredSequencedEventOf] {
      override private[sequencing] def traverse[G[_], A <: Envelope[?], B <: Envelope[?]](
          event: PossiblyIgnoredSequencedEvent[Batch[A]]
      )(f: A => G[B])(implicit G: Applicative[G]): G[PossiblyIgnoredSequencedEvent[Batch[B]]] =
        event match {
          case ignored: IgnoredSequencedEvent[Batch[A]] =>
            G.widen(traverseIgnoredSequencedEvent[G, Batch[A], Batch[B]](ignored)(_.traverse(f)))
          case ordinary: OrdinarySequencedEvent[Batch[A]] =>
            G.widen(traverseOrdinarySequencedEvent[G, Batch[A], Batch[B]](ordinary)(_.traverse(f)))
        }
    }

  private type TracedSeq[+A] = Traced[Seq[A]]
  implicit val ordinaryEnvelopeBox: EnvelopeBox[OrdinaryEnvelopeBox] =
    ordinarySequencedEventEnvelopeBox.revCompose(Traverse[Traced].compose[Seq]: Traverse[TracedSeq])

  implicit val possiblyIgnoredEnvelopeBox: EnvelopeBox[PossiblyIgnoredEnvelopeBox] =
    possiblyIgnoredSequencedEventEnvelopeBox.revCompose(
      Traverse[Traced].compose[Seq]: Traverse[TracedSeq]
    )
}
