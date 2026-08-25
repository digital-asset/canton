// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.InvalidProtocolBufferException
import scalapb.CollectionAdapter

import scala.collection.{IterableOps, mutable}
import scala.language.implicitConversions

/** The type a `repeated` proto field maps to. Exposes the length but not the elements: a collection
  * must not be processed before its length is checked, so the elements are reachable only through
  * [[ProtoValidation.validateLength]] and the entry points built on it.
  */
final class ProtoUnvalidatedSeq[+E](private[validation] val elements: Seq[E]) extends AnyVal {
  def nonEmpty: Boolean = elements.nonEmpty

  def size: Int = elements.size

  def sizeIs: IterableOps.SizeCompareOps = elements.sizeIs
}

object ProtoUnvalidatedSeq {
  def apply[E](elements: Seq[E]): ProtoUnvalidatedSeq[E] = new ProtoUnvalidatedSeq(elements)

  /** Writing a trusted collection out is safe, so `toProto` builders may pass a plain `Seq`. */
  implicit def fromSeq[E](elements: Seq[E]): ProtoUnvalidatedSeq[E] = apply(elements)

  /** As `fromSeq` for a `NonEmpty` collection, which `fromSeq` cannot see: `NonEmpty` carries no
    * upper bound. Without it every write site needs a `.forgetNE` that says nothing about the
    * field.
    */
  implicit def fromNonEmpty[E](elements: NonEmpty[Seq[E]]): ProtoUnvalidatedSeq[E] =
    apply(elements.forgetNE)

  /** scalapb hook named by the `collection.adapter` option, there for generated (de)serializers and
    * nothing else. MUST NOT be used directly: it hands out the elements with no length check, so
    * every per-element step after it runs unbounded; read through
    * [[ProtoValidation.validateLength]] instead. Generated code shares the `canton` package tree,
    * so this is a convention, not a visibility guarantee.
    *
    * One adapter serves every element type: `E` is inferred from the field's expected
    * `CollectionAdapter[E, ProtoUnvalidatedSeq[E]]`.
    */
  object Adapter {
    def apply[E](): CollectionAdapter[E, ProtoUnvalidatedSeq[E]] = new Impl[E]
  }

  private final class Impl[E] extends CollectionAdapter[E, ProtoUnvalidatedSeq[E]] {

    /** MUST NOT be used directly: handing out the elements skips the length check that guards every
      * per-element step after it.
      */
    override def foreach(coll: ProtoUnvalidatedSeq[E])(f: E => Unit): Unit =
      coll.elements.foreach(f)

    override def empty: ProtoUnvalidatedSeq[E] = new ProtoUnvalidatedSeq(Seq.empty)

    override def newBuilder: mutable.Builder[
      E,
      Either[InvalidProtocolBufferException, ProtoUnvalidatedSeq[E]],
    ] = Seq.newBuilder[E].mapResult(s => Right(new ProtoUnvalidatedSeq(s)))

    override def concat(
        coll: ProtoUnvalidatedSeq[E],
        other: Iterable[E],
    ): ProtoUnvalidatedSeq[E] = new ProtoUnvalidatedSeq(coll.elements ++ other)

    /** MUST NOT be used directly, for the reason on `foreach`. */
    override def toIterator(coll: ProtoUnvalidatedSeq[E]): Iterator[E] = coll.elements.iterator

    override def size(coll: ProtoUnvalidatedSeq[E]): Int = coll.size
  }
}
