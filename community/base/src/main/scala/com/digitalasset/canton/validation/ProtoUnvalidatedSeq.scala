// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.InvalidProtocolBufferException
import pprint.Tree
import scalapb.CollectionAdapter

import scala.collection.{IterableOps, mutable}
import scala.language.implicitConversions

/** The type a `repeated` proto field maps to. Exposes the length but not the elements: a collection
  * must not be processed before its length is checked, so the elements are reachable only through
  * [[ProtoValidation.validateLength]] and the entry points built on it. Pretty printing is the one
  * exception, and caps what it renders itself.
  */
final class ProtoUnvalidatedSeq[+E](private[validation] val elements: Seq[E])
    extends AnyVal
    with PrettyPrintingFromCompanion {
  def nonEmpty: Boolean = elements.nonEmpty

  def size: Int = elements.size

  def sizeIs: IterableOps.SizeCompareOps = elements.sizeIs

  override def prettyCompanion: PrettyPrintingCompanion[ProtoUnvalidatedSeq[Any]] =
    ProtoUnvalidatedSeq
}

object ProtoUnvalidatedSeq extends PrettyPrintingCompanion[ProtoUnvalidatedSeq[Any]] {

  /** How many elements a printer may render: a repeated field's length is unvalidated input. Set to
    * the default printer's height, which cuts a longer collection off anyway.
    */
  val MaxPrettyElements: Int = Pretty.DefaultHeight

  def apply[E](elements: Seq[E]): ProtoUnvalidatedSeq[E] = new ProtoUnvalidatedSeq(elements)

  /** `Seq(...)` over at most [[MaxPrettyElements]] elements, marked as cut off when there are more,
    * and a single element collapsed to itself.
    *
    * @param treeOfElement
    *   renders one element; a printer passes its own so that its limits reach the elements
    */
  private[canton] def prettyTree[E](seq: ProtoUnvalidatedSeq[E])(treeOfElement: E => Tree): Tree = {
    val shown = seq.elements.take(MaxPrettyElements)
    // `sizeIs` stops at the cap, so an unvalidated length costs nothing; counting the rest for the
    // marker would walk a List to the end
    val truncated = seq.sizeIs > MaxPrettyElements
    shown match {
      case Seq(single) if !truncated => treeOfElement(single)
      case _ =>
        val marker = Option.when(truncated)(Tree.Literal("... more"))
        Tree.Apply("Seq", shown.iterator.map(treeOfElement) ++ marker)
    }
  }

  override protected val pretty: Pretty[ProtoUnvalidatedSeq[Any]] = prettyTree(_)(treeOfElement)

  /** An element's type is erased, so render it as the default printer does: through its own
    * instance if it has one, structurally otherwise.
    */
  private def treeOfElement(element: Any): Tree =
    Pretty.DefaultPprinter.treeify(
      element,
      escapeUnicode = Pretty.DefaultEscapeUnicode,
      showFieldNames = Pretty.DefaultShowFieldNames,
    )

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
