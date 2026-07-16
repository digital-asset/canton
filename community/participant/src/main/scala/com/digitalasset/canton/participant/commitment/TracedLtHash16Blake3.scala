// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.crypto.LtHash16Blake3.LtHash16Blake3Impl
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.participant.digest.DigestOperation
import com.digitalasset.canton.util.{ShowUtil, SnapshottableVector}

/** A container for tracking changes applied to an ACS digest. Since the digest itself is mutable
  * and is intended to be updated in-place, also the traced changes are kept in a mutable data
  * structure.
  */
sealed trait TracedLtHash16Blake3 extends PrettyPrinting {

  /** The digest itself */
  def digest: LtHash16Blake3

  /** The changes that were applied to the digest */
  protected val mutableTrace: SnapshottableVector[TraceElement]

  /** Getter for the traced changes.
    */
  def trace: Option[AcsDigestTrace] = {
    val snapshot = mutableTrace.snapshot
    Option.when(snapshot.nonEmpty)(AcsDigestTrace(snapshot))
  }

  /** Like [[com.digitalasset.canton.crypto.LtHash16Blake3.add]], except trace data can be provided
    * to describe the change.
    */
  def add(
      bytes: Array[Byte],
      traceData: Option[SingleTrace],
  ): Unit = {
    digest.add(bytes)
    mutableTrace.addAll(traceData)
  }

  /** Like [[com.digitalasset.canton.crypto.LtHash16Blake3.remove]], except trace data can be
    * provided to describe the change.
    */
  def remove(
      bytes: Array[Byte],
      traceData: Option[SingleTrace],
  ): Unit = {
    digest.remove(bytes)
    mutableTrace.addAll(traceData)
  }

  /** Like [[com.digitalasset.canton.crypto.LtHash16Blake3.union]], but will append the trace
    * elements of `other` to this digest's trace elements.
    */
  def union(other: TracedLtHash16Blake3): Unit = {
    digest.union(other.digest)
    mutableTrace.addAll(other.mutableTrace.snapshot)
  }

  /** Like [[com.digitalasset.canton.crypto.LtHash16Blake3.removeAll]], but will append the trace
    * elements of `other` to this digest's trace elements.
    */
  def removeAll(other: TracedLtHash16Blake3): Unit = {
    digest.removeAll(other.digest)
    mutableTrace.addAll(other.mutableTrace.snapshot)
  }

  /** Flattens groups into a sequence of single traces.
    */
  private def flattenedTraceGroups: Seq[SingleTrace] =
    trace.toList.flatMap(_.traces).flatMap {
      case group: TraceGroup => group.traces
      case single: SingleTrace => Seq(single)
    }

  def asBulk(description: String, operation: DigestOperation): TracedLtHash16Blake3 =
    operation match {
      case DigestOperation.Add => asBulkAddition(description)
      case DigestOperation.Remove => asBulkRemoval(description)
    }

  /** @return
    *   a traced hash, containing the same digest but with all traced changes wrapped in
    *   `TraceGroup(_, addedToHash = true)` in preparation for this digest to be unioned with
    *   another digest (e.g. for party onboarding).
    */
  def asBulkAddition(description: String): TracedLtHash16Blake3 =
    if (mutableTrace.snapshot.nonEmpty)
      TracedLtHash16Blake3(
        digest,
        Seq(TraceGroup(description, flattenedTraceGroups, addedToHash = true)),
      )
    else this

  /** @return
    *   a traced hash, containing the same digest but with all traced changes wrapped in
    *   `TraceGroup(_, addedToHash = false)` in preparation for this digest to be unioned with
    *   another digest (e.g. for party onboarding).
    */
  def asBulkRemoval(description: String): TracedLtHash16Blake3 =
    if (mutableTrace.snapshot.nonEmpty)
      TracedLtHash16Blake3(
        digest,
        Seq(TraceGroup(description, flattenedTraceGroups, addedToHash = false)),
      )
    else this

  override protected def pretty: Pretty[TracedLtHash16Blake3] = TracedLtHash16Blake3.pretty
}

object TracedLtHash16Blake3 extends PrettyUtil with ShowUtil {

  /** Factory method for an empty hash with an empty trace.
    */
  def empty: TracedLtHash16Blake3 = TracedLtHash16Blake3Impl.empty

  def apply(digest: LtHash16Blake3, traceData: Seq[TraceElement]): TracedLtHash16Blake3 =
    digest match {
      case impl: LtHash16Blake3Impl => TracedLtHash16Blake3Impl(impl, traceData)
    }

  def apply(digest: LtHash16Blake3, tracedChanges: Option[AcsDigestTrace]): TracedLtHash16Blake3 =
    digest match {
      case impl: LtHash16Blake3Impl => TracedLtHash16Blake3Impl(impl, tracedChanges)
    }

  val pretty: Pretty[TracedLtHash16Blake3] = prettyOfClass(
    param("digest", _.digest.hexString().unquoted),
    paramIfDefined("trace", _.trace),
  )
}

private[commitment] class TracedLtHash16Blake3Impl private (
    override val digest: LtHash16Blake3Impl,
    override protected val mutableTrace: SnapshottableVector[TraceElement],
) extends TracedLtHash16Blake3 {

  def addAll(trace: IterableOnce[TraceElement]): Unit =
    mutableTrace.addAll(trace)

  /** Equality is based on the digest, not the traced changes.
    */
  override def equals(obj: Any): Boolean = obj match {
    case traced: TracedLtHash16Blake3Impl => this.digest == traced.digest
    case _ => false
  }

  override def hashCode(): Int = digest.hashCode()
}

private[commitment] object TracedLtHash16Blake3Impl {

  def empty: TracedLtHash16Blake3Impl = apply(LtHash16Blake3.empty, Seq.empty)

  def apply(digest: LtHash16Blake3Impl, traceData: Seq[TraceElement]): TracedLtHash16Blake3Impl =
    new TracedLtHash16Blake3Impl(digest, SnapshottableVector.from(traceData))

  def apply(
      digest: LtHash16Blake3Impl,
      tracedChanges: Option[AcsDigestTrace],
  ): TracedLtHash16Blake3Impl =
    TracedLtHash16Blake3Impl(digest, tracedChanges.toList.flatMap(_.traces))
}
