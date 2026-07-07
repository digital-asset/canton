// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.PathRollbackContext.{RollbackSibling, firstChild}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.Ordering.Implicits.*
import scala.math.Ordered.orderingToOrdered

sealed trait RollbackScope {
  def inRollback: Boolean
}

object RollbackScope {
  def tryRollbackEffects(subview: RollbackScope, current: RollbackScope): Boolean =
    (subview, current) match {
      case (s: PathRollbackScope, c: PathRollbackScope) =>
        c != s
      case (_: NoPathRollbackScope, _: NoPathRollbackScope) =>
        // From PV35 rollbacks must be un-effectful so no need to rewrite
        false
      case _ =>
        throw new IllegalArgumentException(
          s"Mismatching scope types: ${subview.getClass} and ${current.getClass}"
        )
    }
}

final case class WithRollbackScope[T](rbScope: RollbackScope, unwrap: T)

sealed trait RollbackContext extends PrettyPrinting {
  def rollbackScope: RollbackScope
  def inRollback: Boolean
}

final case class NoPathRollbackScope(inRollback: Boolean) extends RollbackScope
final case class NoPathRollbackContext(inRollback: Boolean) extends RollbackContext {
  override def rollbackScope: RollbackScope = NoPathRollbackScope(inRollback = inRollback)
  override protected def pretty: Pretty[NoPathRollbackContext] =
    prettyOfClass(
      paramIfTrue("in rollback", _.inRollback)
    )
}
object NoPathRollbackContext {
  val empty: NoPathRollbackContext = NoPathRollbackContext(inRollback = false)
}

final case class PathRollbackScope(path: Seq[RollbackSibling]) extends RollbackScope {
  override def inRollback: Boolean = path.nonEmpty
}

object PathRollbackScope {
  def empty: PathRollbackScope = PathRollbackScope(Vector.empty[RollbackSibling])

  def popsAndPushes(origin: PathRollbackScope, target: PathRollbackScope): (Int, Int) = {
    val longestCommonRollbackScopePrefixLength =
      origin.path.lazyZip(target.path).takeWhile { case (i, j) => i == j }.size

    val rbPops = origin.path.length - longestCommonRollbackScopePrefixLength
    val rbPushes = target.path.length - longestCommonRollbackScopePrefixLength
    (rbPops, rbPushes)
  }

  def tryToPathRollbackScope(context: RollbackScope): PathRollbackScope = context match {
    case p: PathRollbackScope => p
    case other =>
      throw new IllegalArgumentException(
        s"Expected a PathRollbackContext.RbScope, but got ${other.getClass}: $other"
      )
  }

}

/** PathRollbackContext tracks the location of lf transaction nodes or canton participant views
  * within a hierarchy of LfNodeRollback suitable for maintaining the local position within the
  * hierarchy of rollback nodes when iterating over a transaction.
  * @param path
  *   scope or path of sibling ordinals ordered "from the outside-in", e.g. [2, 1, 3] points via the
  *   second rollback node to its first rollback node descendant to the latter's third rollback node
  *   descendant where rollback node "levels" may be separated from the root and each other only by
  *   non-rollback nodes.
  * @param nextChild
  *   the next sibling ordinal to assign to a newly encountered rollback node. This is needed on top
  *   of rbScope in use cases in which the overall transaction structure is not known a priori.
  */
final case class PathRollbackContext(
    path: Vector[RollbackSibling],
    nextChild: RollbackSibling,
) extends RollbackContext
    with PrettyPrinting
    with Ordered[PathRollbackContext] {

  def enterRollback: PathRollbackContext = PathRollbackContext(path :+ nextChild, firstChild)

  def tryExitRollback: PathRollbackContext = {
    val lastChild =
      path.lastOption.getOrElse(
        throw new IllegalStateException("Attempt to exit rollback on empty rollback context")
      )

    PathRollbackContext(path.dropRight(1), lastChild.increment)
  }

  def rollbackScope: PathRollbackScope = PathRollbackScope(path)

  def inRollback: Boolean = path.nonEmpty

  def isEmpty: Boolean = equals(PathRollbackContext.empty)

  def toProtoV30: v30.ViewParticipantData.RollbackContext =
    v30.ViewParticipantData.RollbackContext(
      rollbackScope = rollbackScope.path.map(_.unwrap),
      nextChild = nextChild.unwrap,
    )

  override protected def pretty: Pretty[PathRollbackContext] = prettyOfClass(
    param("rollback scope", _.rollbackScope.path),
    param("next child", _.nextChild),
  )

  private lazy val sortKey: Vector[PositiveInt] = path :+ nextChild
  override def compare(that: PathRollbackContext): Int = sortKey.compare(that.sortKey)

}

object PathRollbackContext {
  type RollbackSibling = PositiveInt
  val firstChild: RollbackSibling = PositiveInt.one

  val empty: PathRollbackContext = PathRollbackContext(Vector.empty, firstChild)

  def fromProtoV30(
      maybeRbContext: Option[v30.ViewParticipantData.RollbackContext]
  ): ParsingResult[PathRollbackContext] =
    maybeRbContext.fold(Either.right[ProtoDeserializationError, PathRollbackContext](empty)) {
      case v30.ViewParticipantData.RollbackContext(rbScope, nextChildP) =>
        for {
          nextChild <- PositiveInt
            .create(nextChildP)
            .leftMap(_ =>
              ProtoDeserializationError.ValueConversionError(
                "next_child",
                s"positive value expected; found: $nextChildP",
              )
            )

          rbScopeVector <- rbScope.toVector.zipWithIndex
            .traverse { case (value, idx) =>
              PositiveInt.create(value).leftMap { _ =>
                s"positive value expected; found $value at position $idx"
              }
            }
            .leftMap(
              ProtoDeserializationError.ValueConversionError("rollback_scope", _)
            )

        } yield new PathRollbackContext(rbScopeVector, nextChild)
    }

  def tryToPathRollbackContext(context: RollbackContext): PathRollbackContext = context match {
    case p: PathRollbackContext => p
    case other =>
      throw new IllegalArgumentException(
        s"Expected a PathRollbackContext, but got ${other.getClass}: $other"
      )
  }

}
