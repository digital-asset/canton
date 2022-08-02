// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0.ViewParticipantData
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy

import RollbackContext.{RollbackScope, RollbackSibling, firstChild}

/** RollbackContext tracks the location of lf transaction nodes or canton participant views within a hierarchy of
  * LfNodeRollback suitable for maintaining the local position within the hierarchy of rollback nodes when iterating
  * over a transaction.
  * @param rbScope   scope or path of sibling ordinals ordered "from the outside-in", e.g. [2, 1, 3] points via the
  *                  second rollback node to its first rollback node descendant to the latter's third rollback node
  *                  descendant where rollback node "levels" may be separated from the root and each other only by
  *                  non-rollback nodes.
  * @param nextChild the next sibling ordinal to assign to a newly encountered rollback node. This is needed on top of
  *                  rbScope in use cases in which the overall transaction structure is not known a priori.
  */
case class RollbackContext private (
    private val rbScope: Vector[RollbackSibling],
    private val nextChild: RollbackSibling = firstChild,
) extends NoCopy
    with PrettyPrinting {

  def enterRollback: RollbackContext = new RollbackContext(rbScope :+ nextChild)

  def exitRollback: RollbackContext = {
    val lastChild =
      rbScope.lastOption.getOrElse(
        throw new IllegalStateException("Attempt to exit rollback on empty rollback context")
      )

    new RollbackContext(rbScope.dropRight(1), lastChild + 1)
  }

  def rollbackScope: RollbackScope = rbScope

  def inRollback: Boolean = rbScope.nonEmpty

  def isEmpty: Boolean = equals(RollbackContext.empty)

  /** Checks if the rollback scope passed in embeds/contains or matches this rollback scope, i.e. if this rollback
    * scope starts with the provided rollback scope.
    */
  def embeddedInScope(rbScopeToCheck: RollbackScope): Boolean =
    rollbackScope.startsWith(rbScopeToCheck)

  def toProtoV0: ViewParticipantData.RollbackContext =
    ViewParticipantData.RollbackContext(rollbackScope = rollbackScope, nextChild = nextChild)

  override def pretty: Pretty[RollbackContext] = prettyOfClass(
    param("rollback scope", _.rollbackScope),
    param("next child", _.nextChild),
  )
}

case class WithRollbackScope[T](rbScope: RollbackScope, unwrap: T)

object RollbackContext {
  type RollbackSibling = Int
  val firstChild: RollbackSibling = 1

  type RollbackScope = Seq[RollbackSibling]

  object RollbackScope {
    def empty: RollbackScope = Vector.empty[RollbackSibling]

    def popsAndPushes(origin: RollbackScope, target: RollbackScope): (Int, Int) = {
      val longestCommonRollbackScopePrefixLength =
        origin.lazyZip(target).takeWhile { case (i, j) => i == j }.size

      val rbPops = origin.length - longestCommonRollbackScopePrefixLength
      val rbPushes = target.length - longestCommonRollbackScopePrefixLength
      (rbPops, rbPushes)
    }
  }

  private def apply(rbScope: RollbackScope, nextChild: RollbackSibling) =
    throw new UnsupportedOperationException("Use one of the other builders")

  def empty: RollbackContext = new RollbackContext(Vector.empty)

  def apply(scope: RollbackScope): RollbackContext = new RollbackContext(scope.toVector)

  def fromProtoV0(
      maybeRbContext: Option[
        com.digitalasset.canton.protocol.v0.ViewParticipantData.RollbackContext
      ]
  ): ParsingResult[RollbackContext] =
    maybeRbContext.fold(Either.right[ProtoDeserializationError, RollbackContext](empty)) {
      case com.digitalasset.canton.protocol.v0.ViewParticipantData
            .RollbackContext(rbScope, nextChild) =>
        import cats.syntax.traverse._

        val rbScopeVector = rbScope.toVector

        for {
          _ <- Either.cond(
            nextChild > 0,
            (),
            ProtoDeserializationError.ValueConversionError(
              "next_child",
              s"Next rollback child must be positive and not ${nextChild}",
            ),
          )
          _ <- rbScopeVector.traverse { i =>
            Either.cond(
              i > 0,
              (),
              ProtoDeserializationError.ValueConversionError(
                "rollback_scope",
                s"Rollback scope elements must be positive and not ${i}",
              ),
            )
          }
        } yield new RollbackContext(rbScopeVector, nextChild)
    }

}
