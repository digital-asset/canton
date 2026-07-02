// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.checked
import com.digitalasset.canton.data.{FullTransactionViewTree, PathRollbackContextFactory}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.*
import com.digitalasset.canton.participant.protocol.validation.LegacyInternalConsistencyChecker.*
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfGlobalKey,
  LfTransaction,
  PathRollbackContext,
  PathRollbackScope,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.nonempty.NonEmpty

import scala.annotation.tailrec

class LegacyInternalConsistencyChecker(
    override val participantId: ParticipantId,
    override val loggerFactory: NamedLoggerFactory,
) extends InternalConsistencyChecker
    with NamedLogging {

  /** Checks if there is no internal consistency issue between views, e.g., it would return an error
    * if there are two different views (within the same rollback scope) that archive the same
    * contract.
    *
    * The method does not check for consistency issues inside of a single view. This is checked by
    * Daml engine as part of [[ModelConformanceChecker]].
    */
  override def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      unmergedTransactionsWithoutTopLevelRollbackNodes: Seq[LfTransaction],
      hostedKeys: Set[LfGlobalKey],
  )(implicit
      traceContext: TraceContext
  ): Either[ErrorWithInternalConsistencyCheck, Unit] =
    for {
      _ <- checkRollbackScopes(rootViewTrees)
      _ <- checkContractState(rootViewTrees)
    } yield ()

  private def checkRollbackScopes(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  ): Result[Unit] =
    PathRollbackContextFactory
      .checkRollbackScopeOrder(
        rootViewTrees.map(_.viewParticipantData.rollbackContext)
      )
      .left
      .map { error =>
        ErrorWithInternalConsistencyCheck(IncorrectRollbackScopeOrder(error))
      }

  private def checkContractState(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  )(implicit traceContext: TraceContext): Result[Unit] =
    MonadUtil
      .foldLeftM[Result, ContractState, FullTransactionViewTree](
        ContractState.empty,
        rootViewTrees,
      ) { (previous, rootViewTree) =>
        val state = adjustRollbackScope[ContractState, Set[LfContractId]](
          previous,
          checked(
            PathRollbackContext.tryToPathRollbackContext(
              rootViewTree.viewParticipantData.tryUnwrap.rollbackContext
            )
          ).rollbackScope,
        )

        val created = rootViewTree.view.createdContracts.keySet
        val input = rootViewTree.view.inputContracts.keySet
        val consumed = rootViewTree.view.consumed.keySet

        val referenced = created ++ input

        for {
          _ <- checkNotUsedBeforeCreation(state.referenced, created)
          _ <- checkNotUsedAfterArchive(state.consumed, referenced)
        } yield state.update(referenced = referenced, consumed = consumed)

      }
      .map(_.discard)

}

object LegacyInternalConsistencyChecker {

  private final case class WithPathRollbackScope[T](
      val rollbackScope: PathRollbackScope,
      val activeState: T,
  )

  /** This trait manages pushing the active state onto a stack when a new rollback context is
    * entered and restoring the rollback back active state when a rollback scope is exited.
    *
    * It is assumed that not all rollback scopes will be presented to [[adjustRollbackScope]] in
    * order but there may be hierarchical jumps in rollback scopt between calls.
    */
  private sealed trait PushPopRollbackScope[M <: PushPopRollbackScope[M, T], T] {

    def self: M

    def rollbackScope: PathRollbackScope

    def stack: List[WithPathRollbackScope[T]]

    /** State that will be recorded / restored at the beginning / end of a rollback scope. */
    def activeRollbackState: T

    def copyWith(
        rollbackScope: PathRollbackScope,
        activeState: T,
        stack: List[WithPathRollbackScope[T]],
    ): M

    private[validation] def pushRollbackScope(newScope: PathRollbackScope): M =
      copyWith(
        newScope,
        activeRollbackState,
        WithPathRollbackScope(
          rollbackScope,
          activeRollbackState,
        ) :: stack,
      )

    private[validation] def popRollbackScope(): M = stack match {
      case WithPathRollbackScope(stackScope, stackActive) :: other =>
        copyWith(rollbackScope = stackScope, activeState = stackActive, stack = other)
      case _ =>
        throw new IllegalStateException(
          s"Unable to pop scope of empty stack ${getClass.getSimpleName}"
        )
    }
  }

  private final def adjustRollbackScope[M <: PushPopRollbackScope[M, T], T](
      starting: M,
      targetScope: PathRollbackScope,
  ): M = {
    @tailrec def loop(current: M): M =
      PathRollbackScope.popsAndPushes(
        current.rollbackScope,
        targetScope,
      ) match {
        case (0, 0) => current
        case (0, _) => current.pushRollbackScope(targetScope)
        case _ => loop(current.popRollbackScope())
      }

    loop(starting)
  }

  /** @param referenced
    *   Contract ids used or created by previous views, including rolled back usages and creations.
    * @param rollbackScope
    *   The current rollback scope
    * @param consumed
    *   Contract ids consumed in a previous view
    * @param stack
    *   The stack of rollback scopes that are currently open
    */
  private final case class ContractState(
      referenced: Set[LfContractId],
      rollbackScope: PathRollbackScope,
      consumed: Set[LfContractId],
      stack: List[WithPathRollbackScope[Set[LfContractId]]],
  ) extends PushPopRollbackScope[ContractState, Set[LfContractId]] {

    override def self: ContractState = this

    override def activeRollbackState: Set[LfContractId] = consumed

    override def copyWith(
        rollbackScope: PathRollbackScope,
        activeState: Set[LfContractId],
        stack: List[WithPathRollbackScope[Set[LfContractId]]],
    ): ContractState = copy(rollbackScope = rollbackScope, consumed = activeState, stack = stack)

    def update(referenced: Set[LfContractId], consumed: Set[LfContractId]): ContractState =
      copy(referenced = this.referenced ++ referenced, consumed = this.consumed ++ consumed)

  }

  private object ContractState {
    val empty: ContractState = ContractState(Set.empty, PathRollbackScope.empty, Set.empty, Nil)
  }

}
