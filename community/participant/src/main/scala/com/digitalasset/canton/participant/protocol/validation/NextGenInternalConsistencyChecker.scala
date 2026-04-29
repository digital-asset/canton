// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.*
import com.digitalasset.canton.participant.protocol.validation.NextGenInternalConsistencyChecker.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine.LLState
import com.digitalasset.daml.lf.transaction.{ErrOr, NextGenContractStateMachine, TransactionError}

class NextGenInternalConsistencyChecker(
    override val participantId: ParticipantId,
    override val loggerFactory: NamedLoggerFactory,
) extends InternalConsistencyChecker
    with NamedLogging {

  override def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      mergedTransaction: LfTransaction,
      hostedKeys: Set[LfGlobalKey],
  )(implicit
      traceContext: TraceContext
  ): Either[ErrorWithInternalConsistencyCheck, Unit] =
    for {
      _ <- checkRollbackScopes(rootViewTrees)
      _ <- checkContractState(rootViewTrees)
      _ <- checkKeyState(hostedKeys, Seq(mergedTransaction))
    } yield ()

  private[validation] def checkContractState(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  )(implicit traceContext: TraceContext): Result[Unit] =
    MonadUtil
      .foldLeftM[Result, ContractState, FullTransactionViewTree](
        ContractState.empty,
        rootViewTrees,
      ) { (state, rootViewTree) =>
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

  private[validation] def checkKeyState(
      hostedKeys: Set[LfGlobalKey],
      txs: Seq[LfTransaction],
  ): Result[Unit] = {
    val init: LLState = NextGenContractStateMachine.empty()
    val errOr =
      MonadUtil.foldLeftM(init, txs)((csm, tx) => handleTx(hostedKeys, csm, tx)).map(_ => ())
    errOr.leftMap(err =>
      ErrorWithInternalConsistencyCheck(InternalConsistencyChecker.Error.fromTransactionError(err))
    )
  }

  private def handleTx(keys: Set[LfGlobalKey], init: LLState, tx: LfTransaction): ErrOr[LLState] =
    tx.fold(init.asRight[TransactionError]) { case (acc, (nodeId, node)) =>
      acc.flatMap { csm =>
        node match {
          case action: LfActionNode if action.keyOpt.exists(k => keys.contains(k.globalKey)) =>
            csm.handleNode(nodeId, action)
          case _: LfActionNode | _: LfNodeRollback =>
            csm.asRight
        }
      }
    }

}

object NextGenInternalConsistencyChecker {

  private final case class ContractState(
      referenced: Set[LfContractId],
      consumed: Set[LfContractId],
  ) {

    def update(
        referenced: Set[LfContractId],
        consumed: Set[LfContractId],
    ): ContractState =
      copy(
        referenced = this.referenced ++ referenced,
        consumed = this.consumed ++ consumed,
      )
  }

  private object ContractState {
    val empty: ContractState = ContractState(Set.empty, Set.empty)
  }

}
