// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.{
  ErrorWithInternalConsistencyCheck,
  InconsistentKeyMaintainers,
}
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfGlobalKey,
  LfTemplateId,
  LfTransaction,
  RollbackContext,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.transaction.{NodeId, TransactionError}

import scala.concurrent.ExecutionContext

trait InternalConsistencyChecker {

  protected def participantId: ParticipantId

  def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      mergedTransaction: LfTransaction,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ErrorWithInternalConsistencyCheck, Unit] =
    for {
      keyMaintainers <- EitherT.fromEither(
        MapsUtil
          .toNonConflictingMap(rootViewTrees.forgetNE.flatMap(_.view.keyMaintainers()))
          .leftMap(inconsistent =>
            ErrorWithInternalConsistencyCheck(InconsistentKeyMaintainers(inconsistent))
          )
      )
      allMaintainers = keyMaintainers.values.flatten.toSet
      hostedMaintainers <- EitherT.right[ErrorWithInternalConsistencyCheck](
        topologySnapshot.hostedOn(allMaintainers, participantId)
      )
      hostedKeys = keyMaintainers.toSeq.collect {
        case (key, maintainers) if maintainers.exists(hostedMaintainers.contains) => key
      }.toSet
      _ <- EitherT.fromEither(check(rootViewTrees, mergedTransaction, hostedKeys))
    } yield ()

  protected[validation] def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      mergedTransaction: LfTransaction,
      hostedKeys: Set[LfGlobalKey],
  )(implicit
      traceContext: TraceContext
  ): Either[ErrorWithInternalConsistencyCheck, Unit]

}

object InternalConsistencyChecker {

  def apply(
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  ): InternalConsistencyChecker =
    if (protocolVersion >= ProtocolVersion.v35) {
      new NextGenInternalConsistencyChecker(participantId, loggerFactory)
    } else {
      new LegacyInternalConsistencyChecker(participantId, loggerFactory)
    }

  trait Error extends PrettyPrinting

  object Error {
    def fromTransactionError(error: TransactionError): Error =
      error match {
        case TransactionError.EffectfulRollback(nodeIds) =>
          EffectfulRollbackError(nodeIds)
        case TransactionError.DuplicateContractId(cid) =>
          DuplicateContractIdError(cid)
        case TransactionError.AlreadyConsumed(cid, tmplId, nodeId) =>
          AlreadyConsumedContractError(cid, tmplId, nodeId.toString)
        case TransactionError.InconsistentContractKey(key) =>
          InconsistentContractKeyError(key)
      }
  }

  final case class ErrorWithInternalConsistencyCheck(error: Error) extends PrettyPrinting {
    override protected def pretty: Pretty[ErrorWithInternalConsistencyCheck] =
      prettyOfClass(
        unnamedParam(_.error)
      )
  }

  private[validation] type Result[R] = Either[ErrorWithInternalConsistencyCheck, R]

  case object Aborted extends Error {
    override protected def pretty: Pretty[Aborted.type] = prettyOfObject[Aborted.type]
  }

  final case class InconsistentKeyMaintainers(inconsistent: Map[LfGlobalKey, Set[Set[LfPartyId]]])
      extends Error {
    override protected def pretty: Pretty[InconsistentKeyMaintainers] = prettyOfClass(
      param("inconsistent", _ => inconsistent)
    )
  }

  final case class EffectfulRollbackError(nodeIds: Set[NodeId]) extends Error {
    override protected def pretty: Pretty[EffectfulRollbackError] =
      prettyOfClass(unnamedParam(_.nodeIds))
  }

  final case class DuplicateContractIdError(contractId: LfContractId) extends Error {
    override protected def pretty: Pretty[DuplicateContractIdError] =
      prettyOfClass(unnamedParam(_.contractId))
  }

  final case class AlreadyConsumedContractError(
      contractId: LfContractId,
      tmplId: LfTemplateId,
      nodeId: String,
  ) extends Error {
    override protected def pretty: Pretty[AlreadyConsumedContractError] = prettyOfClass(
      param("contractId", _.contractId),
      param("tmplId", _.tmplId),
      param("nodeId", _.nodeId.unquoted),
    )
  }

  final case class InconsistentContractKeyError(key: LfGlobalKey) extends Error {
    override protected def pretty: Pretty[InconsistentContractKeyError] =
      prettyOfClass(unnamedParam(_.key))
  }

  final case class IncorrectRollbackScopeOrder(error: String) extends Error {
    override protected def pretty: Pretty[IncorrectRollbackScopeOrder] = prettyOfClass(
      param("cause", _.error.unquoted)
    )
  }

  final case class UsedBeforeCreation(contractIds: NonEmpty[Set[LfContractId]]) extends Error {
    override protected def pretty: Pretty[UsedBeforeCreation] = prettyOfClass(
      param("contractIds", _.contractIds)
    )
  }

  final case class UsedAfterArchive(contractIds: NonEmpty[Set[LfContractId]]) extends Error {
    override protected def pretty: Pretty[UsedAfterArchive] = prettyOfClass(
      param("contractIds", _.contractIds)
    )
  }

  private[validation] def checkRollbackScopeOrder(
      presented: Seq[RollbackContext]
  ): Either[String, Unit] =
    Either.cond(
      presented == presented.sorted,
      (),
      s"Detected out of order rollback scopes in: $presented",
    )

  private[validation] def checkRollbackScopes(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  ): Result[Unit] =
    checkRollbackScopeOrder(
      rootViewTrees.map(_.viewParticipantData.rollbackContext)
    ).left.map { error =>
      ErrorWithInternalConsistencyCheck(IncorrectRollbackScopeOrder(error))
    }

  private[validation] def checkNotUsedBeforeCreation(
      previouslyReferenced: Set[LfContractId],
      newlyCreated: Set[LfContractId],
  ): Result[Unit] =
    NonEmpty.from(newlyCreated.intersect(previouslyReferenced)) match {
      case Some(ne) => Left(ErrorWithInternalConsistencyCheck(UsedBeforeCreation(ne)))
      case None => Either.unit
    }

  private[validation] def checkNotUsedAfterArchive(
      previouslyConsumed: Set[LfContractId],
      newlyReferenced: Set[LfContractId],
  ): Result[Unit] =
    NonEmpty.from(previouslyConsumed.intersect(newlyReferenced)) match {
      case Some(ne) => Left(ErrorWithInternalConsistencyCheck(UsedAfterArchive(ne)))
      case None => Either.unit
    }

}
