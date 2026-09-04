// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.implicits.catsSyntaxAlternativeSeparate
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.data.ReassignmentRef
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SynchronizerRank}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.{
  ReassigningParticipantsComputation,
  ReassignmentValidation,
  ReassignmentValidationError,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.{ExecutionContext, Future}

import TransactionRoutingError.AutomaticReassignmentForTransactionFailure

private[routing] class SynchronizerRankComputation(
    participantId: ParticipantId,
    priorityOfSynchronizer: PhysicalSynchronizerId => Int,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import com.digitalasset.canton.util.ShowUtil.*

  def computeBestSynchronizerRank(
      synchronizerState: RoutingSynchronizerState,
      contracts: Seq[ContractData],
      readers: Set[LfPartyId],
      synchronizerIds: NonEmpty[Set[PhysicalSynchronizerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
    for {
      // The stakeholders do not depend on the candidate, so one lookup serves all of them.
      stakeholdersOfContractsToReassign <- getStakeholdersOfContracts(
        synchronizerState,
        contracts.filter(c => synchronizerIds.exists(_ != c.synchronizerId)),
      )
      rank <- computeBestSynchronizerRank(
        synchronizerState,
        contracts,
        stakeholdersOfContractsToReassign,
        readers,
        synchronizerIds,
      )
    } yield rank

  private def getStakeholdersOfContracts(
      synchronizerState: RoutingSynchronizerState,
      contracts: Seq[ContractData],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Map[LfContractId, Stakeholders]] =
    synchronizerState
      .getContractsStakeholders(contracts.map(_.id))
      .leftMap[TransactionRoutingError](unknownContracts =>
        AutomaticReassignmentForTransactionFailure.Failed(
          s"Cannot find contracts ${unknownContracts.mkString(", ")}"
        )
      )

  private def computeBestSynchronizerRank(
      synchronizerState: RoutingSynchronizerState,
      contracts: Seq[ContractData],
      stakeholdersOfContractsToReassign: Map[LfContractId, Stakeholders],
      readers: Set[LfPartyId],
      synchronizerIds: NonEmpty[Set[PhysicalSynchronizerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
    EitherT(
      // Avoid nesting asynchronous computation, `compute` uses a parallel traverse over `contracts`
      MonadUtil
        .sequentialTraverse(synchronizerIds)(targetSynchronizer =>
          compute(
            contracts,
            stakeholdersOfContractsToReassign,
            Target(targetSynchronizer),
            readers,
            synchronizerState,
          )
            .leftMap(targetSynchronizer -> _)
            .value
        )
        .map(_.separate)
        .map { case (failedRankings, successfulRankings) =>
          // Priority of synchronizer
          // Number of reassignments if we use this synchronizer
          // pick according to the least amount of reassignments
          successfulRankings.minOption.toRight(
            TransactionRoutingError.TopologyErrors.NoSynchronizerForSubmission
              .SynchronizerRankingFailed(
                failedRankings.map { case (synchronizerId, err) =>
                  synchronizerId -> err.cause
                }.toMap
              )
          )
        }
    )

  // Includes check that submitting party has a participant with submission rights on source and target synchronizer
  def compute(
      contracts: Seq[ContractData],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      readers: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
    for {
      stakeholdersOfContractsToReassign <- getStakeholdersOfContracts(
        synchronizerState,
        contracts.filter(_.synchronizerId != targetSynchronizer.unwrap),
      )
      rank <- compute(
        contracts,
        stakeholdersOfContractsToReassign,
        targetSynchronizer,
        readers,
        synchronizerState,
      )
    } yield rank

  private def compute(
      contracts: Seq[ContractData],
      stakeholdersOfContractsToReassign: Map[LfContractId, Stakeholders],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      readers: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] = {
    type SingleReassignment = ((LfPartyId, PhysicalSynchronizerId, Stakeholders), LfContractId)

    val contractsToReassign = contracts.filter(_.synchronizerId != targetSynchronizer.unwrap)

    for {
      targetSnapshot <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerState.getTopologySnapshotFor(targetSynchronizer)
      )

      reassignments <- MonadUtil
        .parTraverseWithLimit(Threading.detectNumberOfThreads(noTracingLogger))(
          contractsToReassign
        ) { c =>
          val contractAssignation = c.synchronizerId

          for {
            stakeholders <- EitherT.fromEither[FutureUnlessShutdown](
              stakeholdersOfContractsToReassign
                .get(c.id)
                .toRight[TransactionRoutingError](
                  AutomaticReassignmentForTransactionFailure.Failed(s"Cannot find contract ${c.id}")
                )
            )
            sourceSnapshot <- EitherT
              .fromEither[FutureUnlessShutdown](
                synchronizerState.getTopologySnapshotFor(contractAssignation)
              )
              .map(Source(_))
            submitter <- findReaderThatCanReassignContract(
              sourceSnapshot = sourceSnapshot,
              sourceSynchronizerId = Source(contractAssignation),
              targetSnapshot = targetSnapshot,
              targetSynchronizerId = targetSynchronizer,
              contract = c,
              stakeholders = stakeholders,
              readers = readers,
            ).mapK(FutureUnlessShutdown.outcomeK)
          } yield ((submitter, contractAssignation, stakeholders) -> c.id): SingleReassignment
        }
    } yield SynchronizerRank(
      reassignments
        .groupMap { case (batch, _) => batch } { case (_, contractId) => contractId }
        .view
        .mapValues(_.toSet)
        .toMap,
      priorityOfSynchronizer(targetSynchronizer.unwrap),
      targetSynchronizer.unwrap,
    )
  }

  private def findReaderThatCanReassignContract(
      sourceSnapshot: Source[TopologySnapshot],
      sourceSynchronizerId: Source[PhysicalSynchronizerId],
      targetSnapshot: Target[TopologySnapshot],
      targetSynchronizerId: Target[PhysicalSynchronizerId],
      contract: ContractData,
      stakeholders: Stakeholders,
      readers: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, LfPartyId] = {
    logger.debug(
      s"Computing submitter that can submit reassignment of ${contract.id} with stakeholders $stakeholders from $sourceSynchronizerId to $targetSynchronizerId. Candidates are: $readers"
    )

    // Building the unassignment requests lets us check whether contract can be reassigned to target synchronizer
    def go(
        readers: List[LfPartyId],
        errAccum: List[String] = List.empty,
    ): EitherT[Future, String, LfPartyId] =
      readers match {
        case Nil =>
          EitherT.leftT(
            show"Cannot reassign contract ${contract.id} from $sourceSynchronizerId to $targetSynchronizerId: ${errAccum
                .mkString(",")}"
          )
        case reader :: rest =>
          val result =
            for {
              _ <- ReassignmentValidation
                .checkSubmitter(
                  ReassignmentRef(contract.id),
                  sourceSnapshot,
                  reader,
                  participantId,
                  stakeholders.all,
                )
              _ <- new ReassigningParticipantsComputation(
                stakeholders = stakeholders,
                sourceSnapshot,
                targetSnapshot,
              ).compute.leftWiden[ReassignmentValidationError]
            } yield ()
          result
            .onShutdown(Left(ReassignmentValidationError.AbortedDueToShutdownOut(contract.id)))
            .biflatMap(
              left => go(rest, errAccum :+ show"Read $reader cannot reassign: $left"),
              _ => EitherT.rightT(reader),
            )
      }

    go(readers.intersect(stakeholders.all).toList).leftMap(errors =>
      AutomaticReassignmentForTransactionFailure.Failed(errors)
    )
  }
}
