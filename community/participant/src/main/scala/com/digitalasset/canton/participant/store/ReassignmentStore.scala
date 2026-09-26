// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.UnassignmentData.{
  AssignmentGlobalOffset,
  ReassignmentGlobalOffset,
  UnassignmentGlobalOffset,
}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  Offset,
  UnassignmentData,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  TopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateLookup
import com.digitalasset.canton.platform.indexer.parallel.ReassignmentOffsetPersistence
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{CheckedT, EitherTUtil, MonadUtil}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

trait ReassignmentStore extends ReassignmentLookup {
  import ReassignmentStore.*

  /** Adds the unassignment data to the store.
    *
    * Calls to this method are idempotent, independent of the order.
    * @throws java.lang.IllegalArgumentException
    *   if the reassignment's target synchronizer is not the synchronizer this [[ReassignmentStore]]
    *   belongs to.
    */
  def addUnassignmentData(unassignmentData: UnassignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given offsets to the reassignment data in the store.
    *
    * The same offset can be added any number of times.
    */
  def addReassignmentsOffsets(offsets: Map[ReassignmentId, ReassignmentGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Handles the event offboarding parties `parties` at offset `offset`.
    * @param offset
    *   Offset of the offboarding event
    * @param ts
    *   Effective time of the offboarding
    * @param parties
    *   Parties that are offboarded
    *
    * Summary: the offboarding event might make some of the incomplete reassignments not incomplete
    * anymore because the requirement for a reassignment to be incomplete is that the participant is
    * reassigning, which means that it hosts a stakeholder of the contract.
    *
    * Consider a reassignment R with that is incomplete unassigned with unassignment offset o_un and
    * suppose that the last locally hosted stakeholders of the reassignment are offboarded at offset
    * o_off. Then for an offset o, R is:
    *   - Incomplete if o_un <= o <= o_off
    *   - Not incomplete otherwise
    *
    * Algorithm for the update:
    *   - List all the incompletes at offset `offset` that have a stakeholder in `parties`
    *   - Filter out the incompletes that have at least another locally hosted stakeholder remaining
    *   - Mark the remaining incompletes as not incomplete anymore
    *
    * Note: the offline client needs to be able to get a topology snapshot for `ts`. This is fine as
    * long as this method is called after the topology client has processed the topology up to ts.
    */
  // TODO(#23636) Remove this method
  def handlePartiesOffboarding(
      offset: Offset,
      ts: CantonTimestamp,
      parties: NonEmpty[Set[LfPartyId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.data.Offset]] for the reassignment events to the
    * reassignment data in the store, provided that the reassignment data has previously been
    * stored.
    *
    * The same [[com.digitalasset.canton.data.Offset]] can be added any number of times.
    */
  def addReassignmentsOffsets(
      events: Seq[(ReassignmentId, ReassignmentGlobalOffset)]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
    for {
      preparedOffsets <- EitherT.fromEither[FutureUnlessShutdown](mergeReassignmentOffsets(events))
      _ <- addReassignmentsOffsets(preparedOffsets)
    } yield ()

  /** Marks the reassignment as completed, i.e., an assignment request was committed. If the
    * reassignment has already been completed then a
    * [[ReassignmentStore.ReassignmentAlreadyCompleted]] is reported, and the
    * [[com.digitalasset.canton.data.CantonTimestamp]] of the completion is not changed from the old
    * value.
    *
    * @param tsCompletion
    *   Provides the activeness timestamp of the committed assignment request.
    */
  def completeReassignment(reassignmentId: ReassignmentId, tsCompletion: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit]

  /** Removes all completions of reassignments that have been triggered by requests with at least
    * the given timestamp. This method must not be called concurrently with
    * [[completeReassignment]], but may be called concurrently with [[addUnassignmentData]].
    *
    * Therefore, this method need not be linearizable w.r.t. [[completeReassignment]]. For example,
    * if two requests at `ts1` and `ts2` complete two reassignments while [[deleteCompletionsSince]]
    * is running for some `ts <= ts1, ts2`, then there are no guarantees which of the completions at
    * `ts1` and `ts2` remain.
    */
  def deleteCompletionsSince(criterionInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** In certain special cases, where an assignment starts before the unassignment has had a chance
    * to write the reassignment data (either due to slow processing on the source synchronizer or
    * simply because the participant is disconnected from the source synchronizer), we want to
    * insert assignment data to allow the assignment to complete.
    */
  def addAssignmentDataIfAbsent(assignmentData: AssignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Removes the reassignment from the store, when the unassignment request is rejected or the
    * reassignment is pruned.
    */
  @VisibleForTesting
  def deleteReassignment(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object ReassignmentStore {
  def reassignmentStoreFor(
      syncPersistentStateLookup: SyncPersistentStateLookup
  ): Target[SynchronizerId] => Either[String, ReassignmentStore] =
    (synchronizerId: Target[SynchronizerId]) =>
      syncPersistentStateLookup
        .reassignmentStore(synchronizerId.unwrap)
        .toRight(s"Unknown synchronizer `${synchronizerId.unwrap}`")

  private final case class PartyOffboardingData(
      offset: Offset,
      effectiveTime: CantonTimestamp,
      lsid: SynchronizerId,
      party: Party,
  )

  /** Collect events that lead to an update of the store:
    *   - ReassignmentAccepted for which the participant is reassigning
    *   - Party offboarding from the local participant
    */
  private def collectUpdates(
      participantId: ParticipantId,
      updates: Seq[(Offset, Update)],
  ): (Seq[(Offset, Update.ReassignmentAccepted)], Seq[PartyOffboardingData]) =
    updates.foldLeft(
      (
        Seq.empty[(Offset, Update.ReassignmentAccepted)],
        Seq.empty[PartyOffboardingData],
      )
    ) { case (acc @ (accReassignments, accOffboardings), (offset, update)) =>
      update match {
        // ReassignmentAccepted and participant is reassigning
        case reassignmentAccepted: Update.ReassignmentAccepted
            if reassignmentAccepted.reassignmentInfo.isReassigningParticipant =>
          ((offset, reassignmentAccepted) +: accReassignments, accOffboardings)

        // Offboarding parties from the local participant
        case topologyTransaction: Update.TopologyTransactionEffective =>
          val newElements = topologyTransaction.events.collect {
            case TopologyEvent.PartyToParticipantAuthorization(
                  party,
                  participant,
                  AuthorizationEvent.Revoked,
                ) if participant == participantId.toLf =>
              PartyOffboardingData(
                offset,
                topologyTransaction.effectiveTime,
                topologyTransaction.synchronizerId,
                party,
              )
          }

          (accReassignments, newElements ++: accOffboardings)

        case _ => acc
      }
    }

  def reassignmentOffsetPersistenceFor(
      participantId: ParticipantId,
      syncPersistentStateLookup: SyncPersistentStateLookup,
      batchingConfig: BatchingConfig,
  )(implicit
      executionContext: ExecutionContext
  ): ReassignmentOffsetPersistence = new ReassignmentOffsetPersistence {
    override def persist(
        updates: Seq[(Offset, Update)],
        tracedLogger: TracedLogger,
    )(implicit traceContext: TraceContext): Future[Unit] = {
      val (reassignmentAccepted, offboardings) = collectUpdates(participantId, updates)

      for {
        _ <- handleReassignmentAccepted(syncPersistentStateLookup, batchingConfig, tracedLogger)(
          reassignmentAccepted
        )

        _ <- handlePartiesOffboarding(syncPersistentStateLookup, tracedLogger)(offboardings)
      } yield ()

    }
  }

  /** For all ReassignmentAccepted event, set the offset of the unassigned or assigned event in the
    * DB.
    *
    * Precondition:
    *   - Participant is reassigning for all the events.
    */
  private def handleReassignmentAccepted(
      syncPersistentStateLookup: SyncPersistentStateLookup,
      batchingConfig: BatchingConfig,
      tracedLogger: TracedLogger,
  )(
      reassignmentAccepted: Seq[(Offset, Update.ReassignmentAccepted)]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Unit] = {

    val updatesPerSynchronizer = reassignmentAccepted.groupBy { case (_, event) =>
      event.reassignmentInfo.targetSynchronizer
    }.toList

    MonadUtil.parTraverseWithLimit_(batchingConfig.parallelism)(updatesPerSynchronizer) {
      case (targetSynchronizer, eventsForSynchronizer) =>
        lazy val updates = eventsForSynchronizer
          .map { case (offset, event) =>
            s"${event.reassignmentInfo.sourceSynchronizer} ${event.reassignmentInfo.reassignmentId} (${event.reassignment}): $offset"
          }
          .mkString(", ")

        val res: EitherT[FutureUnlessShutdown, String, Unit] = for {
          reassignmentStore <- EitherT
            .fromEither[FutureUnlessShutdown](
              reassignmentStoreFor(syncPersistentStateLookup)(targetSynchronizer)
            )
          offsets = eventsForSynchronizer.flatMap { case (globalOffset, reassignmentEvent) =>
            reassignmentGlobalOffset(reassignmentEvent.reassignment, globalOffset).map {
              reassignmentEvent.reassignmentInfo.reassignmentId -> _
            }
          }
          _ = tracedLogger.debug(s"Updated global offsets for reassignments: $updates")
          _ <- reassignmentStore.addReassignmentsOffsets(offsets).leftMap(_.message)
        } yield ()

        EitherTUtil
          .toFutureUnlessShutdown(
            res.leftMap(err =>
              new RuntimeException(
                s"Unable to update global offsets for reassignments ($updates): $err"
              )
            )
          )
          .onShutdown(
            throw new RuntimeException(
              "Notification upon published reassignment aborted due to shutdown"
            )
          )
    }
  }

  private def handlePartiesOffboarding(
      syncPersistentStateLookup: SyncPersistentStateLookup,
      tracedLogger: TracedLogger,
  )(
      offboardings: Seq[PartyOffboardingData]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Unit] =
    /*
     We handle events in order because we want to mark the reassignment with the lowest offset
      that leaves not locally hosted stakeholder.
     */
    MonadUtil.sequentialTraverse_(
      offboardings
        .groupMap(offboarding => (offboarding.offset, offboarding.effectiveTime, offboarding.lsid))(
          _.party
        )
        .toSeq
        .sortBy { case ((offset, ts, _), _) => (offset, ts) }
    ) { case ((offset, effectiveTime, lsid), parties) =>
      val res: EitherT[FutureUnlessShutdown, String, Unit] = for {
        reassignmentStore <- EitherT
          .fromEither[FutureUnlessShutdown](
            reassignmentStoreFor(syncPersistentStateLookup)(Target(lsid))
          )

        partiesNE = NonEmpty.from(parties.toSet.map(LfPartyId.assertFromString))
        _ <- partiesNE.fold(EitherTUtil.unitUS[String]) { partiesNE =>
          tracedLogger.debug(
            s"Handling offboarding of parties at offset $offset and ts $effectiveTime on $lsid: $parties"
          )

          reassignmentStore
            .handlePartiesOffboarding(offset, effectiveTime, partiesNE)
            .leftMap(_.message)
        }
      } yield ()

      EitherTUtil
        .toFutureUnlessShutdown(
          res.leftMap(err =>
            new RuntimeException(s"Unable to handle parties offboarding ($parties): $err")
          )
        )
        .onShutdown(
          throw new RuntimeException(
            "Handling parties offboarding aborted due to shutdown"
          )
        )
    }

  private def reassignmentGlobalOffset(
      reassignment: Reassignment.Batch,
      offset: Offset,
  ): Option[ReassignmentGlobalOffset] =
    reassignment.headOption.map { // The batch will all have the same kind
      case _: Reassignment.Assign => AssignmentGlobalOffset(offset)
      case _: Reassignment.Unassign => UnassignmentGlobalOffset(offset)
    }

  /** Merge the offsets corresponding to the same reassignment id. Returns an error in case of
    * inconsistent offsets.
    */
  private def mergeReassignmentOffsets(
      events: Seq[(ReassignmentId, ReassignmentGlobalOffset)]
  ): Either[ConflictingGlobalOffsets, Map[ReassignmentId, ReassignmentGlobalOffset]] = {
    type Acc = Map[ReassignmentId, ReassignmentGlobalOffset]
    val zero: Acc = Map.empty[ReassignmentId, ReassignmentGlobalOffset]

    MonadUtil.foldLeftM[Either[
      ConflictingGlobalOffsets,
      *,
    ], Acc, (ReassignmentId, ReassignmentGlobalOffset)](
      zero,
      events,
    ) { case (acc, (reassignmentId, offset)) =>
      val newOffsetE = acc.get(reassignmentId) match {
        case Some(value) =>
          value.merge(offset).leftMap(ConflictingGlobalOffsets(reassignmentId, _))
        case None => Right(offset)
      }

      newOffsetE.map(newOffset => acc + (reassignmentId -> newOffset))
    }
  }

  sealed trait ReassignmentStoreError extends Product with Serializable {
    def message: String
  }
  sealed trait ReassignmentLookupError extends ReassignmentStoreError {
    def cause: String
    def reassignmentId: ReassignmentId
    def message: String = s"Cannot lookup for reassignment `$reassignmentId`: $cause"
  }

  final case class UnknownReassignmentId(reassignmentId: ReassignmentId)
      extends ReassignmentLookupError {
    override def cause: String = "unknown reassignment id"
  }

  final case class ReassignmentCompleted(
      reassignmentId: ReassignmentId,
      tsCompletion: CantonTimestamp,
  ) extends ReassignmentLookupError {
    override def cause: String = "reassignment already completed"
  }

  final case class AssignmentStartingBeforeUnassignment(
      reassignmentId: ReassignmentId
  ) extends ReassignmentLookupError {
    override def cause: String = "assignment already completed before unassignment"
  }

  final case class ReassignmentGlobalOffsetsMerge(
      reassignmentId: ReassignmentId,
      error: String,
  ) extends ReassignmentStoreError {
    override def message: String =
      s"Unable to merge global offsets for reassignment `$reassignmentId`: $error"
  }

  final case class ConflictingGlobalOffsets(reassignmentId: ReassignmentId, error: String)
      extends ReassignmentStoreError {
    override def message: String = s"Conflicting global offsets for $reassignmentId: $error"
  }

  final case class ReassignmentAlreadyCompleted(
      reassignmentId: ReassignmentId,
      newCompletion: CantonTimestamp,
  ) extends ReassignmentStoreError {
    override def message: String = s"Reassignment `$reassignmentId` is already completed"
  }

  final case class TopologyLookupError(lsid: SynchronizerId, ts: CantonTimestamp, error: String)
      extends ReassignmentStoreError {
    override def message: String =
      s"Unable to query topology snapshot at $ts for synchronizer $lsid: $error"
  }

  /** The data for a reassignment and possible when the reassignment was completed.
    *
    * @param reassignmentId
    *   Id of the reassignment
    * @param sourceSynchronizer
    *   Source synchronizer
    * @param stakeholders
    *   All stakeholders of the contracts
    * @param unassignmentData
    *   Data for the unassignment. Set in phase 7.
    * @param reassignmentGlobalOffset
    *   Offsets at which (un)assigned events are emitted.
    * @param incompleteLastStakeholderOffboardedTargetOffset
    *   If the last locally hosted stakeholder on the target synchronizer is offboarded from the
    *   participant while the reassignment is incomplete unassigned, the offset of the offboarding.
    * @param unassignmentTs
    *   Record time of the unassignment.
    * @param assignmentTs
    *   Record time of the assignment.
    */
  final case class ReassignmentEntry(
      reassignmentId: ReassignmentId,
      sourceSynchronizer: Source[SynchronizerId],
      stakeholders: NonEmpty[Set[LfPartyId]],
      unassignmentData: Option[UnassignmentData],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      incompleteLastStakeholderOffboardedTargetOffset: Option[Offset],
      unassignmentTs: CantonTimestamp,
      assignmentTs: Option[CantonTimestamp],
  ) {
    def unassignmentGlobalOffset: Option[Offset] = reassignmentGlobalOffset.flatMap(_.unassignment)
    def assignmentGlobalOffset: Option[Offset] = reassignmentGlobalOffset.flatMap(_.assignment)

    def clearCompletion: ReassignmentEntry = this.copy(assignmentTs = None)
  }

  object ReassignmentEntry {
    def apply(
        unassignmentData: UnassignmentData,
        reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
        incompleteLastStakeholderOffboardedTargetOffset: Option[Offset],
        tsCompletion: Option[CantonTimestamp],
    ): ReassignmentEntry =
      ReassignmentEntry(
        unassignmentData.reassignmentId,
        unassignmentData.sourcePsid.map(_.logical),
        stakeholdersOf(unassignmentData.reassignmentId, unassignmentData.contractsBatch),
        Some(unassignmentData),
        reassignmentGlobalOffset,
        incompleteLastStakeholderOffboardedTargetOffset,
        unassignmentData.unassignmentTs,
        tsCompletion,
      )

    def apply(
        assignmentData: AssignmentData,
        reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
        incompleteLastStakeholderOffboardedTargetOffset: Option[Offset],
        unassignmentTs: CantonTimestamp,
        tsCompletion: Option[CantonTimestamp],
    ): ReassignmentEntry =
      ReassignmentEntry(
        assignmentData.reassignmentId,
        assignmentData.sourceSynchronizer,
        stakeholdersOf(assignmentData.reassignmentId, assignmentData.contracts),
        None,
        reassignmentGlobalOffset,
        incompleteLastStakeholderOffboardedTargetOffset,
        unassignmentTs,
        tsCompletion,
      )

    private def stakeholdersOf(
        reassignmentId: ReassignmentId,
        contracts: ContractsReassignmentBatch,
    ): NonEmpty[Set[LfPartyId]] =
      NonEmpty
        .from(contracts.stakeholders.all)
        .getOrElse(
          throw new IllegalStateException(
            s"Reassignment $reassignmentId has a contract without stakeholders"
          )
        )
  }
}

trait ReassignmentLookup {
  import ReassignmentStore.*

  /** Looks up the given in-flight reassignment and returns the data associated with the
    * reassignment.
    *
    * @return
    *   [[scala.Left$]]([[ReassignmentStore.UnknownReassignmentId]]) if the reassignment is unknown;
    *   [[scala.Left$]]([[ReassignmentStore.ReassignmentCompleted]]) if the reassignment has already
    *   been completed.
    */
  def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, UnassignmentData]

  /** Find utility to look for in-flight reassignments. Reassignments are ordered by the tuple
    * (request timestamp, source synchronizer id), ie reassignments are ordered by request
    * timestamps and ties are broken with lexicographic ordering on synchronizer ids.
    *
    * The ordering here has been chosen to allow a participant to fetch all the pending
    * reassignments. The ordering has to be consistent accross calls and uniquely identify a pending
    * reassignment, but is otherwise arbitrary.
    *
    * @param requestAfter
    *   optionally, specify a strict lower bound for the reassignments returned, according to the
    *   (request timestamp, source synchronizer id) ordering
    * @param limit
    *   limit the number of results
    */
  def findAfter(requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])], limit: Int)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[UnassignmentData]]

  /** Find utility to look for incomplete reassignments. Reassignments are ordered by global offset.
    *
    * A reassignment `t` is considered as incomplete at offset `validAt` if only one of the two
    * reassignment events was emitted on the indexer at `validAt`. That is, one of the following
    * hold:
    *   1. Only unassignment was emitted
    *      - `t.unassignmentGlobalOffset` is smaller or equal to `validAt`
    *      - `t.assignmentGlobalOffset` is null or greater than `validAt` 2. Only assignment was
    *        emitted
    *      - `t.assignmentGlobalOffset` is smaller or equal to `validAt`
    *      - `t.unassignmentGlobalOffset` is null or greater than `validAt`
    *
    * In particular, for a reassignment to be considered incomplete at `validAt`, then exactly one
    * of the two offsets (unassignmentGlobalOffset, assignmentGlobalOffset) is not null and smaller
    * or equal to `validAt`.
    *
    * @param validAt
    *   select only reassignments that are successfully unassigned
    * @param stakeholders
    *   if non-empty, select only reassignments of contracts whose set of stakeholders intersects
    *   `stakeholders`.
    * @param limit
    *   limit the number of results
    */
  def findIncomplete(
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]]

  /** Find utility to look for the earliest incomplete reassignment w.r.t. the ledger end. If an
    * incomplete reassignment exists, the method returns the global offset of the incomplete
    * reassignment for either the unassignment or the assignment, whichever of these is not null,
    * the reassignment id and the target synchronizer id. It returns None if there is no incomplete
    * reassignment (either because all reassignments are complete or are in-flight, or because there
    * are no reassignments), or the reassignment table is empty.
    */
  def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]]

  /** Queries the reassignment ids for the given contract ids. Optional filtering by unassignment
    * and completion (assignment) timestamps, and by source synchronizer.
    */
  def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceSynchronizer: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]]

  @VisibleForTesting
  def findReassignmentEntry(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownReassignmentId, ReassignmentEntry]

  @VisibleForTesting
  def listInFlightReassignmentIds()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ReassignmentId]]
}
