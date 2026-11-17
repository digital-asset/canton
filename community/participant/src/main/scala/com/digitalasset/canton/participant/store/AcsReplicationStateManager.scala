// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationParameters.AcsReplicationDestination
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationParameters.AcsReplicationDestination.Persistence
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicator.AcsReplicationRequestId
import com.digitalasset.canton.participant.store.AcsReplicationStateManager.{
  AcsReplicationOperationName,
  Modification,
  pendingDbOperationFromStatus,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, FutureUnlessShutdownUtil, SimpleExecutionQueue}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** ACS replication progress specific read and write methods.
  *
  * Note: Non-sealed for testing.
  */
private[canton] trait AcsReplicationProgress {
  def getAcsReplicationProgress(requestId: AcsReplicationRequestId)(implicit
      traceContext: TraceContext
  ): Option[AcsReplicationStatus.AcsReplicationProgress]

  def updateAcsReplicationProgress(
      requestId: AcsReplicationRequestId,
      progress: AcsReplicationStatus.AcsReplicationProgress,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit]
}

/** The [[AcsReplicationStateManager]] manages sequencer channel based ACS replication state in
  * memory and conditionally on disk (on behalf of ACS replications handled by this participant in
  * the target participant TP role). This lets TPs re-initiate interrupted ACS replications after
  * restarts or crashes.
  *
  * Note:
  *   - On TPs state is kept both in memory and on disk, as the in-memory state includes the
  *     non-serializable and hence non-persistable party replication processors. The in-memory state
  *     is eagerly loaded from the store at startup and kept in sync with the db.
  *   - The [[com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicator]] owns
  *     adding and removing
  *     [[com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus]]
  *     entries as well as updates that add (set to Some(_)) or remove (set to None) optional status
  *     subcomponents such as `replicationO` of
  *     [[com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus]] and
  *     does so from a simple execution queue.
  *   - Other components such as the
  *     [[com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationTargetParticipantProcessor]]
  *     update individual status subcomponents set to Some(_) such as
  *     [[com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationProgress]]
  *     from a simple execution queue.
  *   - Given that multiple components perform updates (but not necessarily in a racy fashion),
  *     error on the side of safety and have only the update methods use the simple execution queue
  *     to avoid lost updates in the absence of atomic update support in the underlying db store.
  */
final class AcsReplicationStateManager(
    participantId: ParticipantId,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
)(implicit executionContext: ExecutionContext)
    extends AcsReplicationProgress
    with NamedLogging
    with FlagCloseable {

  private val acsReplications = new TrieMap[AcsReplicationRequestId, AcsReplicationStatus]()

  private val targetParticipantStore =
    PendingOperationStore.apply[AcsReplicationStatus, SynchronizerId](
      storage,
      timeouts,
      loggerFactory,
      AcsReplicationStatus,
      SynchronizerId.fromString,
    )

  // Execution queue serializes db and map access and modifications.
  private val executionQueue = new SimpleExecutionQueue(
    "acs-replication-state",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  ).tap { eq =>
    implicit val tc: TraceContext = TraceContext.empty
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      eq.executeUS(
        targetParticipantStore
          .getAll(AcsReplicationOperationName)
          .map(
            _.foreach(op =>
              acsReplications
                .putIfAbsent(op.operation.params.requestId, op.operation)
                .foreach(duplicateStatus =>
                  throw new IllegalStateException(
                    s"Duplicate ACS replication ${op.operation.params.requestId} upon load should be impossible: existing ${duplicateStatus.params} new ${op.operation}"
                  )
                )
            )
          ),
        "load ACS replication map from store",
      ),
      "load ACS replication map from store failed",
    )
  }

  /** Add a previously unknown ACS replication returning an error if a status with the same
    * requestId is already known.
    *
    * @param status
    *   the status of a new ACS replication, i.e. previously unknown
    * @return
    *   an "already exists" error if a status with the same request id already exists
    */
  // TODO(#35267) Check if calling this method could collide with initialization before it's finished
  def add(status: AcsReplicationStatus)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        acsReplications
          .get(status.params.requestId)
          .toLeft(())
          .leftMap(alreadyExists =>
            s"ACS replication ${status.params.requestId} already exists: $alreadyExists"
          )
      )
      _ <- EitherTUtil.ifThenET(shouldStatusBePersisted(status))(
        targetParticipantStore
          .insert(pendingDbOperationFromStatus(status))
          .leftMap(alreadyExists =>
            s"ACS replication ${status.params.requestId} already exists in the store: $alreadyExists"
          )
      )
    } yield {
      // Reflect new status in memory only once store has been (conditionally) updated.
      acsReplications.putIfAbsent(status.params.requestId, status).discard
    }

  /** Update an already known ACS replication status returning an error if a status with the
    * requestId is unknown.
    *
    * @param requestId
    *   the request id of the status to update
    * @param modify
    *   a status to status modifier to alter the existing status within the simple execution queue.
    *   The update expressed by modify needs to be an idempotent update, i.e. setting a new counter
    *   value rather than incrementing the counter
    * @return
    *   the updated status
    */
  def update(requestId: AcsReplicationRequestId, modify: Modification)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AcsReplicationStatus] =
    updateIf(requestId, modify, _ => Either.unit)

  /** Like [[update]], but only if the existing status satisfies the precondition checked within the
    * simple execution queue.
    */
  private def updateIf(
      requestId: AcsReplicationRequestId,
      modify: Modification,
      precondition: AcsReplicationStatus => Either[String, Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AcsReplicationStatus] = executionQueue.executeEUS(
    for {
      previousStatus <- EitherT.fromEither[FutureUnlessShutdown](
        acsReplications
          .get(requestId)
          .toRight(s"ACS replication $requestId does not exist")
          .flatMap(status => precondition(status).map(_ => status))
      )
      updatedStatus = modify(previousStatus)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        previousStatus.params == updatedStatus.params,
        s"ACS replication $requestId parameters ${previousStatus.params} not allowed to change ${updatedStatus.params}",
      )
      _ <- EitherTUtil.ifThenET(shouldStatusBePersisted(previousStatus))(
        EitherT.right[String](
          targetParticipantStore.updateOperation(
            updatedStatus,
            previousStatus.params.synchronizerId,
            AcsReplicationOperationName,
            previousStatus.params.requestId.toHexString,
          )
        )
      )
    } yield {
      // Reflect updated status in memory only once store has been updated (conditionally on the TP only).
      // Before the in-memory state has been updated, readers (see under "get" below) might obtain potentially
      // stale values. See the notes in the scaladoc above and under "get" below with respect to idempotent updates.
      updatedStatus.tap(acsReplications.put(requestId, _).discard)
    },
    s"update $requestId",
  )

  def update_(requestId: AcsReplicationRequestId, modify: Modification)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = update(requestId, modify).map(_ => ())

  /** Resets an existing ACS replication progress to zero and sets a new output stream destination.
    *
    * Unlike [[update]], this is allowed to change the replication destination parameter. As
    * replications to output streams are never persisted, only the in-memory state is updated.
    *
    * @return
    *   the reset status or an error if the ACS replication does not exist or cannot be restarted
    */
  def resetProgress(
      requestId: AcsReplicationRequestId,
      destination: AcsReplicationDestination.OutputStream,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AcsReplicationStatus] = executionQueue.executeEUS(
    EitherT.fromEither[FutureUnlessShutdown](
      for {
        previousStatus <- acsReplications
          .get(requestId)
          .toRight(s"ACS replication $requestId does not exist")
        _ <- Either.cond(
          previousStatus.restartRequiresReplicationFromBeginning,
          (),
          s"ACS replication $requestId exists but cannot be restarted: $previousStatus",
        )
        restartedStatus = previousStatus.resetProgress(destination)
        _ <- Either.cond(
          !shouldStatusBePersisted(previousStatus) && !shouldStatusBePersisted(restartedStatus),
          (),
          s"ACS replication $requestId with persisted status cannot be restarted",
        )
      } yield restartedStatus.tap(acsReplications.put(requestId, _).discard)
    ),
    s"restart $requestId",
  )

  /** Fetches a status if the request id exists. When the output is used to describe an update, it
    * is important that the update only modifies the portion of the status that is changing and does
    * so in an idempotent way, i.e. setting new values rather than incrementing a counter.
    */
  def get(requestId: AcsReplicationRequestId): Option[AcsReplicationStatus] =
    acsReplications.get(requestId)

  /** Finds replication status by Daml sequencer channel agreement contract id.
    *
    * @param agreementContractId
    *   Daml replication agreement contract id
    * @return
    *   party replication status if found, None otherwise
    */
  def findByAgreementContractId(agreementContractId: LfContractId): Option[AcsReplicationStatus] =
    acsReplications.values.find(_.agreementStatus match {
      case AcsReplicationStatus.AgreementStatus.Exists(contractId, _, _) =>
        contractId.coid == agreementContractId.coid
      case _ => false
    })

  /** Finds replication status by Daml sequencer channel proposal contract id.
    *
    * @param proposalContractId
    *   Daml replication proposal contract id
    * @return
    *   party replication status if found, None otherwise
    */
  def findByProposalContractId(proposalContractId: LfContractId): Option[AcsReplicationStatus] =
    acsReplications.values.find(_.agreementStatus match {
      case AcsReplicationStatus.AgreementStatus.Proposed(contractId) =>
        contractId.coid == proposalContractId.coid
      case _ => false
    })

  def collectFirst[T](
      f: PartialFunction[(AcsReplicationRequestId, AcsReplicationStatus), T]
  ): Option[T] = acsReplications.iterator.collectFirst(f)

  def collect[T](f: PartialFunction[(AcsReplicationRequestId, AcsReplicationStatus), T]): Seq[T] =
    acsReplications.iterator.collect(f).toSeq

  override def getAcsReplicationProgress(requestId: AcsReplicationRequestId)(implicit
      traceContext: TraceContext
  ): Option[AcsReplicationStatus.AcsReplicationProgress] =
    get(requestId).flatMap(_.replicationO)

  /** Updates the progress only if it's been reported by the current ACS Replication Processor (or
    * no processor at all).
    *
    * If the update comes from a superseded ACS replication processor (in case of ACS replication
    * restart), it is rejected.
    */
  override def updateAcsReplicationProgress(
      requestId: AcsReplicationRequestId,
      progress: AcsReplicationStatus.AcsReplicationProgress,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    updateIf(
      requestId,
      _.modifyReplication(_ => progress),
      status =>
        Either.cond(
          progress.processorO.forall(processor =>
            status.replicationO.flatMap(_.processorO).exists(_ eq processor)
          ),
          (),
          s"Rejecting progress update of superseded ACS replication processor $requestId",
        ),
    ).map(_ => ())

  /** Decides whether a status should be persisted to pending operations store.
    *
    * If the status is persisted, it survives the participant restart and can be used to resume the
    * ACS replication.
    * @param status
    *   status to check
    * @return
    *   whether the status is persisted or not
    */
  private def shouldStatusBePersisted(
      status: AcsReplicationStatus
  ): Boolean =
    participantId == status.params.targetParticipantId && status.params.acsReplicationDestination == Persistence

  override protected def onClosed(): Unit =
    LifeCycle.close(executionQueue)(logger)
}

object AcsReplicationStateManager {
  type Modification = AcsReplicationStatus => AcsReplicationStatus

  private[AcsReplicationStateManager] lazy val AcsReplicationOperationName =
    NonEmptyString.tryCreate("acs_replication")

  private[AcsReplicationStateManager] def pendingDbOperationFromStatus(
      status: AcsReplicationStatus
  ): PendingOperation[AcsReplicationStatus, SynchronizerId] =
    PendingOperation[AcsReplicationStatus, SynchronizerId](
      AcsReplicationOperationName,
      status.params.requestId.toHexString,
      status,
      status.params.synchronizerId,
    )
}
