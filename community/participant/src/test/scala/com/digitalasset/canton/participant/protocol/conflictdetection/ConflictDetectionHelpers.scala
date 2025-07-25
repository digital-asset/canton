// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  Purged,
  ReassignedAway,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryActiveContractStore,
  InMemoryReassignmentStore,
  ReassignmentCache,
}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ReassignmentStore,
  ReassignmentStoreTest,
}
import com.digitalasset.canton.participant.util.{TimeOfChange, TimeOfRequest}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LfPartyId,
  ReassignmentCounter,
  ScalaFuturesWithPatience,
}
import org.scalatest.AsyncTestSuite

import scala.concurrent.ExecutionContext

private[protocol] trait ConflictDetectionHelpers {
  this: AsyncTestSuite & BaseTest & HasExecutorService =>

  import ConflictDetectionHelpers.*

  def parallelExecutionContext: ExecutionContext = executorService

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 2)

  def mkEmptyAcs(): ActiveContractStore =
    new InMemoryActiveContractStore(indexedStringStore, loggerFactory)(
      parallelExecutionContext
    )

  protected def mkAcs(
      entries: (LfContractId, TimeOfRequest, ActiveContractStore.Status)*
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ActiveContractStore] = {
    val acs = mkEmptyAcs()
    insertEntriesAcs(
      acs,
      entries.map { case (cid, tor, status) => (cid, TimeOfChange(tor.timestamp), status) },
    ).map(_ => acs)
  }

  protected def mkReassignmentCache(
      loggerFactory: NamedLoggerFactory,
      store: ReassignmentStore = new InMemoryReassignmentStore(
        ReassignmentStoreTest.targetSynchronizerId,
        loggerFactory,
      ),
  )(
      entries: (Source[PhysicalSynchronizerId], MediatorGroupRecipient)*
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(ReassignmentCache, Seq[ReassignmentId])] =
    MonadUtil
      .sequentialTraverse(entries) { case (sourceSynchronizer, sourceMediator) =>
        val unassignmentData = ReassignmentStoreTest.mkUnassignmentDataForSynchronizer(
          sourceMediator,
          sourceSynchronizerId = sourceSynchronizer,
          targetSynchronizerId = ReassignmentStoreTest.targetSynchronizerId,
        )

        store.addUnassignmentData(unassignmentData).value.map(_ => unassignmentData.reassignmentId)
      }
      .map { reassignmentIds =>
        val cache = new ReassignmentCache(store, futureSupervisor, timeouts, loggerFactory)(
          parallelExecutionContext
        )

        (cache, reassignmentIds)
      }
}

private[protocol] object ConflictDetectionHelpers extends ScalaFuturesWithPatience {

  private val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  def insertEntriesAcs(
      acs: ActiveContractStore,
      entries: Seq[(LfContractId, TimeOfChange, ActiveContractStore.Status)],
  )(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    MonadUtil
      .sequentialTraverse(entries) {
        case (coid, tor, Active(_reassignmentCounter)) =>
          acs.markContractCreated(coid -> initialReassignmentCounter, tor)
        case (coid, tor, Archived) =>
          acs.archiveContract(coid, tor)
        case (coid, tor, Purged) =>
          acs.purgeContracts(Seq((coid, tor)))
        case (coid, tor, ReassignedAway(targetSynchronizer, reassignmentCounter)) =>
          acs.unassignContracts(coid, tor, targetSynchronizer, reassignmentCounter)
      }
      .value
      .map(_.void)

  def mkActivenessCheck[Key: Pretty](
      fresh: Set[Key] = Set.empty[Key],
      free: Set[Key] = Set.empty[Key],
      active: Set[Key] = Set.empty[Key],
      lock: Set[Key] = Set.empty[Key],
      lockMaybeUnknown: Set[Key] = Set.empty[Key],
      prior: Set[Key] = Set.empty[Key],
  ): ActivenessCheck[Key] =
    ActivenessCheck.tryCreate(
      checkFresh = fresh,
      checkFree = free,
      checkActive = active,
      lock = lock,
      lockMaybeUnknown = lockMaybeUnknown,
      needPriorState = prior,
    )

  def mkActivenessSet(
      deact: Set[LfContractId] = Set.empty,
      useOnly: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      assign: Set[LfContractId] = Set.empty,
      prior: Set[LfContractId] = Set.empty,
      reassignmentIds: Set[ReassignmentId] = Set.empty,
  ): ActivenessSet = {
    val contracts = ActivenessCheck.tryCreate(
      checkFresh = create,
      checkFree = assign,
      checkActive = deact ++ useOnly,
      lock = create ++ assign ++ deact,
      lockMaybeUnknown = Set.empty,
      needPriorState = prior,
    )
    ActivenessSet(
      contracts = contracts,
      reassignmentIds = reassignmentIds,
    )
  }

  def mkActivenessCheckResult[Key: Pretty, Status <: PrettyPrinting](
      locked: Set[Key] = Set.empty[Key],
      notFresh: Set[Key] = Set.empty[Key],
      unknown: Set[Key] = Set.empty[Key],
      notFree: Map[Key, Status] = Map.empty[Key, Status],
      notActive: Map[Key, Status] = Map.empty[Key, Status],
      prior: Map[Key, Option[Status]] = Map.empty[Key, Option[Status]],
  ): ActivenessCheckResult[Key, Status] =
    ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
      priorStates = prior,
    )

  def mkActivenessResult(
      locked: Set[LfContractId] = Set.empty,
      notFresh: Set[LfContractId] = Set.empty,
      unknown: Set[LfContractId] = Set.empty,
      notFree: Map[LfContractId, ActiveContractStore.Status] = Map.empty,
      notActive: Map[LfContractId, ActiveContractStore.Status] = Map.empty,
      prior: Map[LfContractId, Option[ActiveContractStore.Status]] = Map.empty,
      inactiveReassignments: Set[ReassignmentId] = Set.empty,
  ): ActivenessResult = {
    val contracts = ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
      priorStates = prior,
    )
    ActivenessResult(
      contracts = contracts,
      inactiveReassignments = inactiveReassignments,
    )
  }

  def mkCommitSet(
      arch: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      unassign: Map[LfContractId, (PhysicalSynchronizerId, ReassignmentCounter)] = Map.empty,
      assign: Map[LfContractId, (Source[PhysicalSynchronizerId], ReassignmentId)] = Map.empty,
  ): CommitSet =
    CommitSet(
      archivals = arch
        .map(
          _ -> CommitSet.ArchivalCommit(Set.empty[LfPartyId])
        )
        .toMap,
      creations = create
        .map(
          _ -> CommitSet.CreationCommit(
            ContractMetadata.empty,
            initialReassignmentCounter,
          )
        )
        .toMap,
      unassignments = unassign.fmap { case (id, reassignmentCounter) =>
        CommitSet.UnassignmentCommit(
          Target(id),
          Set.empty,
          reassignmentCounter,
        )
      },
      assignments = assign.fmap { case (sourcePSId, id) =>
        CommitSet.AssignmentCommit(
          sourcePSId.map(_.logical),
          id,
          ContractMetadata.empty,
          initialReassignmentCounter,
        )
      },
    )
}
