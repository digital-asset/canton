// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  TransferredAway,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryActiveContractStore,
  InMemoryContractKeyJournal,
  InMemoryTransferStore,
  TransferCache,
}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractKeyJournal,
  TransferStore,
  TransferStoreTest,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  DomainId,
  HasExecutorService,
  LfPartyId,
  ScalaFuturesWithPatience,
}
import org.scalactic.source.Position
import org.scalatest.AsyncTestSuite
import org.scalatest.exceptions.TestFailedException

import scala.concurrent.{ExecutionContext, Future}

trait ConflictDetectionHelpers { this: AsyncTestSuite with BaseTest with HasExecutorService =>

  import ConflictDetectionHelpers._

  def parallelExecutionContext: ExecutionContext = executorService

  def mkEmptyAcs(): ActiveContractStore = new InMemoryActiveContractStore(loggerFactory)(
    parallelExecutionContext
  )

  def mkAcs(
      entries: (LfContractId, TimeOfChange, ActiveContractStore.Status)*
  )(implicit traceContext: TraceContext): Future[ActiveContractStore] = {
    val acs = mkEmptyAcs()
    insertEntriesAcs(acs, entries).map(_ => acs)
  }

  def mkEmptyCkj(): ContractKeyJournal = new InMemoryContractKeyJournal(loggerFactory)(
    parallelExecutionContext
  )

  def mkCkj(
      entries: (LfGlobalKey, TimeOfChange, ContractKeyJournal.Status)*
  )(implicit traceContext: TraceContext, position: Position): Future[ContractKeyJournal] = {
    val ckj = mkEmptyCkj()
    insertEntriesCkj(ckj, entries).map(_ => ckj)
  }

  def mkTransferCache(
      loggerFactory: NamedLoggerFactory,
      store: TransferStore =
        new InMemoryTransferStore(TransferStoreTest.targetDomain, loggerFactory),
  )(
      entries: (TransferId, MediatorId)*
  )(implicit traceContext: TraceContext): Future[TransferCache] = {
    Future
      .traverse(entries) { case (transferId, originMediator) =>
        for {
          transfer <- TransferStoreTest.mkTransferDataForDomain(
            transferId,
            originMediator,
            targetDomainId = TransferStoreTest.targetDomain,
          )
          result <- store
            .addTransfer(transfer)
            .value
        } yield result
      }
      .map(_ => new TransferCache(store, loggerFactory)(parallelExecutionContext))
  }
}

object ConflictDetectionHelpers extends ScalaFuturesWithPatience {

  def insertEntriesAcs(
      acs: ActiveContractStore,
      entries: Seq[(LfContractId, TimeOfChange, ActiveContractStore.Status)],
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    Future
      .traverse(entries) {
        case (coid, toc, Active) => acs.createContract(coid, toc).value
        case (coid, toc, Archived) => acs.archiveContract(coid, toc).value
        case (coid, toc, TransferredAway(targetDomain)) =>
          acs.transferOutContract(coid, toc, targetDomain).value
      }
      .void
  }

  def insertEntriesCkj(
      ckj: ContractKeyJournal,
      entries: Seq[(LfGlobalKey, TimeOfChange, ContractKeyJournal.Status)],
  )(implicit ec: ExecutionContext, traceContext: TraceContext, position: Position): Future[Unit] = {
    Future
      .traverse(entries) { case (key, toc, status) =>
        ckj
          .addKeyStateUpdates(Map(key -> status), toc)
          .valueOr(err => throw new TestFailedException(_ => Some(err.toString), None, position))
      }
      .void
  }

  def mkActivenessCheck[Key: Pretty](
      fresh: Set[Key] = Set.empty[Key],
      free: Set[Key] = Set.empty[Key],
      active: Set[Key] = Set.empty[Key],
      lock: Set[Key] = Set.empty[Key],
  ): ActivenessCheck[Key] =
    ActivenessCheck(checkFresh = fresh, checkFree = free, checkActive = active, lock = lock)

  def mkActivenessSet(
      deact: Set[LfContractId] = Set.empty,
      useOnly: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      txIn: Set[LfContractId] = Set.empty,
      transferIds: Set[TransferId] = Set.empty,
      freeKeys: Set[LfGlobalKey] = Set.empty,
      assignKeys: Set[LfGlobalKey] = Set.empty,
      unassignKeys: Set[LfGlobalKey] = Set.empty,
  ): ActivenessSet = {
    val contracts = ActivenessCheck(
      checkFresh = create,
      checkFree = txIn,
      checkActive = deact ++ useOnly,
      lock = create ++ txIn ++ deact,
    )
    val keys = ActivenessCheck(
      checkFresh = Set.empty,
      checkFree = freeKeys ++ assignKeys,
      checkActive =
        Set.empty, // We don't check that assigned contract keys are active during conflict detection
      lock = assignKeys ++ unassignKeys,
    )
    ActivenessSet(
      contracts = contracts,
      transferIds = transferIds,
      keys = keys,
    )
  }

  def mkActivenessCheckResult[Key: Pretty, Status <: PrettyPrinting](
      locked: Set[Key] = Set.empty[Key],
      notFresh: Set[Key] = Set.empty[Key],
      unknown: Set[Key] = Set.empty[Key],
      notFree: Map[Key, Status] = Map.empty[Key, Status],
      notActive: Map[Key, Status] = Map.empty[Key, Status],
  ): ActivenessCheckResult[Key, Status] =
    ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
    )

  def mkActivenessResult(
      locked: Set[LfContractId] = Set.empty,
      notFresh: Set[LfContractId] = Set.empty,
      unknown: Set[LfContractId] = Set.empty,
      notFree: Map[LfContractId, ActiveContractStore.Status] = Map.empty,
      notActive: Map[LfContractId, ActiveContractStore.Status] = Map.empty,
      inactiveTransfers: Set[TransferId] = Set.empty,
      lockedKeys: Set[LfGlobalKey] = Set.empty,
      unknownKeys: Set[LfGlobalKey] = Set.empty,
      notFreeKeys: Map[LfGlobalKey, ContractKeyJournal.Status] = Map.empty,
      notActiveKeys: Map[LfGlobalKey, ContractKeyJournal.Status] = Map.empty,
  ): ActivenessResult = {
    val contracts = ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
    )
    val keys = ActivenessCheckResult(
      alreadyLocked = lockedKeys,
      notFresh = Set.empty,
      unknown = unknownKeys,
      notFree = notFreeKeys,
      notActive = notActiveKeys,
    )
    ActivenessResult(
      contracts = contracts,
      inactiveTransfers = inactiveTransfers,
      keys = keys,
    )
  }

  def mkCommitSet(
      arch: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      txOut: Map[LfContractId, DomainId] = Map.empty,
      txIn: Map[LfContractId, TransferId] = Map.empty,
      keys: Map[LfGlobalKey, ContractKeyJournal.Status] = Map.empty,
  ): CommitSet = {
    val contractHash = ExampleTransactionFactory.lfHash(0)
    CommitSet(
      archivals = arch.map(_ -> WithContractHash(Set.empty[LfPartyId], contractHash)).toMap,
      creations = create.map(_ -> WithContractHash(ContractMetadata.empty, contractHash)).toMap,
      transferOuts = txOut.fmap(id => WithContractHash((id, Set.empty), contractHash)),
      transferIns = txIn.fmap(id =>
        WithContractHash(WithContractMetadata(id, ContractMetadata.empty), contractHash)
      ),
      keyUpdates = keys,
    )
  }
}
