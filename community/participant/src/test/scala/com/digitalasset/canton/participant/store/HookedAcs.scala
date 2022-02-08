// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  AcsError,
  AcsWarning,
  ContractState,
}
import com.digitalasset.canton.participant.store.HookedAcs.noFetchAction
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.CheckedT

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

class HookedAcs(private val acs: ActiveContractStore)(implicit val ec: ExecutionContext)
    extends ActiveContractStore {
  import HookedAcs.{noAction, noTransferAction}

  private val nextCreateHook: AtomicReference[(Seq[LfContractId], TimeOfChange) => Future[Unit]] =
    new AtomicReference[(Seq[LfContractId], TimeOfChange) => Future[Unit]](noAction)
  private val nextArchiveHook: AtomicReference[(Seq[LfContractId], TimeOfChange) => Future[Unit]] =
    new AtomicReference[(Seq[LfContractId], TimeOfChange) => Future[Unit]](noAction)
  private val nextTransferHook
      : AtomicReference[(Seq[(LfContractId, DomainId)], TimeOfChange, Boolean) => Future[Unit]] =
    new AtomicReference[(Seq[(LfContractId, DomainId)], TimeOfChange, Boolean) => Future[Unit]](
      noTransferAction
    )
  private val nextFetchHook: AtomicReference[Iterable[LfContractId] => Future[Unit]] =
    new AtomicReference[Iterable[LfContractId] => Future[Unit]](noFetchAction)

  def setCreateHook(preCreate: (Seq[LfContractId], TimeOfChange) => Future[Unit]): Unit =
    nextCreateHook.set(preCreate)
  def setArchiveHook(preArchive: (Seq[LfContractId], TimeOfChange) => Future[Unit]): Unit =
    nextArchiveHook.set(preArchive)
  def setTransferHook(
      preTransfer: (Seq[(LfContractId, DomainId)], TimeOfChange, Boolean) => Future[Unit]
  ): Unit =
    nextTransferHook.set(preTransfer)
  def setFetchHook(preFetch: Iterable[LfContractId] => Future[Unit]) = nextFetchHook.set(preFetch)

  override def createContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preCreate = nextCreateHook.getAndSet(noAction)
    preCreate(contractIds, toc).flatMap { _ =>
      acs.createContracts(contractIds, toc).value
    }
  }

  override def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preArchive = nextArchiveHook.getAndSet(noAction)
    preArchive(contractIds, toc).flatMap { _ =>
      acs.archiveContracts(contractIds, toc).value
    }
  }

  override def transferInContracts(transferIns: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preTransfer = nextTransferHook.getAndSet(noTransferAction)
    preTransfer(transferIns, toc, false).flatMap { _ =>
      acs.transferInContracts(transferIns, toc).value
    }
  }

  override def transferOutContracts(transferOuts: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preTransfer = nextTransferHook.getAndSet(noTransferAction)
    preTransfer(transferOuts, toc, true).flatMap { _ =>
      acs.transferOutContracts(transferOuts, toc).value
    }
  }

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] = {
    val preFetch = nextFetchHook.getAndSet(noFetchAction)
    preFetch(contractIds).flatMap(_ => acs.fetchStates(contractIds))
  }

  override def fetchStatesForInvariantChecking(ids: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, StateChange[ActiveContractStore.Status]]] =
    acs.fetchStatesForInvariantChecking(ids)

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Either[AcsError, SortedMap[LfContractId, CantonTimestamp]]] =
    acs.snapshot(timestamp)

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, CantonTimestamp]] =
    acs.contractSnapshot(contractIds, timestamp)

  override def doPrune(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Unit] =
    acs.doPrune(beforeAndIncluding)

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, AcsError, Unit] = {
    acs.advancePruningTimestamp(phase, timestamp)
  }

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Option[PruningStatus]] =
    acs.pruningStatus

  override def deleteSince(criterion: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    acs.deleteSince(criterion)

  override def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    acs.contractCount(timestamp)

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    acs.changesBetween(fromExclusive, toInclusive)

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[(LfContractId)]] = acs.packageUsage(pkg, contractStore)
}

object HookedAcs {
  private val noAction: (Seq[LfContractId], TimeOfChange) => Future[Unit] = { (_, _) =>
    Future.unit
  }

  private val noTransferAction
      : (Seq[(LfContractId, DomainId)], TimeOfChange, Boolean) => Future[Unit] = { (_, _, _) =>
    Future.unit
  }

  private val noFetchAction: Iterable[LfContractId] => Future[Unit] = _ => Future.unit
}
