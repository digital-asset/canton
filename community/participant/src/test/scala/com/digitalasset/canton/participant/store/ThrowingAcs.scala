// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either._
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  AcsError,
  AcsWarning,
  ContractState,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT}

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

class ThrowingAcs[T <: Throwable](mk: String => T)(override implicit val ec: ExecutionContext)
    extends ActiveContractStore {
  private[this] type M = Checked[AcsError, AcsWarning, Unit]

  override def createContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"createContracts for $contractIds at $toc")))

  override def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"archiveContracts for $contractIds at $toc")))

  override def transferInContracts(transferIns: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"transferInContracts for $transferIns at $toc")))

  override def transferOutContracts(transferOuts: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"transferOutContracts for $transferOuts at $toc")))

  override def fetchStates(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ContractState]] =
    Future.failed(mk(s"fetchContractStates for $contractIds"))

  /** Always returns [[scala.Map$.empty]] so that the failure does not happen while checking the invariant. */
  override def fetchStatesForInvariantChecking(ids: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, StateChange[ActiveContractStore.Status]]] =
    Future.successful(Map.empty)

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Either[AcsError, SortedMap[LfContractId, CantonTimestamp]]] =
    Future.failed(mk(s"snapshot at $timestamp"))

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, CantonTimestamp]] =
    EitherT(
      Future.failed[Either[AcsError, Map[LfContractId, CantonTimestamp]]](
        mk(s"contractSnapshot for $contractIds at $timestamp")
      )
    )

  override protected[canton] def doPrune(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Unit] =
    EitherT(Future.failed[Either[AcsError, Unit]](mk(s"doPrune at $beforeAndIncluding")))

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, AcsError, Unit] = {
    EitherT(Future.failed[Either[AcsError, Unit]](mk(s"advancePruningTimestamp")))
  }

  /** Always returns [[scala.None$]] so that the failure does not happen while checking the invariant. */
  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Option[PruningStatus]] =
    EitherT(Future.successful(Either.right[AcsError, Option[PruningStatus]](None)))

  override def deleteSince(criterion: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.failed[Unit](mk(s"deleteSince at $criterion"))

  override def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.failed(mk(s"contractCount at $timestamp"))

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    Future.failed(mk(s"changesBetween for $fromExclusive, $toInclusive"))

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[(LfContractId)]] =
    Future.failed(mk(s"packageUnused for $pkg"))
}
