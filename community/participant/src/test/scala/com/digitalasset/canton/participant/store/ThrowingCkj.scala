// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.ContractKeyJournal.{
  ContractKeyJournalError,
  ContractKeyState,
  Status,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfGlobalKey
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._

import scala.concurrent.{ExecutionContext, Future}

class ThrowingCkj[T <: Throwable](mk: String => T)(override implicit val ec: ExecutionContext)
    extends ContractKeyJournal {
  private[this] type M = Either[ContractKeyJournalError, Unit]

  override def fetchStates(keys: Iterable[LfGlobalKey])(implicit
      traceContext: TraceContext
  ): Future[Map[LfGlobalKey, ContractKeyState]] =
    Future.failed(mk(show"fetchKeyStates(${keys.toSeq})"))

  /** Always returns [[scala.Map$.empty]] so that the failure does not happen while checking the invariant. */
  override def fetchStatesForInvariantChecking(ids: Iterable[LfGlobalKey])(implicit
      traceContext: TraceContext
  ): Future[Map[LfGlobalKey, StateChange[Status]]] = Future.successful(Map.empty)

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, Status], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    EitherT(Future.failed[M](mk(show"addKeyStateUpdates($updates, $toc)")))

  override def doPrune(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    EitherT(Future.failed[M](mk(show"doPrune($beforeAndIncluding)")))

  override def deleteSince(inclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    EitherT(Future.failed[M](mk(show"deleteSince($inclusive)")))

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    Future.failed(mk(show"countUpdates($key)"))

  /** Always returns [[scala.None$]] so that the failure does not happen while checking the invariant. */
  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Option[PruningStatus]] =
    EitherT(Future.successful(Either.right[ContractKeyJournalError, Option[PruningStatus]](None)))

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, ContractKeyJournalError, Unit] =
    EitherT(Future.failed[M](mk(show"advancePruningTimestamp($phase, $timestamp)")))
}
