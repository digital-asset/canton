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
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfGlobalKey
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class PreUpdateHookCkj(private val ckj: ContractKeyJournal)(
    override implicit val ec: ExecutionContext
) extends ContractKeyJournal {
  import PreUpdateHookCkj._

  private val nextAddKeyStateUpdatedHook =
    new AtomicReference[AddKeyStateUpdateHook](noKeyStateUpdateHook)

  def setUpdateHook(newHook: AddKeyStateUpdateHook): Unit = nextAddKeyStateUpdatedHook.set(newHook)

  override def fetchStates(keys: Iterable[LfGlobalKey])(implicit
      traceContext: TraceContext
  ): Future[Map[LfGlobalKey, ContractKeyState]] =
    ckj.fetchStates(keys)

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, Status], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] = {
    val preUpdate = nextAddKeyStateUpdatedHook.getAndSet(noKeyStateUpdateHook)
    preUpdate(updates, toc).flatMap(_ => ckj.addKeyStateUpdates(updates, toc))
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    ckj.doPrune(beforeAndIncluding)

  override def deleteSince(inclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    ckj.deleteSince(inclusive)

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    ckj.countUpdates(key)

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Option[PruningStatus]] =
    ckj.pruningStatus

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, ContractKeyJournalError, Unit] =
    ckj.advancePruningTimestamp(phase, timestamp)
}

object PreUpdateHookCkj {
  type AddKeyStateUpdateHook =
    (Map[LfGlobalKey, Status], TimeOfChange) => EitherT[Future, ContractKeyJournalError, Unit]

  val noKeyStateUpdateHook: AddKeyStateUpdateHook = (_, _) =>
    EitherT(Future.successful(Either.right(())))
}
