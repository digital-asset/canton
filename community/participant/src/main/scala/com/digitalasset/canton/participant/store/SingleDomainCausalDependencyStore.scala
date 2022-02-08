// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.DomainPerPartyCausalState
import com.digitalasset.canton.participant.store.SingleDomainCausalDependencyStore.{
  CausalityWrite,
  CausalityWriteFinished,
}
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DomainId, LfPartyId}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

trait SingleDomainCausalDependencyStore {

  protected def state: DomainPerPartyCausalState

  @VisibleForTesting
  def snapshotStateForTesting: Map[LfPartyId, Map[DomainId, CantonTimestamp]] = state.toMap

  /** Synchronization to ensure the `state` has been recovered before we use this class. */
  protected val initialized: Future[Unit]

  val domainId: DomainId

  protected def logger: TracedLogger

  implicit def ec: ExecutionContext

  /** Synchronously update the in-memory state with the new causal dependencies specified by `clocks`. Persist the
    * change to the causality state asynchronously.
    *
    * This should not be called concurrently; the caller must wait for each returned future to complete before making another call.
    *
    * @return A future that completes when the new causality state has been persisted.
    */
  def updateStateAndStore(
      rc: RequestCounter,
      ts: CantonTimestamp,
      clocks: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      transferOutId: Option[TransferId],
  )(implicit tc: TraceContext): Future[CausalityWrite] = {
    initialized.map { case () =>
      // Does not run concurrently
      val perPartyDeps = clocks
      val next = perPartyDeps.map { case (partyId, delta) =>
        val before: Map[DomainId, CantonTimestamp] = state.getOrElse(partyId, TrieMap.empty).toMap
        val updated = SingleDomainCausalTracker.bound(List(before, delta))
        state.put(partyId, updated)
        partyId -> updated
      }

      CausalityWrite(next, CausalityWriteFinished(persistentInsert(rc, ts, clocks, transferOutId)))
    }
  }

  protected def persistentInsert(
      rc: RequestCounter,
      ts: CantonTimestamp,
      deps: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      transferOutId: Option[TransferId],
  )(implicit tc: TraceContext): Future[Unit]

  def initialize(lastIncluded: Option[RequestCounter])(implicit tc: TraceContext): Future[Unit]
}

object SingleDomainCausalDependencyStore {

  case class CausalityWriteFinished(finished: Future[Unit])

  case class CausalityWrite(
      stateAtWrite: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      finished: CausalityWriteFinished,
  )

}
