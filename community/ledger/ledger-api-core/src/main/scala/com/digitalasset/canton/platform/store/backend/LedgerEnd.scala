// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import cats.{Semigroup, Show}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ShowUtil

final case class LedgerEnd(
    lastOffset: Offset,
    lastEventSeqId: Long,
    lastStringInterningId: Int,
    lastPublicationTime: CantonTimestamp,
    synchronizerIndices: Map[SynchronizerId, SynchronizerIndex],
) extends PrettyPrinting {
  override protected def pretty: Pretty[LedgerEnd] = LedgerEnd.prettyLedgerEndProperties
}

object LedgerEnd {
  private val synchronizerIndexMaxSemigroup = new Semigroup[SynchronizerIndex] {
    override def combine(x: SynchronizerIndex, y: SynchronizerIndex): SynchronizerIndex = x.max(y)
  }
  val synchronizerIndexStateSemigroup: Semigroup[Map[SynchronizerId, SynchronizerIndex]] =
    cats.instances.map.catsKernelStdMonoidForMap(synchronizerIndexMaxSemigroup)

  def update(
      ledgerEndWithSynchronizerIndex: Option[LedgerEnd],
      ledgerEndUpdate: LedgerEnd,
  ): LedgerEnd =
    LedgerEnd(
      ledgerEndUpdate.lastOffset,
      ledgerEndUpdate.lastEventSeqId,
      ledgerEndUpdate.lastStringInterningId,
      ledgerEndUpdate.lastPublicationTime,
      synchronizerIndices = synchronizerIndexStateSemigroup.combine(
        ledgerEndWithSynchronizerIndex.map(_.synchronizerIndices).getOrElse(Map()),
        ledgerEndUpdate.synchronizerIndices,
      ),
    )

  def combineUpdates(
      x: LedgerEnd,
      y: LedgerEnd,
  ): LedgerEnd = update(Some(x), y)

  val beforeBegin: Option[LedgerEnd] = None

  /** Pretty instance to print only ledger end properties without synchronizer idx */
  val prettyLedgerEndProperties: Pretty[LedgerEnd] = PrettyUtil.prettyOfClass(
    PrettyUtil.param("lastOffset", _.lastOffset),
    PrettyUtil.param("lastEventSeqId", _.lastEventSeqId),
    PrettyUtil.param("lastStringInterningId", _.lastStringInterningId),
    PrettyUtil.param("lastPublicationTime", _.lastPublicationTime),
  )

  val showLedgerEndProperties: Show[LedgerEnd] = ShowUtil.showPretty(prettyLedgerEndProperties)
}
