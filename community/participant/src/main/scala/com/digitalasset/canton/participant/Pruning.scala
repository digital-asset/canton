// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.participant.store.EventLogId
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ShowUtil.*

object Pruning {
  trait LedgerPruningError extends Product with Serializable { def message: String }

  case object LedgerPruningCancelledDueToShutdown extends LedgerPruningError {
    override def message: String = "Cancelled due to shutdown"
  }

  final case class LedgerPruningNothingPruned(message: String) extends LedgerPruningError

  final case class LedgerPruningInternalError(message: String) extends LedgerPruningError

  final case class LedgerPruningOnlySupportedInEnterpriseEdition(message: String)
      extends LedgerPruningError

  final case class LedgerPruningOffsetUnsafeDomain(domain: DomainId) extends LedgerPruningError {
    override def message =
      s"No safe-to-prune offset for domain $domain."
  }

  final case class LedgerPruningOffsetUnsafeToPrune(
      globalOffset: GlobalOffset,
      eventLog: EventLogId,
      localOffset: LocalOffset,
      cause: String,
      lastSafeOffset: Option[GlobalOffset],
  ) extends LedgerPruningError {
    override def message =
      show"Unsafe to prune offset ${UpstreamOffsetConvert.fromGlobalOffset(globalOffset)} due to the event in $eventLog with local offset $localOffset"
  }

  final case class LedgerPruningOffsetNonCantonFormat(message: String) extends LedgerPruningError
}
