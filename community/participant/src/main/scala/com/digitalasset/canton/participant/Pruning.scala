// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.participant.store.AcsCommitmentStore.AcsCommitmentStoreError
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.ContractKeyJournal.ContractKeyJournalError
import com.digitalasset.canton.participant.store.EventLogId
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ShowUtil._

object Pruning {
  trait LedgerPruningError extends Product with Serializable { def message: String }

  case object LedgerPruningCancelledDueToShutdown extends LedgerPruningError {
    override def message: String = "Cancelled due to shutdown"
  }

  case class LedgerPruningNothingPruned(message: String) extends LedgerPruningError

  case class LedgerPruningInternalError(message: String) extends LedgerPruningError

  case class LedgerPruningOnlySupportedInEnterpriseEdition(message: String)
      extends LedgerPruningError

  case class LedgerPruningOffsetUnsafeDomain(domain: DomainId) extends LedgerPruningError {
    override def message =
      s"No safe-to-prune offset for domain $domain."
  }

  case class LedgerPruningOffsetUnsafeToPrune(
      globalOffset: GlobalOffset,
      eventLog: EventLogId,
      localOffset: LocalOffset,
      cause: String,
      lastSafeOffset: Option[GlobalOffset],
  ) extends LedgerPruningError {
    override def message =
      show"Unsafe to prune offset ${UpstreamOffsetConvert.fromGlobalOffset(globalOffset)} due to the event in $eventLog with local offset $localOffset"
  }

  case class LedgerPruningOffsetNonCantonFormat(message: String) extends LedgerPruningError

  case class LedgerPruningAcsError(err: AcsError) extends LedgerPruningError {
    override def message = err.toString
  }
  case class LedgerPruningContractKeyJournalError(err: ContractKeyJournalError)
      extends LedgerPruningError {
    override def message = err.toString
  }

  case class LedgerPruningAcsCommitmentStoreError[M](err: AcsCommitmentStoreError)
      extends LedgerPruningError {
    override def message = err.toString
  }

  case class LedgerPruningUnknownMaxDeduplicationDuration(override val message: String)
      extends LedgerPruningError
}
