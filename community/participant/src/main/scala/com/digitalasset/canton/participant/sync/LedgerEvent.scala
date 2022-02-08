// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.lf.crypto
import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.participant.LedgerSyncEvent

/** Ledger sync event related helper functions for upstream communication via the participant state read service.
  */
object LedgerEvent {

  /** Helper to modify ledger event timestamp.
    */
  def setTimestamp(event: LedgerSyncEvent, timestamp: LfTimestamp): LedgerSyncEvent =
    event match {
      case ta: LedgerSyncEvent.TransactionAccepted =>
        ta.copy(
          recordTime = timestamp,
          transactionMeta = ta.transactionMeta.copy(submissionTime = timestamp),
        )
      case ev: LedgerSyncEvent.PublicPackageUpload => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PublicPackageUploadRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.CommandRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PartyAddedToParticipant => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PartyAllocationRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ConfigurationChanged => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ConfigurationChangeRejected => ev.copy(recordTime = timestamp)
    }

  /** Produces a constant dummy transaction seed for transactions in which we cannot expose a seed. Essentially all of
    * them. TransactionMeta.submissionSeed can no longer be set to None starting with Daml 1.3
    * @return LF hash
    */
  def noOpSeed: crypto.Hash =
    crypto.Hash.assertFromString("00" * crypto.Hash.underlyingHashLength)

}
