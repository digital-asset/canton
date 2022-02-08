// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.offset
import com.daml.ledger.participant.state.v2
import com.daml.lf.data.Time

package object participant {

  // Sync event and offset used by participant state ReadService api
  type LedgerSyncEvent = v2.Update
  val LedgerSyncEvent: v2.Update.type = v2.Update
  type LedgerSyncOffset = offset.Offset
  val LedgerSyncOffset: offset.Offset.type = offset.Offset

  // Sync events passed around with sync offsets
  type LedgerSyncEventWithOffset = (LedgerSyncOffset, LedgerSyncEvent)

  // A Long serves as the ledger offset in a single domain and for the multi domain event log
  type GlobalOffset = Long
  type LocalOffset = Long

  // Ledger record time is "single-dimensional"
  type LedgerSyncRecordTime = Time.Timestamp
  val LedgerSyncRecordTime: Time.Timestamp.type = Time.Timestamp

  /** The counter assigned by the transaction processor to confirmation and transfer requests. */
  type RequestCounter = Long

  object RequestCounter {

    /** A strict lower bound on all request counters
      */
    def LowerBound: RequestCounter = -1

    /** The request counter assigned to the first request in the lifetime of a participant */
    val GenesisRequestCounter: RequestCounter = 0L

    def MaxValue: RequestCounter = Long.MaxValue
  }
}
