// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.offset
import com.daml.lf.data.Time

package object participant {

  // Sync event and offset used by participant state ReadService api
  type LedgerSyncOffset = offset.Offset
  val LedgerSyncOffset: offset.Offset.type = offset.Offset

  // A Long serves as the ledger offset in a single domain and for the multi domain event log
  type GlobalOffset = Long
  type LocalOffset = Long

  // Ledger record time is "single-dimensional"
  type LedgerSyncRecordTime = Time.Timestamp
  val LedgerSyncRecordTime: Time.Timestamp.type = Time.Timestamp
}
