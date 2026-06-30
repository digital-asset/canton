// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.platform.store.backend.LedgerEnd

/** Serves as a backend to implement ledger end related API calls.
  */
trait LedgerEndService {
  def currentLedgerEnd(): Option[LedgerEnd]
}
