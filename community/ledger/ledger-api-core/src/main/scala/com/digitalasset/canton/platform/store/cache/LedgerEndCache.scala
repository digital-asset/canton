// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.LedgerEnd

import java.util.concurrent.atomic.AtomicReference

trait LedgerEndCache {
  def apply(): Option[LedgerEnd]
}

trait MutableLedgerEndCache extends LedgerEndCache {
  def set(ledgerEnd: Option[LedgerEnd]): Unit
  def update(
      ledgerEnd: LedgerEnd
  ): Unit
}

object MutableLedgerEndCache {
  def apply(): MutableLedgerEndCache =
    new MutableLedgerEndCache {
      private val ledgerEnd: AtomicReference[Option[LedgerEnd]] =
        new AtomicReference[Option[LedgerEnd]](None)

      override def set(ledgerEnd: Option[LedgerEnd]): Unit =
        this.ledgerEnd.set(ledgerEnd)

      override def apply(): Option[LedgerEnd] =
        ledgerEnd.get()

      override def update(
          ledgerEndUpdate: LedgerEnd
      ): Unit =
        this.ledgerEnd
          .updateAndGet(old => Some(LedgerEnd.update(old, ledgerEndUpdate)))
          .discard
    }
}
