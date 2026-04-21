// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.tls.TlsClientConfig

package object v2_dev {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    v2_3.default(timeoutScaleFactor) ++ Vector(
      new EventsDescendantsIT,
      new ExceptionRaceConditionIT,
      new ExceptionsIT,
    )

  def optional(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    v2_3.optional(tlsConfig)
}
