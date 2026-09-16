// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.TestDars
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.runner.AvailableTests
import com.daml.tls.TlsClientConfig

class V2_4(override val testDars: TestDars) extends AvailableTests {
  override def defaultTests(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    new V2_3(testDars).defaultTests(timeoutScaleFactor) ++ Vector(
    )

  override def optionalTests(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    new V2_3(testDars).optionalTests(tlsConfig)
}
