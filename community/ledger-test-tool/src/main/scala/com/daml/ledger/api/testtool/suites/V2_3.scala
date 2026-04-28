// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.TestDars
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.runner.AvailableTests
import com.daml.ledger.api.testtool.suites.v2_3.*
import com.daml.tls.TlsClientConfig

class V2_3(override val testDars: TestDars) extends AvailableTests {
  override def defaultTests(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    new V2_2(testDars).defaultTests(timeoutScaleFactor) ++ Vector(
      new ContractKeysCommandDeduplicationIT,
      new ContractKeysContractIdIT,
      new ContractKeysDeeplyNestedValueIT,
      new ContractKeysDivulgenceIT,
      new ContractKeysExplicitDisclosureIT,
      new ContractKeysIT,
      new ContractKeysMultiPartySubmissionIT,
      new ContractKeysWronglyTypedContractIdIT,
      new PrefetchContractKeysIT,
      new RaceConditionIT,
    )

  override def optionalTests(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    new V2_2(testDars).optionalTests(tlsConfig)
}
