// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.suites.{V2_2, V2_3}
import com.daml.ledger.api.testtool.{TestDar, TestDars}
import com.daml.tls.TlsClientConfig
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

trait AvailableTests {
  protected def testDars: TestDars

  def defaultTests(timeoutScaleFactor: Double): Vector[LedgerTestSuite]

  def optionalTests(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite]

  def darsToUpload: List[TestDar] = testDars.darsToUpload
  def lfVersion: LanguageVersion = testDars.lfVersion
}

object AvailableTests {
  val v2_2 = new V2_2(TestDars.v2_2)
  val v2_3 = new V2_3(TestDars.v2_3)

  val latestStableLf = v2_3

  def testsForProtocol(protocolVersion: ProtocolVersion): AvailableTests =
    if (protocolVersion <= ProtocolVersion.v34) v2_2
    else latestStableLf

  private def map = Map(
    LanguageVersion.v2_2 -> v2_2,
    LanguageVersion.v2_3 -> v2_3,
  )

  def apply(lfVersion: LanguageVersion): AvailableTests = map(lfVersion)
}
