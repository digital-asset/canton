// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.ledger.api.testtool.infrastructure.TestConstraints
import com.daml.ledger.api.testtool.runner.AvailableTests

/** Discovers test case names that have to be excluded for a given tool x canton run of the ledger
  * API test tool suites.
  */
object ExcludedTests {

  /** Suites excluded when running via JSON API (service-level unsupported operations) */
  val jsonApiExcludedSuites: Seq[String] = Seq(
    // health service not available in JSON API
    "HealthServiceIT",
    // updatePartyIdentityProviderId not available; getParties fails with empty party list
    "PartyManagementServiceIT",
    // PERMISSION_DENIED due to user-management auth mismatch in JSON API
    "UserManagementServiceIT",
  )

  lazy val grpcOnlyTestNames: Seq[String] = AvailableTests.v2_2
    .defaultTests(timeoutScaleFactor = 1.0)
    .flatMap(_.tests)
    .collect {
      case testCase if testCase.limitation.isInstanceOf[TestConstraints.GrpcOnly] =>
        testCase.name
    }

  // TODO (i31440) GrpcOnlyTests.grpcOnlyTestNames is derived from local Test classes - not from actually downloaded tests tools jar
  // This is intentional - as we might want to introduce limitation such as VersionBelow(3.5.2) to mark tests that SHOULD not work on a newer version
  // For that it might make a sense to calculate exclusions using latest main code, not stored artifacts
  // On the other hand we might occasionally have tests only in previous release lines, that current main does not know about
  // So ideally we need to have exclude using current code plus test tools
  def findExcludedTests(useJson: Boolean): Seq[String] =
    if (useJson) grpcOnlyTestNames ++ jsonApiExcludedSuites else Seq.empty
}
