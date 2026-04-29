// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity.api

import com.digitalasset.canton.integration.tests.continuity.{
  MultiVersionLedgerApiConformanceBase,
  ProtocolContinuityConformanceTest,
}
import com.digitalasset.canton.integration.{EnvironmentDefinition, IsolatedEnvironments}
import com.digitalasset.canton.util.ReleaseUtils

/** Tests various TestTools (as external processes) against the current canton. No canton is
  * downloaded from a zip; the current build is used directly.
  */
trait LedgerApiBackwardsCompatibilityConformanceTest
    extends MultiVersionLedgerApiConformanceBase
    with IsolatedEnvironments {
  override lazy val testedReleases: List[ReleaseUtils.TestedRelease] =
    ProtocolContinuityConformanceTest.previousSupportedReleases(logger).forgetNE

  override def connectedSynchronizersCount = 2

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
      }

  protected val numShards: Int = 8
  protected def shard: Int

  private val api: Seq[Boolean] = Seq(true, false)
  api.foreach { useJsonApi =>
    val allowedTestTools = testedReleases
      .map(_.releaseVersion)
      .filter(versionShouldBeChecked)
      .sorted
      .reverse
      .take(numberOfVersionsToCheck)

    allowedTestTools.foreach { clientRelease =>
      s"run conformance tests of shard $shard client-tools $clientRelease with current canton, json = $useJsonApi" in {
        implicit env =>
          runShardedTests(clientRelease, useJsonApi)(shard, numShards)(env)
      }
    }
  }

  override def excludedTests(): Seq[String] =
    super.excludedTests() ++ ledgerApiCompatiblityExcludedTests

  private val ledgerApiCompatiblityExcludedTests: Seq[String] = Seq(
    // TODO (i31463) Recheck excluded tests
    // Added in 3.4 line
    "TransactionServiceCorrectnessIT:TXExposesPaidTrafficCost",

    // emits error  PERMISSION_DENIED(7,84f62386) - probably should be filtered out ( enabled = _.userManagement.supported,)
    "UserManagementServiceIT:UserManagementUpdateUserIdpWithNonDefaultIdps",
    "UserManagementServiceIT:UserManagementUpdateUserIdpMismatchedSourceIdp",

    // Tools 3.4.12 to canton 3.5.0 over grpc
    "InteractiveSubmissionServiceIT:ISSExecuteAndWaitInvalidSynchronizerId", // wrong error message
    "ExplicitDisclosureIT:EDDuplicates", // unexpected failure
    "CommandServiceIT:CSSubmitAndWaitForTransactionFilterTemplate", // Expected a non empty traffic cost
    "InteractiveSubmissionServiceIT:ISSPrepareSubmissionRequestSynchronizerId", // unexpected synchronizer ID
    "ClosedWorldIT:ClosedWorldObserver", // Cannot execute a transaction that references unallocated observer parties
    "CommandSubmissionCompletionIT:CSCCompletions", // Empty traffic cost
    "InteractiveSubmissionServiceIT:ISSExecuteAndWaitForTransactionInvalidSynchronizerId", // wrong error message
    "CommandServiceIT:CSsubmitAndWaitBasic", // Expected a non empty traffic cost
    "CommandServiceIT:CSSubmitAndWaitForTransactionFilterAnyParty", // Expected a non empty traffic cost
    // Tools 3.4.12 to canton 3.5.0 over json
    "CommandServiceIT:CSsubmitAndWaitForTransactionLedgerEffectsBasic", //  Expected a non empty traffic cost

    // Fails on TT 3.4.12 and 3.5.0  FAILED_PRECONDITION: AUTOMATIC_REASSIGNMENT_FOR_TRANSACTION_FAILED(9,7f2edb88): Automatically reassigning contracts to a common synchronizer failed.[0m
    "ExplicitDisclosureIT:EDRouteByDisclosedContractSynchronizerId",
    // Unknown reason TT 3.4.12 never ends
    "DeduplicationMixedClients",
  )
}

class LedgerApiBackwardsCompatibilityShard0ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 0
}

class LedgerApiBackwardsCompatibilityShard1ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 1
}

class LedgerApiBackwardsCompatibilityShard2ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 2
}

class LedgerApiBackwardsCompatibilityShard3ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 3
}

class LedgerApiBackwardsCompatibilityShard4ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 4
}

class LedgerApiBackwardsCompatibilityShard5ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 5
}

class LedgerApiBackwardsCompatibilityShard6ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 6
}

class LedgerApiBackwardsCompatibilityShard7ConformanceTest
    extends LedgerApiBackwardsCompatibilityConformanceTest {
  override def shard: Int = 7
}
