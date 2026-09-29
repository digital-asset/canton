// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity

import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.LAPITTVersion
import com.digitalasset.canton.integration.tests.ledgerapi.{
  ExcludedTests,
  LedgerApiConformanceBase,
  ProtocolType,
}
import com.digitalasset.canton.util.ReleaseUtils.TestedRelease
import com.digitalasset.canton.version.ReleaseVersion

trait MultiVersionLedgerApiConformanceBase extends LedgerApiConformanceBase {

  protected def testedReleases: List[TestedRelease]

  protected val oldestVersionToCheck = ReleaseVersion(3, 4, 10, Some("snapshot"))

  protected val numberOfVersionsToCheck = 2

  protected def versionShouldBeChecked(v: ReleaseVersion): Boolean =
    v >= oldestVersionToCheck

  protected val ledgerApiTestToolPlugins: Map[ReleaseVersion, UseLedgerApiTestTool] =
    testedReleases
      .filter { tested =>
        // This is initial filtering of versions -> done to prevent downloading of too many historic versions
        versionShouldBeChecked(tested.releaseVersion)
      }
      .map { tested =>
        tested.releaseVersion -> new UseLedgerApiTestTool(
          loggerFactory = loggerFactory,
          connectedSynchronizersCount = connectedSynchronizersCount,
          version = LAPITTVersion.Explicit(tested.releaseVersion),
        )
      }
      .toMap

  ledgerApiTestToolPlugins.values.foreach(registerPlugin)

  // The tests that are limited to a single participant are not relevant for protocol continuity testing,
  // as they do not test any cross-participant interactions. The versions of the other participants are not relevant.
  // Likewise single-participant tests are not sensitive to the sequencer/mediator version.
  protected def onlyMultiParticipantTests: Boolean = true

  def runShardedTests(
      version: ReleaseVersion,
      useJsonApi: Boolean,
  )(shard: Int, numShards: Int)(
      env: TestConsoleEnvironment
  ): Unit = {
    val jsonExclusions = ExcludedTests.findExcludedTests(useJsonApi)
    ledgerApiTestToolPlugins(version)
      .runShardedSuites(
        shard,
        numShards,
        exclude = excludedTests(version, ProtocolType.fromUseJson(useJsonApi)) ++ jsonExclusions,
        useJson = useJsonApi,
        onlyMultiParticipantTests = onlyMultiParticipantTests,
      )(env)
  }
  def excludedTests(version: ReleaseVersion, protocolType: ProtocolType): Seq[String] = {
    val removedGetPreferredPackageVersionTests =
      Seq(
        "InteractiveSubmissionServiceIT:ISSPreferredPackageVersionKnown",
        "InteractiveSubmissionServiceIT:ISSPreferredPackageVersionUnknownParty",
        "InteractiveSubmissionServiceIT:ISSPreferredPackageVersionUnknownPackageName",
        "InteractiveSubmissionServiceIT:ISSPreferredPackageVersionUnknownSynchronizerId",
      )
    val perReleaseExclusions =
      if (version.majorMinor == (3, 4))
        Seq(
          // 3.5 changed the invalid synchronizer-id error message; the 3.4 test tool still expects
          // the old "Invalid unique identifier ... with missing namespace" wording.
          "InteractiveSubmissionServiceIT:ISSExecuteAndWaitForTransactionInvalidSynchronizerId",
          "InteractiveSubmissionServiceIT:ISSExecuteAndWaitInvalidSynchronizerId",
          // 3.5 accepts duplicate disclosed contracts with the same payload (idempotence);
          // the 3.4 test tool still expects them to be rejected.
          "ExplicitDisclosureIT:EDDuplicates",
        )
      else Seq.empty

    val jsonDeeplyNestedValueExclusions =
      if (protocolType == ProtocolType.Json && version.majorMinor == (3, 5))
        /** Tests that fail over the JSON API with test tools older than the relaxed assertion
          * introduced in #33832 (and backported to 3.5 by #35972)
          */
        Seq(
          "DeeplyNestedValueIT:RejectCreateCommand110",
          "DeeplyNestedValueIT:RejectCreateCommand200",
          "DeeplyNestedValueIT:RejectCreateArgumentInCreateAndExerciseCommand110",
          "DeeplyNestedValueIT:RejectCreateArgumentInCreateAndExerciseCommand200",
          "DeeplyNestedValueIT:RejectExerciseCommand110",
          "DeeplyNestedValueIT:RejectExerciseCommand200",
          "DeeplyNestedValueIT:RejectChoiceArgumentInCreateAndExerciseCommand110",
          "DeeplyNestedValueIT:RejectChoiceArgumentInCreateAndExerciseCommand200",
        )
      else Seq.empty

    removedGetPreferredPackageVersionTests ++ perReleaseExclusions ++
      jsonDeeplyNestedValueExclusions ++ LedgerApiConformanceBase
        .excludedTests(testedProtocolVersion, protocolType)
  }

}
