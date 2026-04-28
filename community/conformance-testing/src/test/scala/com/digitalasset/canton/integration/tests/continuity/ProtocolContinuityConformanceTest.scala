// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.plugins.UseExternalProcess.RunVersion
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.LAPITTVersion
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiUserManagementServiceSuppressionRule,
  DbActiveContractStoreConsistencyCheckSuppressionRule,
}
import com.digitalasset.canton.integration.tests.ledgerapi.{ExcludedTests, LedgerApiConformanceBase}
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.util.ReleaseUtils.TestedRelease
import com.digitalasset.canton.version.ReleaseVersionToProtocolVersions.majorMinorToStableProtocolVersions
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import org.scalatest.concurrent.PatienceConfiguration

import scala.concurrent.duration.DurationInt

trait MultiVersionLedgerApiConformanceBase extends LedgerApiConformanceBase {

  protected def testedReleases: List[TestedRelease]

  protected val oldestVersionToCheck = ReleaseVersion(3, 4, 10, Some("snapshot"))

  protected val numberOfVersionsToCheck = 2

  protected def versionShouldBeChecked(v: ReleaseVersion): Boolean =
    v >= oldestVersionToCheck

  protected val ledgerApiTestToolPlugins: Map[ReleaseVersion, UseLedgerApiTestTool] =
    testedReleases
      .filter { release =>
        // This is initial filtering of versions -> done to prevent downloading of too many historic versions
        versionShouldBeChecked(release.releaseVersion)
      }
      .map { release =>
        release.releaseVersion -> new UseLedgerApiTestTool(
          loggerFactory = loggerFactory,
          connectedSynchronizersCount = connectedSynchronizersCount,
          version = LAPITTVersion.Explicit(release.ledgerApiTestTool),
        )
      }
      .toMap

  ledgerApiTestToolPlugins.values.foreach(registerPlugin)

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
        exclude = excludedTests() ++ jsonExclusions,
        useJson = useJsonApi,
      )(env)
  }
  def excludedTests(): Seq[String] = LedgerApiConformanceBase.excludedTests

}

/** The Protocol continuity tests test that we don't accidentally break protocol compatibility with
  * respect to the Ledger API.
  *
  * To run them, see :
  *
  *   - AllProtocolContinuityConformanceTest Tests against all previously published releases.
  *
  *   - LatestProtocolContinuityConformanceTest Tests against the latest published release.
  */
trait ProtocolContinuityConformanceTest
    extends MultiVersionLedgerApiConformanceBase
    with IsolatedEnvironments
    with HasExecutionContext {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Manual
      .addConfigTransforms(ConfigTransforms.clearMinimumProtocolVersion*)
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)

  protected def numShards: Int
  protected def shard: Int
}

/** For a given release R, the Ledger API conformance test suites at release R are run against:
  *   - 1x synchronizer of release R with the latest protocol version of release R
  *   - 2x participants based on current main
  */
trait ProtocolContinuityConformanceTestSynchronizer extends ProtocolContinuityConformanceTest {
  private lazy val externalSynchronizer =
    new UseExternalProcess(
      loggerFactory,
      externalSequencers = Set("sequencer1"),
      externalMediators = Set("mediator1"),
      fileNameHint = this.getClass.getSimpleName,
      removeConfigPaths = ProtocolContinuityConformanceTest.removeConfigPaths,
    )

  registerPlugin(externalSynchronizer)

  testedReleases.foreach { case TestedRelease(_, release, protocolVersions) =>
    lazy val binDir = ReleaseUtils
      .retrieve(release)
      .futureValue(timeout = PatienceConfiguration.Timeout(2.minutes))
    lazy val pv = protocolVersions.max1

    s"run conformance tests of shard $shard with release $release and protocol $pv" in {
      implicit env =>
        import env.*

        val cantonReleaseVersion = RunVersion.Release(binDir)

        externalSynchronizer.start(remoteMediator1.name, cantonReleaseVersion)
        externalSynchronizer.start(remoteSequencer1.name, cantonReleaseVersion)
        participants.local.start()

        remoteSequencer1.health.wait_for_ready_for_initialization()
        remoteMediator1.health.wait_for_ready_for_initialization()

        val staticParams = StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = pv)
        NetworkBootstrapper(
          Seq(
            NetworkTopologyDescription.createWithStaticSynchronizerParameters(
              daName,
              synchronizerOwners = Seq(remoteSequencer1),
              synchronizerThreshold = PositiveInt.one,
              sequencers = Seq(remoteSequencer1),
              mediators = Seq(remoteMediator1),
              staticSynchronizerParameters = staticParams,
            )
          )
        )(env).bootstrap()

        remoteSequencer1.health.wait_for_initialized()
        remoteMediator1.health.wait_for_initialized()
        participants.local.foreach(_.health.wait_for_initialized())

        setupLedgerApiConformanceEnvironment(env)

        TrafficControlUtils.applyTrafficControl(
          TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
          TrafficControlUtils.predictableTraffic,
          topUpAllMembers = true,
          disableCommitments = true,
        )

        loggerFactory.suppress(
          ApiUserManagementServiceSuppressionRule ||
            DbActiveContractStoreConsistencyCheckSuppressionRule
        ) {
          runShardedTests(release, useJsonApi = false)(shard, numShards)(env)
        }

        // Shutdown
        shutdownLedgerApiConformanceEnvironment(env)

        externalSynchronizer.kill(remoteMediator1.name)
        externalSynchronizer.kill(remoteSequencer1.name)
    }
  }
}

/** For a given release R, these tests run the Ledger API compatibility tests against
  *   - 1x synchronizer based on current main with the latest protocol version of release R
  *   - 3x participants of release R
  */
trait ProtocolContinuityConformanceTestParticipant extends ProtocolContinuityConformanceTest {
  val external = new UseExternalProcess(
    loggerFactory,
    externalParticipants = Set("participant1", "participant2", "participant3"),
    fileNameHint = this.getClass.getSimpleName,
    removeConfigPaths = ProtocolContinuityConformanceTest.removeConfigPaths,
  )

  registerPlugin(external)

  testedReleases.foreach { case TestedRelease(_, release, protocolVersions) =>
    lazy val binDir = ReleaseUtils
      .retrieve(release)
      .futureValue(timeout = PatienceConfiguration.Timeout(2.minutes))
    lazy val pv = protocolVersions.max1

    s"run conformance tests of shard $shard with release $release and protocol $pv" in {
      implicit env =>
        import env.*

        val cantonReleaseVersion = RunVersion.Release(binDir)

        sequencer1.start()
        mediator1.start()
        mediator1.health.wait_for_ready_for_initialization()
        sequencer1.health.wait_for_ready_for_initialization()

        val staticParams = StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = pv)
        NetworkBootstrapper(
          Seq(
            EnvironmentDefinition.S1M1.copy(staticSynchronizerParameters = staticParams)
          )
        ).bootstrap()

        // Run the participants from the release binary
        external.start(remoteParticipant1.name, cantonReleaseVersion)
        external.start(remoteParticipant2.name, cantonReleaseVersion)
        external.start(remoteParticipant3.name, cantonReleaseVersion)
        remoteParticipant1.health.wait_for_initialized()
        remoteParticipant2.health.wait_for_initialized()
        remoteParticipant3.health.wait_for_initialized()

        setupLedgerApiConformanceEnvironment(env)

        TrafficControlUtils.applyTrafficControl(
          TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
          TrafficControlUtils.predictableTraffic,
          topUpAllMembers = true,
          disableCommitments = true,
        )
        runShardedTests(release, useJsonApi = false)(shard, numShards)(env)

        // Shutdown
        shutdownLedgerApiConformanceEnvironment(env)
        external.kill(remoteParticipant1.name)
        external.kill(remoteParticipant2.name)
        external.kill(remoteParticipant3.name)
    }
  }
}

private[continuity] object ProtocolContinuityConformanceTest {

  /** Computes the list of previous Canton releases that should be tested against the current
    * release for protocol continuity.
    *
    * A previous release is considered supported if its `(major, minor)` shares at least one stable
    * protocol version with the stable protocol versions supported by the current release.
    *
    * @return
    *   the latest patch of each previous supported `(major, minor)`, sorted ascending
    */
  def previousSupportedReleases(logger: TracedLogger)(implicit
      tc: TraceContext
  ): NonEmpty[List[TestedRelease]] = {
    val current = ReleaseVersion.current
    val protocolVersions = ProtocolVersion.stable
    // previous minor versions that share at least one protocol version with the current release
    val eligibleMinors: Map[(Int, Int), NonEmpty[List[ProtocolVersion]]] =
      majorMinorToStableProtocolVersions
        .flatMap { case (minor, pvs) =>
          NonEmpty.from(pvs.intersect(protocolVersions)).map(minor -> _)
        }

    val previousSupportedReleases = for {
      testToolRelease <- UseLedgerApiTestTool.latestReleases(logger)
      releaseVersion = ReleaseVersion.tryCreate(testToolRelease.version)
      if releaseVersion < current
      pvs <- eligibleMinors.get(releaseVersion.majorMinor).toList
    } yield TestedRelease(testToolRelease, releaseVersion, pvs)
    // Keep only the latest patch per (major, minor)
    val latestPatchPerMinor = previousSupportedReleases
      .groupBy(_.releaseVersion.majorMinor)
      .values
      .map(_.maxBy(_.releaseVersion))
      .toList
      .sortBy(_.releaseVersion)
    logger.info(
      s"Previous supported releases: ${latestPatchPerMinor.map(_.releaseVersion).mkString(", ")}"
    )
    NonEmpty.from(latestPatchPerMinor).getOrElse {
      throw new IllegalStateException(
        s"No previous supported releases found for current release ${current.toProtoPrimitive}."
      )
    }
  }

  def latestSupportedRelease(logger: TracedLogger)(implicit
      tc: TraceContext
  ): TestedRelease =
    previousSupportedReleases(logger).last1

  private[continuity] val removeConfigPaths: Set[(String, Option[(String, Any)])] =
    (1 to 3)
      .flatMap(p =>
        Seq(
          s"participants.participant$p.parameters.ledger-api-server.indexer.achs-config.buffer-size",
          s"participants.participant$p.ledger-api.index-service.contract-pruning-delay-before-retry",
          s"participants.participant$p.ledger-api.index-service.contract-pruning-max-retries",
        )
      )
      .map(_ -> Option.empty[(String, Any)])
      .toSet

}
