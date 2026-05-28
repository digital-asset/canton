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
import com.digitalasset.canton.integration.plugins.UseExternalProcess.ReleasePath
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
import monocle.macros.syntax.lens.*
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
        exclude = excludedTests(version) ++ jsonExclusions,
        useJson = useJsonApi,
      )(env)
  }
  def excludedTests(version: ReleaseVersion): Seq[String] = {
    val perReleaseExclusions =
      if (version.majorMinor == (3, 4))
        Seq(
          // 3.4 returns "UNKNOWN_INFORMEES" while the test tool expects "Party not known on ledger".
          "ClosedWorldIT:ClosedWorldObserver",
          // 3.5 changed the invalid synchronizer-id error message; the 3.4 test tool still expects
          // the old "Invalid unique identifier ... with missing namespace" wording.
          "InteractiveSubmissionServiceIT:ISSExecuteAndWaitForTransactionInvalidSynchronizerId",
          "InteractiveSubmissionServiceIT:ISSExecuteAndWaitInvalidSynchronizerId",
          // 3.5 accepts duplicate disclosed contracts with the same payload (idempotence);
          // the 3.4 test tool still expects them to be rejected.
          "ExplicitDisclosureIT:EDDuplicates",
        )
      else Seq.empty
    LedgerApiConformanceBase.excludedTests ++ perReleaseExclusions
  }

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

  override def environmentDefinition: EnvironmentDefinition = {
    val env = EnvironmentDefinition.P3S1M1_Manual
      .addConfigTransforms(ConfigTransforms.clearMinimumProtocolVersion*)
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)
    if (disableBinaryVersionEnforcement)
      env.addConfigTransform(
        // Once we remove PV34, we can remove this config transform!
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.parameters.disableReleaseVersionHandshakeCheck).replace(true)
        )
      )
    else env
  }

  protected def numShards: Int
  protected def shard: Int
  protected def disableBinaryVersionEnforcement: Boolean = false

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

  testedReleases.foreach { case TestedRelease(release, protocolVersions) =>
    lazy val binDir = ReleaseUtils
      .retrieve(release)
      .futureValue(timeout = PatienceConfiguration.Timeout(2.minutes))
    lazy val pv = protocolVersions.max1

    s"run conformance tests of shard $shard with release $release and protocol $pv" in {
      implicit env =>
        import env.*

        val cantonReleaseVersion = ReleasePath(binDir)

        externalSynchronizer.start(
          remoteMediator1.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
        externalSynchronizer.start(
          remoteSequencer1.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
        participants.local.start()

        clue(
          "Waiting for remote sequencer1 to be ready for initialization. If this fails check the logs of the external process for any errors"
        ) {
          remoteSequencer1.health.wait_for_ready_for_initialization()
        }
        clue(
          "Waiting for remote mediator1 to be ready for initialization. If this fails check the logs of the external process for any errors"
        ) {
          remoteMediator1.health.wait_for_ready_for_initialization()
        }

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

  testedReleases.foreach { case TestedRelease(release, protocolVersions) =>
    lazy val binDir = ReleaseUtils
      .retrieve(release)
      .futureValue(timeout = PatienceConfiguration.Timeout(2.minutes))
    lazy val pv = protocolVersions.max1

    s"run conformance tests of shard $shard with release $release and protocol $pv" in {
      implicit env =>
        import env.*

        val cantonReleaseVersion = ReleasePath(binDir)

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
        external.start(
          remoteParticipant1.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
        external.start(
          remoteParticipant2.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
        external.start(
          remoteParticipant3.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
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

/** For a given release R, runs a cross-version ping test against:
  *   - 1x synchronizer based on the current branch with the highest protocol version supported by
  *     both the current branch and release R
  *   - 1x participant based on the current branch
  *   - 1x participant of release R
  *
  * This test ensures that a participant on the current branch and a release-R participant can ping
  * each other in both directions.
  */
trait ProtocolContinuityConformanceTestPing extends ProtocolContinuityConformanceTest {
  private val external = new UseExternalProcess(
    loggerFactory,
    externalParticipants = Set("participant2"),
    fileNameHint = this.getClass.getSimpleName,
    removeConfigPaths = ProtocolContinuityConformanceTest.removeConfigPaths,
  )

  registerPlugin(external)

  testedReleases.foreach { case TestedRelease(release, protocolVersions) =>
    lazy val binDir = ReleaseUtils
      .retrieve(release)
      .futureValue(timeout = PatienceConfiguration.Timeout(2.minutes))
    lazy val pv = protocolVersions.max1

    s"ping between current-branch participant and release $release participant (pv=$pv)" in {
      implicit env =>
        import env.*

        val cantonReleaseVersion = ReleasePath(binDir)

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

        // Local participant on the current branch
        participant1.start()
        participant1.health.wait_for_initialized()

        // External participant on release R
        external.start(
          remoteParticipant2.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
        remoteParticipant2.health.wait_for_initialized()

        // Connect both participants to the synchronizer
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        remoteParticipant2.synchronizers.connect_local(sequencer1, alias = daName)

        // Ping both directions
        participant1.health.ping(remoteParticipant2)
        remoteParticipant2.health.ping(participant1)

        external.kill(remoteParticipant2.name)
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
    *   for each previous supported `(major, minor)` the latest stable patch (if any) and the latest
    *   non-stable patch (snapshot/RC, if any), sorted ascending. At least one of the two exists per
    *   minor.
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

    val latestStableAndNonStablePerMinor = ReleaseUtils
      .listAllReleases()
      .filter(_ < current)
      .groupBy(_.majorMinor)
      .toSeq
      .flatMap { case (_, versions) =>
        val (stables, nonStables) = versions.partition(_.isStable)
        stables.maxOption.toList ++ nonStables.maxOption.toList
      }
      /*
        This version does not have the tooling that allows to filter out unknown config keys.
        Since nobody is using it anymore, we rely on testing 3.4.12 snapshot instead.
       */
      .filterNot(_ == ReleaseVersion(3, 4, 11))

    val tested = for {
      release <- latestStableAndNonStablePerMinor
      pvs <- eligibleMinors.get(release.majorMinor).toList
    } yield TestedRelease(release, pvs)

    val sorted = tested.toList.sortBy(_.releaseVersion)
    logger.info(
      s"Previous supported releases: ${sorted.map(_.releaseVersion).mkString(", ")}"
    )
    NonEmpty.from(sorted).getOrElse {
      throw new IllegalStateException(
        s"No previous supported releases found for current release ${current.toProtoPrimitive}."
      )
    }
  }

  private[continuity] val removeConfigPaths: Set[(String, Option[(String, Any)])] = {
    /*
    For some unknown reason, the missing key is reported as
      circuit-breaker.messages.lsu-sequencing-test
    instead of the full
      sequencers.sequencer1.sequencer.block.circuit-breaker.messages.lsu-sequencing-test

    We will keep this exception.
     */
    val perSequencer =
      Seq[String](
        "sequencers.sequencer1.sequencer.block.circuit-breaker.messages.lsu-sequencing-test"
      )
    perSequencer.map(_ -> Option.empty[(String, Any)]).toSet
  }

}
