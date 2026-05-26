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
  *
  * {{{
  * | branch       | Latest                              | All                                   |
  * |--------------|-------------------------------------|---------------------------------------|
  * | main         | latest snapshot of release-line-3.5 | latest snapshot+stable for (3.5, 3.4) |
  * | release-3.5  | latest snapshot of 3.5.x            | latest snapshot+stable for (3.5, 3.4) |
  * | release-3.4  | latest snapshot of 3.4.x            | latest snapshot+stable for (3.4)      |
  * }}}
  *
  * PV used: highest stable PV shared with the current build. Only releases that share at least one
  * stable PV with the current build are included in the test matrix.
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

  testedReleases.foreach { case TestedRelease(release, protocolVersions) =>
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
  override def disableBinaryVersionEnforcement: Boolean = true
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

        // Local participant on the current branch
        participant1.start()
        participant1.health.wait_for_initialized()

        // External participant on release R
        external.start(remoteParticipant2.name, cantonReleaseVersion)
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
      .filter(r =>
        Ordering[(Int, Int, Int)].lteq(r.majorMinorPatch, current.majorMinorPatch) && r != current
      )
      .groupBy(_.majorMinor)
      .toSeq
      .flatMap { case (_, versions) =>
        val (stables, nonStables) = versions.partition(_.isStable)
        stables.maxOption.toList ++ nonStables.maxOption.toList
      }

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
    val topLevel = Seq(
      "monitoring.logging.api.debug-in-process-requests",
      "monitoring.logging.api.prefix-grpc-addresses",
      "monitoring.sanitize-public-error-messages",
      "parameters.threading",
      "parameters.fail-on-unknown-config-keys",
    )
    val perParticipant = (1 to 3).flatMap { p =>
      val base = s"participants.participant$p"
      Seq(
        s"$base.admin-api.max-concurrent-calls-per-connection",
        s"$base.crypto.parallelism",
        s"$base.crypto.session-signing-keys",
        s"$base.ledger-api.index-service.max-lookup-limit",
        s"$base.ledger-api.index-service.contract-pruning-delay-before-retry",
        s"$base.ledger-api.index-service.contract-pruning-max-retries",
        s"$base.ledger-api.interactive-submission-service.maximum-number-of-signatures-per-party",
        s"$base.ledger-api.max-concurrent-calls-per-connection",
        s"$base.ledger-api.postgres-data-source.client-connection-check-interval",
        s"$base.ledger-api.postgres-data-source.network-timeout",
        s"$base.ledger-api.state-service",
        s"$base.ledger-api.topology-aware-package-selection.max-passes-default",
        s"$base.ledger-api.topology-aware-package-selection.max-passes-limit",
        s"$base.ledger-api.update-service",
        s"$base.parameters.alpha-multi-synchronizer-support",
        s"$base.parameters.caching.bft-ordering-batch-cache",
        s"$base.parameters.caching.sequencer-catchup-payload-cache",
        s"$base.parameters.commit-after-failed-activeness-check",
        s"$base.parameters.commitment-use-db-snapshot-for-participant-lookup",
        s"$base.parameters.validate-legacy-contracts-v-11",
        s"$base.parameters.ledger-api-server.indexer.achs-config",
        s"$base.parameters.ledger-api-server.indexer.postgres-data-source",
        s"$base.parameters.ledger-api-server.indexer.submission-batch-insertion-size",
        s"$base.parameters.ledger-api-server.indexer.use-weighted-batching",
        s"$base.parameters.lsu",
        s"$base.sequencer-client.amplify-on-max-sequencing-time-too-far",
        s"$base.sequencer-client.channel-flow-control-window",
        s"$base.sequencer-client.channel-max-inbound-message-size",
        s"$base.sequencer-client.keep-alive-client.idle-timeout",
        s"$base.sequencer-client.keep-alive-client.keep-alive-without-calls",
      )
    }
    val perMediator = {
      val base = "mediators.mediator1"
      Seq(
        s"$base.admin-api.max-concurrent-calls-per-connection",
        s"$base.caching.bft-ordering-batch-cache",
        s"$base.caching.sequencer-catchup-payload-cache",
        s"$base.crypto.parallelism",
        s"$base.crypto.session-signing-keys",
        s"$base.parameters.caching.bft-ordering-batch-cache",
        s"$base.parameters.caching.sequencer-catchup-payload-cache",
        s"$base.sequencer-client.amplify-on-max-sequencing-time-too-far",
        s"$base.sequencer-client.channel-flow-control-window",
        s"$base.sequencer-client.channel-max-inbound-message-size",
        s"$base.sequencer-client.keep-alive-client.idle-timeout",
        s"$base.sequencer-client.keep-alive-client.keep-alive-without-calls",
      )
    }
    val perSequencer = {
      val base = "sequencers.sequencer1"
      Seq(
        s"$base.admin-api.max-concurrent-calls-per-connection",
        s"$base.crypto.parallelism",
        s"$base.crypto.session-signing-keys",
        s"$base.declarative",
        s"$base.parameters.caching.bft-ordering-batch-cache",
        s"$base.parameters.caching.sequencer-catchup-payload-cache",
        s"$base.parameters.delay-requests-before-lsu-traffic-init",
        s"$base.parameters.disable-submission-checks-for-testing",
        s"$base.parameters.lsu-repair",
        s"$base.parameters.lsu",
        s"$base.parameters.produce-post-ordering-topology-ticks",
        s"$base.parameters.unsafe-sequencer-channel-support",
        // Once we remove PV34, we can remove this exception
        s"$base.parameters.disable-release-version-handshake-check",
        s"$base.public-api.max-concurrent-calls-per-connection",
        s"$base.sequencer-client.amplify-on-max-sequencing-time-too-far",
        s"$base.sequencer-client.channel-flow-control-window",
        s"$base.sequencer-client.channel-max-inbound-message-size",
        s"$base.sequencer-client.keep-alive-client.idle-timeout",
        s"$base.sequencer-client.keep-alive-client.keep-alive-without-calls",
        s"$base.sequencer.block.circuit-breaker.messages.lsu-sequencing-test",
        s"$base.sequencer.block.throughput-cap.strict",
        s"$base.sequencer.block.throughput-cap.thresholds",
        s"$base.sequencer.block.throughput-cap.update-every-ms",
      )
    }
    (topLevel ++ perParticipant ++ perMediator ++ perSequencer)
      .map(_ -> Option.empty[(String, Any)])
      .toSet
  }

}
