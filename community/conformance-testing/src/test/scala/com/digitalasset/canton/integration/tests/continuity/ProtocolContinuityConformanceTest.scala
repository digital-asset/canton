// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.MediatorReference
import com.digitalasset.canton.integration.bootstrap.NetworkTopologyDescription.MediatorSequencersConfiguration
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.plugins.UseExternalProcess.ReleasePath
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiUserManagementServiceSuppressionRule,
  DbActiveContractStoreConsistencyCheckSuppressionRule,
}
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.util.ReleaseUtils.TestedRelease
import com.digitalasset.canton.version.ReleaseVersionToProtocolVersions.majorMinorToStableProtocolVersions
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import com.digitalasset.nonempty.NonEmpty
import monocle.macros.syntax.lens.*
import org.scalatest.concurrent.PatienceConfiguration

import scala.concurrent.duration.DurationInt

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
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def connectedSynchronizersCount = 1

  protected def baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Manual

  override def environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
      .addConfigTransforms(ConfigTransforms.clearMinimumProtocolVersion*)
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set("participant1", "participant2", "participant3")
        )
      )

  protected def numShards: Int
  protected def shard: Int

}

/** For a given release R, the Ledger API conformance test suites at release R are run against:
  *   - 1x synchronizer with the latest protocol version of release R, consisting of
  *     - 1x sequencer and 1x mediator of release R (sequencer1, mediator1)
  *     - 1x sequencer and 1x mediator based on current main (sequencer2, mediator2)
  *   - 1x participant based on current main (participant1)
  *   - 2x participants of release R (participant2, participant3)
  *
  * All participants and mediators read from both sequencers with a trust threshold of 2, and the
  * mediator threshold is 2. Hence, sequenced events and confirmation results are only accepted if
  * the release-R and the current-main nodes agree.
  */
trait ProtocolContinuityConformanceTestMixedVersions extends ProtocolContinuityConformanceTest {
  override protected def baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Manual

  private lazy val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set("participant2", "participant3"),
      externalSequencers = Set("sequencer1"),
      externalMediators = Set("mediator1"),
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

        external.start(
          remoteMediator1.name,
          cantonReleaseVersion,
          failOnUnknownConfigKeys = false,
        )
        external.start(
          remoteSequencer1.name,
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
        sequencer2.start()
        mediator2.start()
        participant1.start()

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
        sequencer2.health.wait_for_ready_for_initialization()
        mediator2.health.wait_for_ready_for_initialization()

        val releaseIdentityTransactions =
          // generate_onboarding_transactions is the recommended way to retrieve
          // the identity transactions, but it was only introduced in 3.7, so we
          // need to branch here.
          if (Ordering.Tuple2[Int, Int].lt(release.majorMinor, (3, 7)))
            remoteSequencer1.topology.transactions.identity_transactions() ++
              remoteMediator1.topology.transactions.identity_transactions()
          else
            remoteSequencer1.topology.transactions.generate_onboarding_transactions(pv) ++
              remoteMediator1.topology.transactions.generate_onboarding_transactions(pv)
        val identityTransactions = releaseIdentityTransactions ++
          sequencer2.topology.transactions.generate_onboarding_transactions(pv) ++
          mediator2.topology.transactions.generate_onboarding_transactions(pv)

        val mediatorSequencers = MediatorSequencersConfiguration(
          sequencers.all,
          trustThreshold = PositiveInt.two,
          livenessMargin = NonNegativeInt.zero,
        )
        val staticParams = StaticSynchronizerParameters.defaults(protocolVersion = pv)
        NetworkBootstrapper(
          Seq(
            NetworkTopologyDescription.createWithStaticSynchronizerParameters(
              daName,
              synchronizerOwners = Seq(remoteSequencer1),
              synchronizerThreshold = PositiveInt.one,
              sequencers = sequencers.all,
              mediators = Seq(remoteMediator1, mediator2),
              staticSynchronizerParameters = staticParams,
              identityTransactions = Some(identityTransactions),
              mediatorThreshold = PositiveInt.two,
              overrideMediatorToSequencers = Some(
                Map[MediatorReference, MediatorSequencersConfiguration](
                  remoteMediator1 -> mediatorSequencers,
                  mediator2 -> mediatorSequencers,
                )
              ),
            )
          )
        )(env).bootstrap()

        remoteSequencer1.health.wait_for_initialized()
        sequencer2.health.wait_for_initialized()
        remoteMediator1.health.wait_for_initialized()
        mediator2.health.wait_for_initialized()
        participant1.health.wait_for_initialized()
        remoteParticipant2.health.wait_for_initialized()
        remoteParticipant3.health.wait_for_initialized()

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

        external.kill(remoteParticipant2.name)
        external.kill(remoteParticipant3.name)
        external.kill(remoteMediator1.name)
        external.kill(remoteSequencer1.name)
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

        val staticParams = StaticSynchronizerParameters.defaults(protocolVersion = pv)
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
      /*
       Filter out releases:
        - that are not older than the current release
        - containing "api" in the key because each release comes once with and once without -api suffix
       */
      .filter(r => r < current && !r.fullVersion.contains("-api"))
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
    val perSequencer = {
      val base = "sequencers.sequencer1"

      Seq[String](
        s"$base.sequencer.block.circuit-breaker.messages.lsu-sequencing-test",
        // introduced with 3.5.2
        s"$base.parameters.enable-async-sequencer-logging",
        s"$base.sequencer.block.throughput-cap.messages.confirmation-response", // Introduced with 3.6
        s"$base.sequencer.block.throughput-cap.delayed-activation", // Introduced with 3.6
      )
    }

    perSequencer.map(_ -> Option.empty[(String, Any)]).toSet
  }

}
