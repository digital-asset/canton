// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.admin.api.client.data.{
  GrpcSequencerConnection,
  SequencerConnectionPoolDelays,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
  SequencingParameters as ConsoleSequencingParameters,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.console.{InstanceReference, SequencerReference}
import com.digitalasset.canton.integration.bootstrap.NetworkTopologyDescription.MediatorSequencersConfiguration
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseExternalProcess,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.WriteReadiness
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.SegmentLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  BlacklistLeaderSelectionPolicyConfig,
  SequencingParameters,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.{SequencerAlias, config}
import org.scalatest.Assertion

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

/** This test checks that the sequencer connection pool properly recovers connections and
  * subscriptions when sequencers go down.
  *
  * The environment is as follows:
  *   - 2 participants
  *   - 7 sequencers (4 external, 3 local)
  *   - 1 mediator
  *
  * The participants and the mediator connect to all the sequencers with a trust threshold of 2 and
  * a liveness margin of 0. When they start up, all sequencers are up, which means that they will
  * have connections to all of them for sending, but only two subscriptions on random sequencers
  * will be started (since the liveness margin is 0).
  *
  * The test then executes the following scenario:
  *
  *   - Ping from participant 1 to participant 2
  *   - Stop sequencers 3 and 4
  *   - Ping again (nodes must acquire subscriptions on sequencers 1 and 2 if they did not yet have
  *     them)
  *   - Restart sequencers 3 and 4, stop sequencers 1 and 2
  *   - Ping again (nodes must acquire subscriptions on sequencers 3 and 4)
  *   - Restart sequencers 1 and 2
  *
  * This scenario is executed once using a graceful stop to bring the sequencers down, and a second
  * time using a kill. This checks that connection failures are detected properly even when they are
  * not gracefully closed.
  */
final class BftSequencerConnectionsIntegrationTestDefault
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(externalPlugin)

  private lazy val externalPlugin = new UseExternalProcess(
    loggerFactory,
    externalSequencers = Set("sequencer1", "sequencer2", "sequencer3", "sequencer4"),
    fileNameHint = this.getClass.getSimpleName,
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        // 4 external sequencers + 3 local sequencers (3f+1 with f=2, so we can crash 2 a time)
        numSequencers = 7,
        numMediators = 1,
      )
      .addConfigTransforms(
        // Increase the acknowledgement interval to avoid flakes with failing acknowledgements log messages
        ConfigTransforms.updateSequencerClientAcknowledgementInterval(
          NonNegativeFiniteDuration.tryOfMinutes(60)
        )
      )
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        sequencers.remote.foreach { seq =>
          logger.debug(s"Starting sequencer ${seq.name}")
          externalPlugin.start(seq.name)
        }
        sequencers.remote.foreach { seq =>
          seq.health.wait_for_running()
          logger.debug(s"Sequencer ${seq.name} is running")
        }
        sequencers.local.foreach(_.start())
      }
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](remoteSequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.all,
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> MediatorSequencersConfiguration(
                  sequencers.remote,
                  trustThreshold = PositiveInt.two,
                  livenessMargin = NonNegativeInt.zero,
                )
              )
            ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        sequencers.all.foreach { seq =>
          seq.health.wait_for_initialized()
          logger.debug(s"Sequencer ${seq.name} is initialized")
        }
      }

  private lazy val expectedLogEntries = Seq[LogEntry => Assertion](
    _.warningMessage should include regex
      raw"Request failed for server-.*\. Is the server running\? Did you configure the server address as 0\.0\.0\.0\?" +
      raw" Are you using the right TLS settings\?",
    _.errorMessage should (include regex
      raw"Request failed for server-.*\." and include("GrpcServerError: UNKNOWN/channel closed")),
    _.warningMessage should include("Detected late processing (or clock skew)"),
  )

  private def pingWithSequencersDown()(implicit env: TestConsoleEnvironment) = {
    import env.*

    val pingTimeout = config.NonNegativeDuration.ofSeconds(120)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      participant1.health.ping(participant2.id, timeout = pingTimeout),
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        mayContain = expectedLogEntries,
      ),
    )
  }

  "BFT Synchronizer" must {
    "Initialize the setup" in { implicit env =>
      import env.*

      val amplification = SubmissionRequestAmplification(
        factor = 10,
        patience = config.NonNegativeFiniteDuration.tryFromDuration(5.seconds),
      )

      clue("reconfigure mediator to use amplification") {
        mediator1.sequencer_connection.modify_connections { old =>
          SequencerConnections.tryMany(
            connections = old.connections,
            sequencerTrustThreshold = old.sequencerTrustThreshold,
            sequencerLivenessMargin = old.sequencerLivenessMargin,
            submissionRequestAmplification = amplification,
            // Make the warning delay large to avoid these warnings in the test
            sequencerConnectionPoolDelays =
              old.sequencerConnectionPoolDelays.copy(warnValidationDelay = 1.day),
            subscriptionLivenessLimits = old.subscriptionLivenessLimits,
          )
        }
      }

      val connectionsConfig = sequencers.remote.map(s =>
        GrpcSequencerConnection.fromInternal(
          s.config.publicApi
            .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
        )
      )
      val delays = SequencerConnectionPoolDelays.default.copy(warnValidationDelay =
        config.NonNegativeFiniteDuration.tryFromDuration(1.days)
      )

      clue("connect participant1 to all sequencers") {
        participant1.start()
        participant1.synchronizers.connect_bft(
          connections = connectionsConfig,
          sequencerTrustThreshold = PositiveInt.two,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = amplification,
          sequencerConnectionPoolDelays = delays,
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
          validation = SequencerConnectionValidation.Disabled,
        )
      }

      clue("connect participant2 to all sequencers") {
        participant2.start()
        participant2.synchronizers.connect_bft(
          connections = connectionsConfig,
          sequencerTrustThreshold = PositiveInt.two,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = amplification,
          sequencerConnectionPoolDelays = delays,
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
          validation = SequencerConnectionValidation.Disabled,
        )
      }

      clue("update sequencing parameters") {
        val newSequencingParameters = SequencingParameters
          .Default(testedProtocolVersion)
          .update(
            // Since this test is starting and stopping sequencers a lot there will be view-changes in CantonBFT.
            // The default timeout is 10s which can make some blocks be delayed by much, so we lower the view-change timeout and segment length,
            // as well as disable blacklisting to avoid that nodes get blacklisted because of view-changes in this test.
            pbftViewChangeTimeout = PositiveFiniteDuration.tryCreate(250.millis.toJava),
            segmentLength = SegmentLength(PositiveLong.tryCreate(2L)),
            blacklistLeaderSelectionPolicyConfig = BlacklistLeaderSelectionPolicyConfig(
              howLongToBlacklist = HowLongToBlacklist.NoBlacklisting,
              howManyCanWeBlacklist = HowManyCanWeBlacklist.NoBlacklisting,
            ),
          )
        remoteSequencer1.topology.sequencing_parameters.propose(
          synchronizer1Id.logical,
          ConsoleSequencingParameters(Some(newSequencingParameters.toByteString)),
        )
        eventually() {
          remoteSequencer1.bft
            .get_ordering_topology()
            .sequencingParameters shouldBe newSequencingParameters
        }
      }
    }

    "Handle sequencers stopped graciously" in { implicit env =>
      handleFailingSequencers(forceKill = false)
    }

    "Restart sequencers 1 and 2" in { implicit env =>
      import env.*

      Seq(remoteSequencer1, remoteSequencer2).foreach(seq => externalPlugin.start(seq.name))
      waitForRestartedSequencers(Seq(remoteSequencer1, remoteSequencer2))
    }

    "Handle killed sequencers" in { implicit env =>
      handleFailingSequencers(forceKill = true)
    }
  }

  private def handleFailingSequencers(
      forceKill: Boolean
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    clue("Perform an initial ping") {
      participant1.health.ping(participant2.id)
    }

    clue("Stop sequencers 3 and 4") {
      Seq(remoteSequencer3, remoteSequencer4).foreach(seq =>
        externalPlugin.kill(seq.name, force = forceKill)
      )
    }
    clue("Ping again") {
      pingWithSequencersDown()
    }

    clue("Restart sequencers 3 and 4, stop sequencers 1 and 2") {
      Seq(remoteSequencer3, remoteSequencer4).foreach(seq => externalPlugin.start(seq.name))
      waitForRestartedSequencers(Seq(remoteSequencer3, remoteSequencer4))
      Seq(remoteSequencer1, remoteSequencer2).foreach(seq =>
        externalPlugin.kill(seq.name, force = forceKill)
      )
    }
    clue("Ping yet again") {
      pingWithSequencersDown()
    }
  }

  /** With 7 sequencers (f=2), ordering needs 5 live nodes. Stopping the next pair before the
    * restarted pair has rejoined stalls ordering until they do, which delays the delivery of the
    * ping messages enough to trigger "Detected late processing" warnings and submission timeouts.
    */
  private def waitForRestartedSequencers(
      restarted: Seq[SequencerReference]
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    restarted.foreach(_.health.wait_for_running())
    val referenceEpoch = sequencers.local.head.bft.get_ordering_topology().currentEpoch
    restarted.foreach { seq =>
      eventually(
        timeUntilSuccess = 60.seconds,
        retryOnTestFailuresOnly = false,
        logElapsed = Some(s"Waiting for sequencer ${seq.name} to rejoin ordering"),
      ) {
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            seq.bft.get_write_readiness() shouldBe a[WriteReadiness.Ready]
            seq.bft.get_ordering_topology().currentEpoch should be >= referenceEpoch
          },
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq.empty,
            mayContain = Seq(
              _.errorMessage should include(
                s"Request failed for ${seq.name}. Is the server initialized or is the server incompatible?"
              )
            ),
          ),
        )
      }
    }
  }
}
