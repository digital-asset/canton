// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.admin.api.client.data.{
  ComponentHealthState,
  GrpcSequencerConnection,
  SubmissionRequestAmplification,
  SubscriptionLivenessLimits,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToSequencerPublicApi,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.{SequencerAlias, config}
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt
import scala.util.Random

/** This test checks that a sequencer subscription properly detects when it is not receiving new
  * events, while the synchronizer is active and the aggregator keeps processing new events,
  * indicating other subscriptions do receive events. The test uses toxiproxy to block a
  * subscription from receiving new events.
  *
  * The environment is as follows:
  *   - 1 participant
  *   - 4 sequencers
  *   - 1 mediator
  *
  * The participant and the mediator connect to all sequencers with a trust threshold of 2 and a
  * liveness margin of 2. The participant connection to sequencer1 is controlled by toxiproxy.
  *
  * The test runs a sequence of 10 cycles (see `Cycle.daml` in `CantonExamples`) while freezing the
  * downstream connection to sequencer1. This generates 60 events in ~15 seconds on a dev machine.
  *
  * The sequence is run using three sets of `SubscriptionLivenessMetrics` configurations:
  *   - `maxTimestampDelta` = 1 day, `maxOrdinalDelta` = 42
  *   - `maxTimestampDelta` = 3 seconds, `maxOrdinalDelta` = 64738
  *   - `maxTimestampDelta` = 3 seconds, `maxOrdinalDelta` = 42
  *
  * The first two runs must pass without the subscription being detected as silent since only one of
  * the limits is exceeded. The third run must detect the silent subscription since both limits are
  * exceeded.
  */
sealed trait ToxiproxySilentSubscriptionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S4M1_Config
      // This is only implemented with the new aggregator
      .addConfigTransforms(ConfigTransforms.useNewAggregator(true))
      .withNetworkBootstrap { implicit env =>
        import env.*

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.local,
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> (sequencers.local,
                /* trust threshold */ PositiveInt.two, /* liveness margin */ NonNegativeInt.two)
              )
            ),
          )
        )
      }

  private val random = {
    val seed = Random.nextLong()
    logger.debug(s"Seed for randomness = $seed")
    new Random(seed)
  }

  private lazy val proxyConfSeq1 = {
    // Ensure toxics have a unique name (with high probability), thereby avoiding conflicts in CI
    val uniqueId = Seq.fill(10)(('a' + random.nextInt(26)).toChar).mkString("")

    ParticipantToSequencerPublicApi("sequencer1", name = s"toxi-$uniqueId-sequencer1")
  }
  private lazy val toxiProxyConf: UseToxiproxy.ToxiproxyConfig =
    UseToxiproxy.ToxiproxyConfig(Seq(proxyConfSeq1))
  protected lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  private lazy val proxySeq1 = toxiproxyPlugin.runningToxiproxy.getProxy(proxyConfSeq1.name).value

  "BFT Synchronizer" must {
    "not detect a subscription going silent if only max ordinal delta is exceeded" in {
      implicit env =>
        import env.*

        // Connect to sequencer1 through toxiproxy, and to sequencers2, 3 and 4 directly.
        val sequencerConnections = GrpcSequencerConnection.tryCreate(
          s"http://${proxySeq1.ipFromHost}:${proxySeq1.portFromHost}",
          sequencerAlias = proxySeq1.underlying.getName,
        ) +: Seq(sequencer2, sequencer3, sequencer4).map(s =>
          GrpcSequencerConnection.fromInternal(
            s.config.publicApi.clientConfig
              .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
          )
        )

        val amplification = SubmissionRequestAmplification(
          factor = 20,
          patience = config.NonNegativeFiniteDuration.tryFromDuration(1.seconds),
        )

        clue("Connect participant1 to the synchronizer") {
          participant1.synchronizers.connect_bft(
            sequencerConnections,
            sequencerTrustThreshold = PositiveInt.two,
            sequencerLivenessMargin = NonNegativeInt.two,
            submissionRequestAmplification = amplification,
            subscriptionLivenessLimits = SubscriptionLivenessLimits(
              maxTimestampDelta = config.NonNegativeFiniteDuration.ofDays(1),
              maxOrdinalDelta = NonNegativeInt.tryCreate(42),
            ),
            synchronizerAlias = daName,
            physicalSynchronizerId = Some(daId),
          )
          waitUntilSubscriptionToSequencer1IsUp()
        }

        runCyclesBlockingASubscription(expectedDetection = false)
    }

    "not detect a subscription going silent if only max timestamp delta is exceeded" in {
      implicit env =>
        import env.*

        clue("Modify SubscriptionLiveness parameters to (3sec, 64738)") {
          participant1.synchronizers.modify(
            synchronizerAlias = daName,
            modifier = _.focus(_.sequencerConnections.subscriptionLivenessLimits).replace(
              SubscriptionLivenessLimits(
                maxTimestampDelta = config.NonNegativeFiniteDuration.ofSeconds(3),
                maxOrdinalDelta = NonNegativeInt.tryCreate(64738),
              )
            ),
          )
        }

        clue("Reconnect to the synchronizer") {
          participant1.synchronizers.reconnect_all()
          waitUntilSubscriptionToSequencer1IsUp()
        }

        // This produces 60 events in about 15s (locally)
        runCyclesBlockingASubscription(expectedDetection = false)
    }

    "detect a subscription going silent if both limits are exceeded" in { implicit env =>
      import env.*

      clue("Modify SubscriptionLiveness parameters to (3sec, 42)") {
        participant1.synchronizers.modify(
          synchronizerAlias = daName,
          modifier = _.focus(_.sequencerConnections.subscriptionLivenessLimits).replace(
            SubscriptionLivenessLimits(
              maxTimestampDelta = config.NonNegativeFiniteDuration.ofSeconds(3),
              maxOrdinalDelta = NonNegativeInt.tryCreate(42),
            )
          ),
        )
      }

      clue("Reconnect to the synchronizer") {
        participant1.synchronizers.reconnect_all()
        waitUntilSubscriptionToSequencer1IsUp()
      }

      runCyclesBlockingASubscription(expectedDetection = true)
    }
  }

  private def waitUntilSubscriptionToSequencer1IsUp()(implicit env: FixtureParam): Unit = {
    import env.*
    eventually() {
      val seq1Status = participant1.health.status.trySuccess.components
        .filter(cs =>
          cs.name.startsWith("subscription-sequencer-connection-") && cs.name.contains("sequencer1")
        )
        .loneElement
      seq1Status.state shouldBe ComponentHealthState.Ok()
    }
  }

  private def runCyclesBlockingASubscription(
      expectedDetection: Boolean
  )(implicit env: FixtureParam): Unit = {
    import env.*

    val proxyName = proxySeq1.underlying.getName

    // Setting the timeout to 0 effectively disables the timeout, and data is dropped until the toxic is removed.
    // We activate the toxic only downstream, so submission requests to sequencer1 don't detect an error
    // immediately (which would restart the connection and therefore the subscription).
    val toxic =
      proxySeq1.underlying.toxics().timeout(proxyName, ToxicDirection.DOWNSTREAM, 0)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        clue(s"Run cycles with toxic $proxyName")(
          // This produces 60 events in about 15s (locally)
          (1 to 10).foreach(_ => runCycle(participant1.adminParty, participant1, participant1))
        )
        clue("Run one more cycle without toxic") {
          toxic.remove()
          runCycle(participant1.adminParty, participant1, participant1)
        }
        clue("Disconnect from synchronizer") {
          participant1.synchronizers.disconnect_all()
        }
      },
      LogEntry.assertLogSeq(
        mustContainWithClue =
          if (expectedDetection)
            Seq(
              (
                _.warningMessage should include(
                  "Subscription inactivity exceeded configured limits"
                ),
                "Silent subscription detected",
              )
            )
          else Seq.empty,
        // The failed submissions get noticed eventually
        mayContain = Seq(
          _.warningMessage should include("UNAVAILABLE/Network closed for unknown reason"),
          _.warningMessage should include("UNAVAILABLE/Channel shutdownNow invoked"),
        ),
      ),
    )
  }
}

class ToxiproxySilentSubscriptionIntegrationTestDefault
    extends ToxiproxySilentSubscriptionIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiproxyPlugin)
}
