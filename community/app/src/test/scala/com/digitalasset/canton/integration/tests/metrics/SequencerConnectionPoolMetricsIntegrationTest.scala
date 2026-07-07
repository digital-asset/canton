// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.metrics

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.admin.api.client.data.{
  GrpcSequencerConnection,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
}
import com.digitalasset.canton.integration.bootstrap.NetworkTopologyDescription.MediatorSequencersConfiguration
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.{SequencerAlias, UniquePortGenerator, config}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

/** This test checks that the sequencer connection pool metrics properly obtain a `psid` label in
  * their context.
  *
  * The environment is as follows:
  *   - 2 participants
  *   - 4 sequencers
  *   - 1 mediator
  *
  * The participants and the mediator connect to all sequencers with a trust threshold of 4.
  *
  * The test first restarts all the participants because the `psid` is known only starting at the
  * second connect.
  *
  * The test then pings participant2 from participant1, and proceeds to examine all the connection
  * pool metrics for all participant and mediator nodes, validating that they have the `psid` label
  * where expected.
  */
final class SequencerConnectionPoolMetricsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Config
      .addConfigTransform(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = MetricQualification.All,
              reporters = Seq(
                MetricsReporterConfig.Prometheus(
                  port = UniquePortGenerator.next
                )
              ),
            )
          )
      )
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
                mediator1 -> MediatorSequencersConfiguration(
                  sequencers.local,
                  trustThreshold = PositiveInt.four,
                  livenessMargin = NonNegativeInt.zero,
                )
              )
            ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        val amplification = SubmissionRequestAmplification(
          factor = 20,
          patience = config.NonNegativeFiniteDuration.tryFromDuration(1.seconds),
        )

        Seq(participant1, participant2).foreach(
          _.synchronizers.connect_bft(
            sequencers.local.map(s =>
              GrpcSequencerConnection.fromInternal(
                s.config.publicApi.clientConfig
                  .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
              )
            ),
            synchronizerAlias = daName,
            sequencerTrustThreshold = PositiveInt.four,
            sequencerLivenessMargin = NonNegativeInt.zero,
            submissionRequestAmplification = amplification,
          )
        )
      }

  "SequencerConnectionPoolMetrics" should {
    "have the `psid` label after the first connect" in { implicit env =>
      import env.*

      val nodes = Seq[LocalInstanceReference](participant1, participant2, mediator1)

      // For participants, the `psid` will be populated starting at the second connect
      clue("disconnect and reconnect participants") {
        participants.all.foreach(_.synchronizers.disconnect_all())
        participants.all.foreach(_.synchronizers.reconnect_all())
      }

      participant1.health.ping(participant2)

      clue("check metrics") {
        val psidKeyName = "psid"
        val prefix = "daml.sequencer-client.sequencer-connection-pool"

        forAll(nodes) { node =>
          val metrics = node.metrics.list(prefix)
          // Ensure the test fails if we change the prefix, which would result in an empty `metrics`
          metrics.size should be >= 8

          node match {
            case _: LocalMediatorReference =>
              // Mediators have their `psid` always defined, even at their first connection
              forAll(metrics) { case (_name, values) =>
                values.size should be >= 1
                forAll(values)(_.attributes should contain key psidKeyName)
              }

            case _: LocalParticipantReference =>
              val uniqueMetrics = Seq(
                "trust-threshold",
                "tracked-connections",
                "validated-connections",
                "subscription-threshold",
                "active-subscriptions",
              ).map(m => s"$prefix.$m")

              forAll(metrics) {
                case (name, values) if uniqueMetrics.contains(name) =>
                  // These metrics don't have separate instances for different sets of labels, so they will all have the `psid`
                  values.loneElement.attributes should contain key psidKeyName

                case (name, values)
                    if name == s"$prefix.connection-health" || name == s"$prefix.subscription-health" =>
                  // The metrics on the first connect will be without `psid`, but they are closed when disconnecting.
                  // Metrics for the second connect will have the `psid`.
                  // With 4 sequencers and trust threshold = 4, we will have 4 connections and 4 subscriptions.
                  values should have size 4
                  forAll(values)(_.attributes should contain key psidKeyName)

                case (name, values) if name == s"$prefix.grpc-requests" =>
                  // These metrics are counters and are not closed when the pool closes, so the metrics without `psid` will be around

                  val metricsPerEndPoint = values.groupBy(_.attributes("endpoint"))
                  val endpointsWithPsidOnAllConnections = Seq(
                    "GetApiInfo",
                    "GetSynchronizerId",
                    "GetSynchronizerParameters",
                    "Handshake",
                    "Authenticate",
                    "Challenge",
                    "Subscribe",
                  )
                  val endpointsWithPsidOnSomeConnections = Seq(
                    "SendAsync",
                    "AcknowledgeSigned",
                  )

                  val endpointsWithoutPsid = Seq(
                    "DownloadTopologyStateForInit",
                    "DownloadTopologyStateForInitHash",
                  )

                  val endpointsNotCalled = Seq(
                    "Logout",
                    "GetTime",
                    "GetTrafficStateForMember",
                  )

                  forAll(metricsPerEndPoint) {
                    case (endpoint, metrics)
                        if endpointsWithPsidOnAllConnections.contains(endpoint) =>
                      // All the connections will have a `psid` because all the connections use these endpoints
                      forExactly(4, metrics)(_.attributes should contain key psidKeyName)

                    case (endpoint, metrics)
                        if endpointsWithPsidOnSomeConnections.contains(endpoint) =>
                      // Not all connections will necessarily have a `psid` because not all connections may use these endpoints
                      forAtLeast(1, metrics)(_.attributes should contain key psidKeyName)

                    case (endpoint, metrics) if endpointsWithoutPsid.contains(endpoint) =>
                      // There are no metric with `psid` because these endpoints are only used during the first connection
                      forAll(metrics)(_.attributes should not(contain key psidKeyName))

                    case (endpoint, _) if endpointsNotCalled.contains(endpoint) =>
                      // These endpoints should not be called during this test
                      fail(s"endpoint should not have been called: $endpoint")

                    case (endpoint, _) => fail(s"unknown endpoint: $endpoint")
                  }

                case (other, _) => fail(s"unexpected metric: $other")
              }

            case _ => fail(s"unexpected node type: $node")
          }
        }
      }
    }
  }
}
