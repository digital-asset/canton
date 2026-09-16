// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.metrics

import com.daml.metrics.MetricsFilterConfig
import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.{MetricValue, MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.{UniquePortGenerator, config}
import monocle.macros.syntax.lens.*

final class ReassignmentMetricsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  private val metricsPrefix = "daml.participant.sync.reassignments"

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = MetricQualification.All,
              reporters = Seq(
                MetricsReporterConfig.Prometheus(
                  port = UniquePortGenerator.next,
                  filters = Seq(MetricsFilterConfig(metricsPrefix)),
                )
              ),
            )
          ),
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        Seq(daId, acmeId)
          .foreach(psid => participant1.dars.upload(CantonExamplesPath, synchronizerId = psid))

        // Disable automatic assignment
        sequencer2.topology.synchronizer_parameters.propose_update(
          acmeId,
          _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
        )
      }

  "Reassignment metrics" should {
    "be recorded with the values and labels each one is meant to carry" in { implicit env =>
      import env.*

      val party = participant1.adminParty
      val ious = Seq(1.0, 2.0).map(amount =>
        IouSyntax.createIou(participant1, Some(daId))(party, party, amount)
      )

      // Two contracts in one request, so that the batch size cannot be confused with a request count
      val cids = ious.map(iou => LfContractId.assertFromString(iou.id.contractId))

      participant1.ledger_api.commands.submit_reassign(party, cids, daId, acmeId)

      val source = daId.logical.toProtoPrimitive
      val target = acmeId.logical.toProtoPrimitive

      // The counters are incremented after the command returns.
      eventually() {
        val metrics = participant1.metrics.list(metricsPrefix)

        def valuesOf(name: String): Seq[MetricValue] =
          metrics.getOrElse(s"$metricsPrefix.$name", fail(s"No metric $metricsPrefix.$name"))

        // One counter carries both the unassignment and the assignment, told apart by `type`
        def assertCounter(name: String, labels: (String, String)*): Unit =
          withClue(s"$name{${labels.map { case (k, v) => s"$k=$v" }.mkString(",")}}: ") {
            val matching = valuesOf(name).collect {
              case point: MetricValue.LongPoint if labels.forall { case (key, expected) =>
                    point.attributes.get(key).contains(expected)
                  } =>
                point
            }
            inside(matching) { case Seq(point) =>
              point.value shouldBe 1L
              // A label this metric does not carry must be absent, not present and empty
              val labelled = labels.map { case (key, _) => key }
              forAll(Set("source", "target") -- labelled)(key =>
                point.attributes.keySet should not contain key
              )
            }
          }

        forAll(Seq("unassignment", "assignment")) { reassignmentType =>
          val labels = Seq("type" -> reassignmentType, "source" -> source, "target" -> target)
          assertCounter("submitted", labels*)
          assertCounter("finalized", labels*)
        }
        forAll(Seq("unassignment", "assignment")) { reassignmentType =>
          assertCounter("requests", "type" -> reassignmentType)

          // The unassignment and the assignment of one reassignment carry the same contracts
          val histograms = valuesOf("batch-size").collect {
            case histogram: MetricValue.Histogram
                if histogram.attributes.get("type").contains(reassignmentType) =>
              histogram
          }
          inside(histograms) { case Seq(histogram) =>
            histogram.count shouldBe 1L
            histogram.sum shouldBe 2.0
          }
        }

        inside(valuesOf("unassignment.local-target-timestamp-lag")) {
          case Seq(histogram: MetricValue.Histogram) =>
            histogram.count shouldBe 1L
            histogram.attributes.get("source").value shouldBe source
            histogram.attributes.get("target").value shouldBe target
        }
      }
    }
  }
}
