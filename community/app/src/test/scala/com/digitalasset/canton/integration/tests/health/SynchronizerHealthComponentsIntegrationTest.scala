// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.health

import com.digitalasset.canton.admin.api.client.data.{ComponentHealthState, ParticipantStatus}
import com.digitalasset.canton.health.ComponentStatus as ComponentStatusInternal
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.sync.{ConnectedSynchronizer, SyncEphemeralState}
import com.digitalasset.canton.sequencing.client.SequencerClient

/** Checks that the participant status report contains one health component entry per component type
  * and connected synchronizer (labeled with the physical synchronizer id).
  */
sealed trait SynchronizerHealthComponentsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1_S1M1

  private lazy val perSynchronizerComponentTypes: Set[String] = Set(
    ConnectedSynchronizer.healthName,
    SyncEphemeralState.healthName,
    SequencerClient.healthName,
    AcsCommitmentProcessor.healthName,
  )

  /** Parses the status report components into: component type -> (synchronizer label -> state).
    * Only components labeled with a synchronizer are returned.
    */
  private def labeledComponents(
      status: ParticipantStatus
  ): Map[String, Map[String, ComponentHealthState]] =
    status.components
      .flatMap(c =>
        c.labels.get(ComponentStatusInternal.SynchronizerLabelKey).map { sync =>
          (c.name, sync, c.state)
        }
      )
      .groupMap { case (componentType, _, _) => componentType } { case (_, sync, state) =>
        sync -> state
      }
      .view
      .mapValues(_.toMap)
      .toMap

  private def participantStatus(implicit
      env: FixtureParam
  ): ParticipantStatus = env.participant1.health.status.trySuccess

  "report no synchronizer-labeled components when not connected" in { implicit env =>
    val status = participantStatus
    status.connectedSynchronizers shouldBe empty
    labeledComponents(status) shouldBe empty
  }

  "report one entry per component type for a single connected synchronizer" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.health.wait_for_initialized()
    participant1.health.ping(participant1)

    val status = participantStatus
    status.connectedSynchronizers.keys.toSeq shouldBe Seq(daId)

    val components = labeledComponents(status)
    components.keySet should contain allElementsOf perSynchronizerComponentTypes

    forEvery(perSynchronizerComponentTypes) { componentType =>
      val entries = components(componentType)
      entries.keySet shouldBe Set(daId.toProtoPrimitive)
      entries(daId.toProtoPrimitive) shouldBe a[ComponentHealthState.Ok]
    }
  }

  "report entries for each connected synchronizer" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)

    eventually() {
      val status = participantStatus
      status.connectedSynchronizers.keys.toSet shouldBe Set(daId, acmeId)

      val components = labeledComponents(status)
      forEvery(perSynchronizerComponentTypes) { componentType =>
        val entries = components(componentType)
        entries.keySet shouldBe Set(daId.toProtoPrimitive, acmeId.toProtoPrimitive)
        forEvery(entries.values)(_ shouldBe a[ComponentHealthState.Ok])
      }
    }
  }

  "restrict the status to a single synchronizer when an id is given" in { implicit env =>
    import env.*

    eventually() {
      val fullStatus = participantStatus
      fullStatus.connectedSynchronizers.keys.toSet shouldBe Set(daId, acmeId)

      val filtered = participant1.health.status(daId).trySuccess

      filtered.connectedSynchronizers.keys.toSeq shouldBe Seq(daId)
      val components = labeledComponents(filtered)
      components should not be empty
      forEvery(components.values)(entries => entries.keySet shouldBe Set(daId.toProtoPrimitive))

      // node-level (unlabeled) components are kept
      def nodeLevel(status: ParticipantStatus): Set[String] =
        status.components.filter(_.labels.isEmpty).map(_.name).toSet
      nodeLevel(filtered) shouldBe nodeLevel(fullStatus)

      // a logical synchronizer id matches all physical instances of that synchronizer
      val filteredByLogical = participant1.health.status(daId.logical).trySuccess
      filteredByLogical.connectedSynchronizers.keys.toSeq shouldBe Seq(daId)
      labeledComponents(filteredByLogical) shouldBe components
    }
  }

  "render per-synchronizer component sections in the status report" in { implicit env =>
    import env.*

    eventually() {
      val status = participantStatus
      val rendered = status.toString

      // component names stay canonical, the synchronizer id travels in the labels only
      forEvery(status.components.map(_.name)) { name =>
        name should not include daId.toString
        name should not include acmeId.toString
      }

      // the synchronizer section lists one sub-section per synchronizer, sorted by id;
      // the full psid stored in the labels is truncated to the display form at render time
      rendered should include("Synchronizers:")
      rendered should not include daId.toProtoPrimitive
      rendered should not include acmeId.toProtoPrimitive
      val daIdx = rendered.indexOf(s"\t\t${daId.toString}")
      val acmeIdx = rendered.indexOf(s"\t\t${acmeId.toString}")
      forEvery(Seq(daIdx, acmeIdx))(_ should be >= 0)
      // sorted by synchronizer id
      if (daId.toString < acmeId.toString) daIdx should be < acmeIdx
      else acmeIdx should be < daIdx

      forEvery(perSynchronizerComponentTypes) { componentType =>
        // each per-synchronizer component type appears once per synchronizer, indented under it
        rendered.linesIterator.count(
          _.startsWith(s"\t\t\t$componentType : ")
        ) shouldBe 2
        // and never as a top-level component line
        rendered.linesIterator.count(_.startsWith(s"\t$componentType : ")) shouldBe 0
      }

      // node-level components keep the ungrouped format
      rendered should include("Components:")
      rendered should include regex """ledger api indexer : Ok"""
    }
  }

  "report a failed sequencer-client for an unhealthy synchronizer while others stay healthy" in {
    implicit env =>
      import env.*

      loggerFactory.suppressWarningsAndErrors {
        sequencer2.stop()

        eventually() {
          val components = labeledComponents(participantStatus)
          val sequencerClientEntries = components(SequencerClient.healthName)

          sequencerClientEntries(acmeId.toProtoPrimitive) shouldBe a[ComponentHealthState.Failed]
          sequencerClientEntries(daId.toProtoPrimitive) shouldBe a[ComponentHealthState.Ok]

          forEvery(perSynchronizerComponentTypes) { componentType =>
            components(componentType)(daId.toProtoPrimitive) shouldBe a[ComponentHealthState.Ok]
          }
        }

        participant1.synchronizers.disconnect_local(acmeName)

        eventually() {
          val status = participantStatus
          status.connectedSynchronizers.keys.toSeq shouldBe Seq(daId)

          val components = labeledComponents(status)
          forEvery(perSynchronizerComponentTypes) { componentType =>
            components(componentType).keySet shouldBe Set(daId.toProtoPrimitive)
            components(componentType)(daId.toProtoPrimitive) shouldBe a[ComponentHealthState.Ok]
          }
        }
      }
  }

  "remove the synchronizer's entries after disconnect" in { implicit env =>
    import env.*

    participant1.synchronizers.disconnect_local(daName)

    eventually() {
      val status = participantStatus
      status.connectedSynchronizers shouldBe empty
      labeledComponents(status) shouldBe empty
    }
  }
}

final class SynchronizerHealthComponentsIntegrationTestDefault
    extends SynchronizerHealthComponentsIntegrationTest

final class SynchronizerHealthComponentsIntegrationTestPostgres
    extends SynchronizerHealthComponentsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
