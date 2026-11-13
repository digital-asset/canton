// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.digitalasset.canton.config.{DbConfig, StorageConfig, TopologyConfig}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClient,
  ValidatingTopologySnapshot,
}
import io.circe.Json

import java.nio.file.{Files, Paths}
import scala.concurrent.duration.*

/** Selected explicitly by the draft's reusable-workflow proof calls, not by ordinary test scans. */
final class TopologyConsistencyIntegrationTestPostgres
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P2_S1M1

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "The running Canton nodes" should {
    "use the expected topology validator and complete a ping" in { implicit env =>
      import env.*

      // The caller supplies this expectation independently of the production nightly variable.
      val expected = sys.env.get("CANTON_TOPOLOGY_VALIDATION_EXPECTED") match {
        case Some("enabled") => true
        case Some("disabled") => false
        case other => fail(s"Expected an explicit enabled/disabled proof expectation, got $other")
      }
      sys.env.get("CI") shouldBe Some("true")
      sys.env.get("CANTON_NIGHTLY_TEST_RUN") shouldBe Some(expected.toString)
      actualConfig.parameters.enableAdditionalConsistencyChecks shouldBe true

      val performanceConfig = EnvironmentDefinition.P2_S1M1
        .addConfigTransform(ConfigTransforms.disableAdditionalConsistencyChecks)
        .generateConfig
      performanceConfig.parameters.enableAdditionalConsistencyChecks shouldBe false
      val performanceTopologies = performanceConfig.participants.values.map(_.topology) ++
        performanceConfig.sequencers.values.map(_.topology) ++
        performanceConfig.mediators.values.map(_.topology)
      forAll(performanceTopologies)(
        _.shouldCheckTopologySnapshots(enableAdditionalConsistencyChecks = false) shouldBe false
      )

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant2.synchronizers.connect_local(sequencer1, daName)
      participant1.health.maybe_ping(participant2, timeout = 30.seconds) shouldBe defined

      val nodes: Seq[(String, TopologyConfig, StorageConfig, SynchronizerTopologyClient, Member)] =
        Seq(
          (
            "participant1",
            participant1.underlying.value.config.topology,
            participant1.underlying.value.config.storage,
            participant1.underlying.value.sync.syncCrypto.ips.tryForSynchronizer(daId),
            participant1.id,
          ),
          (
            "participant2",
            participant2.underlying.value.config.topology,
            participant2.underlying.value.config.storage,
            participant2.underlying.value.sync.syncCrypto.ips.tryForSynchronizer(daId),
            participant2.id,
          ),
          (
            "sequencer1",
            actualConfig.sequencersByString(sequencer1.name).topology,
            actualConfig.sequencersByString(sequencer1.name).storage,
            sequencer1.underlying.value.sequencer.syncCrypto.ips,
            sequencer1.id,
          ),
          (
            "mediator1",
            actualConfig.mediatorsByString(mediator1.name).topology,
            actualConfig.mediatorsByString(mediator1.name).storage,
            mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.topologyClient,
            mediator1.id,
          ),
        )

      val observed = nodes.map { case (name, topology, storage, client, member) =>
        withClue(s"$name: ") {
          storage shouldBe a[DbConfig.Postgres]
          topology.enableTopologySnapshotConsistencyChecks shouldBe
            (if (expected) None else Some(false))
          // Using the topology-change timestamp avoids a forwarding wrapper hiding the validator.
          // This is a fresh snapshot from the real node's client, cache, and topology store.
          val snapshot = client.snapshot(client.latestTopologyChangeTimestamp).futureValueUS
          val validating = snapshot.isInstanceOf[ValidatingTopologySnapshot]
          validating shouldBe expected
          snapshot.signingKeys(member, SigningKeyUsage.All).futureValueUS should not be empty
          Json.obj(
            "node" -> Json.fromString(name),
            "config_override" -> topology.enableTopologySnapshotConsistencyChecks
              .map(Json.fromBoolean)
              .getOrElse(Json.Null),
            "validator" -> Json.fromBoolean(validating),
            "snapshot_class" -> Json.fromString(snapshot.getClass.getSimpleName),
            "storage_class" -> Json.fromString(storage.getClass.getSimpleName),
            "lookup_succeeded" -> Json.True,
          )
        }
      }

      val evidence = Json.obj(
        "suite" -> Json.fromString(getClass.getName),
        "commit" -> Json.fromString(sys.env("GITHUB_SHA")),
        "run_id" -> Json.fromString(sys.env("GITHUB_RUN_ID")),
        "run_attempt" -> Json.fromString(sys.env("GITHUB_RUN_ATTEMPT")),
        "expected_validation" -> Json.fromBoolean(expected),
        "nightly_marker" -> Json.fromString(sys.env("CANTON_NIGHTLY_TEST_RUN")),
        "general_checks_enabled" -> Json.fromBoolean(
          actualConfig.parameters.enableAdditionalConsistencyChecks
        ),
        "ping_succeeded" -> Json.True,
        "performance_opt_out_preserved" -> Json.True,
        "nodes" -> Json.fromValues(observed),
      )
      val path = Paths.get(sys.env("CANTON_TOPOLOGY_PROOF_EVIDENCE"))
      Option(path.getParent).foreach { parent =>
        val _ = Files.createDirectories(parent)
      }
      val _ = Files.writeString(path, evidence.spaces2)
    }
  }
}
