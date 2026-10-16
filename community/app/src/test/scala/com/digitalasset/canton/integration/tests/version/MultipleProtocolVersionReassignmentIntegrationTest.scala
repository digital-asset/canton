// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
  MultiSynchronizerFeatureFlag,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import monocle.macros.syntax.lens.*

/** We test reassignments between synchronizers running different protocol versions.
  *
  * The topology used:
  *   - S1 (da) and S2 (acme) run the two latest stable protocol versions, or both the same one when
  *     there is only one stable protocol version
  *   - S3 (repair) runs the first protocol version after them (for example v35, v36 and v37), or
  *     dev when the tested protocol version is dev
  *   - P1 is connected to S1, S2 and S3
  *
  * A contract is reassigned between every ordered pair of synchronizers. The multi-synchronizer
  * feature flag is enabled on all synchronizers.
  */
sealed trait MultipleProtocolVersionReassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  // The two latest stable protocol versions, equal when there is only one
  private lazy val (beforeLastStable, lastStable) = {
    val lastTwoStables = ProtocolVersion.stable.sorted.takeRight(2)
    if (lastTwoStables.sizeIs == 2)
      (lastTwoStables(0), lastTwoStables(1))
    else (lastTwoStables(0), lastTwoStables(0))
  }

  // The first beta, alpha or dev protocol version after the latest stable one
  private lazy val nextProtocolVersion: ProtocolVersion =
    (ProtocolVersion.beta ++ ProtocolVersion.alpha :+ ProtocolVersion.dev)
      .map(pv => pv: ProtocolVersion)
      .filter(pv => pv > lastStable && pv != ProtocolVersion.almostDev)
      .min

  private lazy val s3ProtocolVersion: ProtocolVersion =
    if (testedProtocolVersion.isStable) nextProtocolVersion else testedProtocolVersion

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .P1_S1M1_S1M1_S1M1_WithStaticParameters(
        Map(
          "synchronizer1" -> StaticSynchronizerParameters.defaults(beforeLastStable),
          "synchronizer2" -> StaticSynchronizerParameters.defaults(lastStable),
          "synchronizer3" -> StaticSynchronizerParameters.defaults(s3ProtocolVersion),
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") {
          _.focus(_.parameters.minimumProtocolVersion)
            .replace(Some(ParticipantProtocolVersion(beforeLastStable)))
        },
        _.focus(_.parameters.enableAlphaStateViaConfig).replace(true),
      )
      .addConfigTransforms(ConfigTransforms.setProtocolVersion(s3ProtocolVersion)*)
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.synchronizers.connect_local(sequencer3, alias = repairSynchronizerName)
        Seq(daId, acmeId, repairSynchronizerId)
          .foreach(psid => participant1.dars.upload(CantonExamplesPath, synchronizerId = psid))

        Seq(daId, acmeId, repairSynchronizerId).foreach(psid =>
          MultiSynchronizerFeatureFlag.enable(Seq(participant1), psid.logical)
        )
      }

  private def createCycleCommand(
      id: String
  )(implicit env: TestConsoleEnvironment): Command = {
    import env.*
    val pkg = participant1.packages.find_by_module("Cycle").headOption.value
    ledger_api_utils.create(
      pkg.packageId,
      "Cycle",
      "Cycle",
      Map[String, Any]("owner" -> participant1.adminParty, "id" -> id),
    )
  }

  // Reassign the Cycle contract from `source` to `target` synchronizer
  private def reassign(
      sourceId: SynchronizerId,
      targetId: SynchronizerId,
      sourceAlias: SynchronizerAlias,
      targetAlias: SynchronizerAlias,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val party = participant1.adminParty

    // Cycle contract creation
    participant1.ledger_api.commands.submit(
      Seq(party),
      Seq(createCycleCommand("cycle-contract1")),
      Some(sourceId),
    )

    val cid1 = searchAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = sourceAlias,
      module = "Cycle",
      template = "Cycle",
    )

    def executeReassignment(): Unit =
      participant1.ledger_api.commands.submit_reassign(party, Seq(cid1), sourceId, targetId)

    executeReassignment()

    searchAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = targetAlias,
      module = "Cycle",
      template = "Cycle",
    )

    // Cycle contract archival
    val cid2 = participant1.ledger_api.javaapi.state.acs.await(M.Cycle.COMPANION)(party)
    participant1.ledger_api.javaapi.commands.submit(
      Seq(party),
      Seq(cid2.id.exerciseArchive().commands.loneElement),
      Some(targetId),
    )

    assertNotInAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = targetAlias,
      module = "Cycle",
      template = "Cycle",
    )
  }

  "A contract created on a synchronizer" should {
    // The protocol version of each synchronizer only depends on whether the tested protocol version
    // is stable, so running this test for every tested protocol version (35, 36, 37) in CI would run
    // the same test several times.
    "be reassignable to synchronizers running on different protocol versions" onlyRunWhen (
      testedProtocolVersion == ProtocolVersion.latest || testedProtocolVersion.isDev
    ) in { implicit env =>
      import env.*

      val synchronizers: List[(ProtocolVersion, SynchronizerId, SynchronizerAlias)] = List(
        (beforeLastStable, daId, daName),
        (lastStable, acmeId, acmeName),
        (s3ProtocolVersion, repairSynchronizerId, repairSynchronizerName),
      )

      // 2-permutations (`permutations` doesn't support it)
      val possibleReassignments =
        for {
          source <- synchronizers
          target <- synchronizers
          if source != target
        } yield (source, target)

      forAll(possibleReassignments) {
        case ((sourcePV, sourceId, sourceAlias), (targetPV, targetId, targetAlias)) =>
          clue(s"Reassigning from $sourceAlias ($sourcePV) to $targetAlias ($targetPV)") {
            reassign(sourceId, targetId, sourceAlias, targetAlias)
          }
      }
    }
  }
}

final class MultipleProtocolVersionReassignmentIntegrationTestPostgres
    extends MultipleProtocolVersionReassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(
        Set("sequencer1"),
        Set("sequencer2"),
        Set("sequencer3"),
      ),
    )
  )
}

//class MultipleProtocolVersionReassignmentIntegrationTestH2
//    extends MultipleProtocolVersionReassignmentIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseBftSequencer(
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3"), Set("sequencer4")),
//    )
//  )
//}
