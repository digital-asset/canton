// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.pkgdars

import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.tests.vettingmain
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, Party, PartyId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser}
import monocle.Monocle.toAppliedFocusOps

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.chaining.scalaUtilChainingOps

class ProtocolVettingChecksIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile private var alice, bob: Party = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          // TODO(#29834): TAPS does not yet support package dependency unvetting so disable it to
          //               showcase the new behavior at the protocol level.
          //               Implement TAPS support and remove this config
          _.focus(_.ledgerApi.topologyAwarePackageSelection.enabled).replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))
        alice = participant1.parties.testing.enable("Alice")
        bob = participant2.parties.testing.enable("Bob")
      }

  private def createMainCommand(party: PartyId) = new vettingmain.v1.java.main.MainT(
    party.toProtoPrimitive,
    new vettingmain.v1.java.dep.DepT(party.toProtoPrimitive),
  )
    .create()
    .commands()
    .asScala
    .toSeq

  "A submitting participant" when {
    s"connected to a PV ${ProtocolVersion.v34} or lower synchronizer" should {
      "NOT be able to submit a command with a vetted package that has an unvetted dependency from which no template nor interface is used in the transaction" onlyRunWithOrLessThan ProtocolVersion.v34 in {
        implicit env =>
          import env.*

          // Main depends on Dep
          participant1.dars.upload(VettingMainPath)
          // Unvet Dep main package
          unvet(participant1)(removes = Seq(tryReadDar(VettingDepPath).main))

          clue(
            "Creating a MainT contract should not be possible even if the unvetted dependency (Dep) is not used in the transaction"
          ) {
            assertThrowsAndLogsCommandFailures(
              participant1.ledger_api.javaapi.commands.submit(Seq(alice), createMainCommand(alice)),
              _.commandFailureMessage should include("No valid synchronizer for submission found"),
            )
          }
      }
    }

    s"connected to a PV ${ProtocolVersion.v35} or higher synchronizer" should {
      "be able to submit a command with a vetted package that has an unvetted dependency that doesn't appear in a transaction action node" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
        implicit env =>
          import env.*

          // Setup vetted Main with unvetted Dep dependency
          participant1.dars.upload(VettingMainPath)
          unvet(participant1)(removes = Seq(tryReadDar(VettingDepPath).main))

          // Creating a MainT contract should be possible as the unvetted dependency (Dep) is not used in the transaction
          participant1.ledger_api.javaapi.commands.submit(Seq(alice), createMainCommand(alice))
      }
    }

    "connected to any synchronizer" should {
      "reject a command submission if the resulting transaction uses an unvetted dependency of a package in a transaction action node" in {
        implicit env =>
          import env.*

          // Vet a new main DAR to ensure create is accepted on both PV 34 and 35
          participant2.dars.upload(VettingMainPath)

          // Create MainT contract
          val mainContractId = participant2.ledger_api.javaapi.commands
            .submit(Seq(bob), createMainCommand(bob))
            .getEvents
            .asScalaProtoCreatedContracts
            .loneElement
            .contractId
            .pipe(new vettingmain.v1.java.main.MainT.ContractId(_))

          // Unvet DepT's package
          unvet(participant2)(
            removes = Seq(tryReadDar(VettingDepPath).main)
          )

          // Try to exercise createDep, which should fail as the resulting transaction would use the unvetted dependency in an action node
          assertThrowsAndLogsCommandFailures(
            participant2.ledger_api.javaapi.commands.submit(
              Seq(bob),
              mainContractId.exerciseMainT_createDep().commands().asScala.toList,
            ),
            _.commandFailureMessage should (include(
              "INVALID_PRESCRIBED_SYNCHRONIZER_ID"
            ) and include("Some packages are not known to all informees")),
          )
      }
    }

    // TODO(#29834): Add malicious participant tests to assert phase 3 rejections
  }

  private def tryReadDar(darPath: String) =
    DarParser
      .readArchiveFromFile(new java.io.File(darPath))
      .getOrElse(fail(s"cannot read DAR: $darPath"))

  private def unvet(participant: ParticipantReference)(
      removes: Seq[DamlLf.Archive]
  )(implicit
      env: TestConsoleEnvironment
  ): Unit =
    participant.topology.vetted_packages.propose_delta(
      participant.id,
      store = env.daId,
      removes = removes.map(DamlPackageStore.readPackageId),
      // TODO(#29834): Remove force flag once vetting state change behavior is adapted to support unvetted dependencies in PV35
      force = ForceFlags(ForceFlag.AllowUnvettedDependencies),
    )
}
