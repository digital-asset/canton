// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.pkgdars

import com.daml.ledger.api.v2.commands.Command
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.PackageSelectionFailed
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.validation.TransactionConfirmationResponsesFactory
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.tests.vettingmain
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, Party, PartyId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser}
import org.slf4j.event.Level

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.chaining.scalaUtilChainingOps

class ProtocolVettingChecksIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with AccessTestScenario {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile private var alice, bob: Party = _
  @volatile private var maliciousP1: MaliciousParticipantNode = _

  val ledgerIntegrity: SecurityTest =
    SecurityTest(property = Integrity, asset = "virtual shared ledger")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))
        alice = participant1.parties.testing.enable("Alice")
        bob = participant2.parties.testing.enable("Bob")

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  private def createMainCommand(party: PartyId, depParty: Option[PartyId] = None) =
    new vettingmain.v1.java.main.MainT(
      party.toProtoPrimitive,
      new vettingmain.v1.java.dep.DepT(
        depParty.map(_.toProtoPrimitive).getOrElse(party.toProtoPrimitive)
      ),
    )
      .create()
      .commands()
      .asScala
      .toSeq

  "A submitting participant" when {
    s"connected to a PV ${ProtocolVersion.v34} or lower synchronizer" should {
      "NOT be able to submit a command with a vetted package that has an unvetted dependency from which no template nor interface is used in the transaction" taggedAs_ (ledgerIntegrity
        .setHappyCase(_)) onlyRunWithOrLessThan ProtocolVersion.v34 in { implicit env =>
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
            logEntry => {
              logEntry.shouldBeCantonErrorCode(PackageSelectionFailed)
              logEntry.commandFailureMessage should include(
                "No synchronizers satisfy the topology requirements for the submitted command"
              )
            },
          )
        }
      }
    }

    s"connected to a PV ${ProtocolVersion.v35} or higher synchronizer" should {
      "be able to submit a command with a vetted package that has an unvetted dependency that doesn't appear in a transaction action node" taggedAs_ (ledgerIntegrity
        .setHappyCase(_)) onlyRunWithOrGreaterThan ProtocolVersion.v35 in { implicit env =>
        import env.*

        // Setup vetted Main with unvetted Dep dependency
        participant1.dars.upload(VettingMainPath)
        unvet(participant1)(removes = Seq(tryReadDar(VettingDepPath).main))

        // Creating a MainT contract should be possible as the unvetted dependency (Dep) is not used in the transaction
        participant1.ledger_api.javaapi.commands.submit(Seq(alice), createMainCommand(alice))
      }
    }

    "connected to any synchronizer" should {
      "reject a command submission if the resulting transaction uses an unvetted dependency of a package in a transaction action node" taggedAs ledgerIntegrity
        .setAttack(
          Attack(
            actor = "ledger api user",
            threat =
              "submit a command yielding a transaction that references an unvetted package in an action node",
            mitigation = "submitting participant rejects the command",
          )
        ) in { implicit env =>
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
        unvet(participant2)(removes = Seq(tryReadDar(VettingDepPath).main))

        // Try to exercise createDep, which should fail as the resulting transaction would use the unvetted dependency in an action node
        assertThrowsAndLogsCommandFailures(
          participant2.ledger_api.javaapi.commands.submit(
            Seq(bob),
            mainContractId.exerciseMainT_createDep().commands().asScala.toList,
          ),
          errLog => {
            errLog.shouldBeCommandFailure(PackageSelectionFailed)
            errLog.message should include regex s"Failed to select package-id for package-name '${vettingmain.v1.java.main.MainT.PACKAGE_NAME}' appearing in a command root node due to: Packages with required dependencies not vetted by all interested parties.*"
          },
        )
      }
    }
  }

  "A confirming participant" when {
    "receives a confirmation request referencing a package with an unvetted dependency but the unvetted dependency doesn't appear in a transaction action node" should {
      s"reject the transaction if connected to a PV ${ProtocolVersion.v34} synchronizer" taggedAs ledgerIntegrity
        .setAttack(
          Attack(
            actor = "malicious submitting participant",
            threat =
              "submits a command yielding a transaction that references an unvetted package that does not appear in an action node (in PV34 or lower)",
            mitigation = "confirming participant rejects the command",
          )
        ) onlyRunWithOrLessThan ProtocolVersion.v34 in { implicit env =>
        import env.*

        // Setup vetted Main with unvetted Dep dependency for participant2
        participant2.dars.upload(VettingMainPath)
        unvet(participant2)(removes = Seq(tryReadDar(VettingDepPath).main))

        val createForAliceAndBob = createMainCommand(alice, Some(bob)).loneElement

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          maliciousP1
            .submitCommand(
              CommandsWithMetadata(
                Seq(Command.fromJavaProto(createForAliceAndBob.toProtoCommand)),
                Seq(alice),
              )
            )
            .futureValueUS
            .value,
          expectModelConformanceRejectLogs(participant1, participant2),
        )
      }
    }

    "receives a confirmation request referencing a package with an unvetted dependency that appears in a transaction action node" should {
      "reject the transaction on any protocol version" taggedAs ledgerIntegrity.setAttack(
        Attack(
          actor = "malicious submitting participant",
          threat =
            "submits a command yielding a transaction that references an unvetted package in an action node",
          mitigation = "confirming participant rejects the command",
        )
      ) in { implicit env =>
        import env.*

        // Upload and vet the main DAR to ensure create is accepted on both PV 34 and 35
        participant1.dars.upload(VettingMainPath)
        participant2.dars.upload(VettingMainPath)

        // Create MainT contract
        val mainContractId = participant1.ledger_api.javaapi.commands
          .submit(Seq(alice), createMainCommand(alice, Some(bob)))
          .getEvents
          .asScalaProtoCreatedContracts
          .loneElement
          .contractId
          .pipe(new vettingmain.v1.java.main.MainT.ContractId(_))

        // Unvet DepT's package for participant2
        unvet(participant2)(removes = Seq(tryReadDar(VettingDepPath).main))

        val exerciseCommand = CommandsWithMetadata(
          Seq(
            Command.fromJavaProto(
              mainContractId
                .exerciseMainT_createDep()
                .commands()
                .asScala
                .toList
                .loneElement
                .toProtoCommand
            )
          ),
          Seq(alice),
        )

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          maliciousP1.submitCommand(exerciseCommand).futureValueUS.value,
          expectModelConformanceRejectLogs(participant1, participant2),
        )
      }
    }

    def expectModelConformanceRejectLogs(
        participant1: => LocalParticipantReference,
        participant2: => LocalParticipantReference,
    ) =
      LogEntry.assertLogSeq({
        val testClassName = ProtocolVettingChecksIntegrationTest.this.getClass.getSimpleName
        val classLoggerName = classOf[TransactionConfirmationResponsesFactory].getSimpleName
        Seq(
          s"$classLoggerName:$testClassName/participant=${participant1.id.identifier}",
          s"$classLoggerName:$testClassName/participant=${participant2.id.identifier}",
        ).map { ln =>
          (
            e => {
              e.loggerName should include regex ln
              e.level shouldBe Level.WARN
              e.message should include regex "LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check.*UnvettedPackages"
            },
            s"Didn't find logger: $ln",
          )
        }
      })
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
