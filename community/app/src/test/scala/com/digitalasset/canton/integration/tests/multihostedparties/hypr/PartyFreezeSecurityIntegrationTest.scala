// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  AcsSnapshot,
  Event,
  Hosting,
  PartySigner,
  Request,
  Signer,
}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.assertCommitmentsMismatch
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray}
import com.digitalasset.daml.lf.transaction.{ContractInstanceCoder, FatContractInstance}
import com.digitalasset.daml.lf.value.Value

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

/** Test scenario: the target of HyPR (Hybrid Party Replication) imports an ACS snapshot that
  * differs from the party's ACS as of t_F + 1µs.
  *
  * Test goal (security test evidence):
  *   - SEC1: the import in Validation mode rejects a snapshot in which a contract's contents were
  *     altered, as the contract ID no longer authenticates the contents.
  *   - SEC2: a snapshot from which a contract was dropped, or to which an archived contract was
  *     added, leads to an ACS commitment mismatch after the target's activation, with a
  *     counterparticipant that shares another contract with the target.
  *
  * Test setup:
  *   - 4 participants, 1 synchronizer, with a simulated clock: SP (participant1), DA
  *     (participant2), Beta (participant3), and CP (participant4), which hosts Bob. The two SEC2
  *     cases use different targets, as a mismatch persists.
  *   - Each case uses its own party, hosted on SP only, with contracts shared with Bob.
  *   - The remote ACS read and the import of its snapshot are simulated with the repair service.
  */
final class PartyFreezeSecurityIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        participants.local.dars.upload(CantonTestsPath)
        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(10),
            mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(10),
          ),
        )

        sp = participant1
        da = participant2
        beta = participant3
        cp = participant4
        bob = cp.parties.enable("Bob")
      }

  private var sp: LocalParticipantReference = _
  private var da: LocalParticipantReference = _
  private var beta: LocalParticipantReference = _
  private var cp: LocalParticipantReference = _
  private var bob: Party = _

  private def newScenario(testCase: String)(implicit
      env: TestConsoleEnvironment
  ): PartyFreezeScenario = {
    import env.*
    PartyFreezeScenario(
      sp.parties.enable(s"Party-$testCase"),
      source = sp,
      partySigner = PartySigner.Local(sp),
      counterparty = bob,
      counterpartyParticipant = cp,
      participants = Seq(sp, da, beta, cp),
      synchronizerId = daId,
      sequencer = getProgrammableSequencer(sequencer1.name),
      loggerFactory = loggerFactory,
    )
  }

  /** Creates a contract of the party, shared with Bob. */
  private def createSharedContract(scenario: PartyFreezeScenario): UniversalContract.ContractId = {
    val transaction = sp.ledger_api.javaapi.commands.submit(
      Seq(scenario.party),
      new UniversalContract(
        List.empty[String].asJava,
        List(scenario.party.toProtoPrimitive).asJava,
        List(bob.toProtoPrimitive).asJava,
        List(scenario.party.toProtoPrimitive).asJava,
      ).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(transaction).loneElement.id
  }

  /** Writes the contracts of `snapshot`, transformed by `change`, into a new snapshot. */
  private def changeSnapshot(snapshot: AcsSnapshot)(
      change: Seq[LapiActiveContract] => Seq[LapiActiveContract]
  )(implicit env: TestConsoleEnvironment): AcsSnapshot = {
    import env.*
    val contracts = change(repair.acs.read_from_file(snapshot.path))
    val file = Files.createTempFile("party-freeze-changed-acs-snapshot", ".gz")
    file.toFile.deleteOnExit()
    repair.acs.write_contracts_to_file(contracts, daId.protocolVersion, file.toString)
    AcsSnapshot(file.toString, contracts.map(_.getCreatedEvent.contractId).toSet)
  }

  /** Removes the observers from the create argument of `contract`, keeping its contract ID. */
  private def withoutObservers(contract: LapiActiveContract): LapiActiveContract = {
    val fatContract = ContractInstanceCoder
      .decodeFatContractInstance(contract.getCreatedEvent.createdEventBlob)
      .value
    val createNode = fatContract.toCreateNode
    val alteredArgument = createNode.arg match {
      // The fields of UniversalContract: maintainers, signatories, observers, actors
      case Value.ValueRecord(templateId, fields) =>
        Value.ValueRecord(
          templateId,
          ImmArray.from(fields.toSeq.zipWithIndex.map {
            case ((label, _), 2) => label -> Value.ValueList(FrontStack.empty)
            case (field, _) => field
          }),
        )
      case other => fail(s"Unexpected create argument $other")
    }
    val alteredContract = FatContractInstance.fromCreateNode(
      createNode.copy(arg = alteredArgument),
      fatContract.createdAt,
      fatContract.authenticationData,
    )
    contract.update(
      _.createdEvent.createdEventBlob.set(
        ContractInstanceCoder.encodeFatContractInstance(alteredContract).value
      )
    )
  }

  private def expectFrozen(scenario: PartyFreezeScenario): Unit =
    scenario.expectStep(
      Hosting.Frozen(PositiveInt.one, Set(sp.id)),
      events = Set.empty,
      Request.Rejected,
    )

  /** Moves `target`, which has imported a snapshot, to Onboarding and to Active. */
  private def onboardAndActivate(
      scenario: PartyFreezeScenario,
      target: LocalParticipantReference,
  ): Unit = {
    val beforeOnboarding = scenario.onboard(target)
    scenario.expectStep(
      Hosting.Hosted(PositiveInt.one, Set(sp.id, target.id)),
      events = Set(Event.Onboarding(target.id)),
      Request.ApprovedBy(Set(sp.id)),
    )
    scenario.activate(target, beforeOnboarding)
  }

  "The import of an ACS snapshot" should {
    "SEC1: reject a snapshot with altered contract contents in Validation mode" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("SEC1")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        val frozenAt = scenario.freeze(da)
        expectFrozen(scenario)

        val altered = changeSnapshot(scenario.readAcs(frozenAt))(_.map(withoutObservers))
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          scenario.importAcs(da, altered),
          entry => {
            entry.shouldBeCantonErrorCode(ImportAcsError)
            entry.errorMessage should include("Failed to authenticate contract")
          },
        )
        da.testing.acs_search(env.daName, filterStakeholder = Some(scenario.party.partyId)) shouldBe
          empty

        scenario.remove(da, by = Signer.Participant)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

  "A snapshot that differs from the party's ACS" should {
    "SEC2: cause an ACS commitment mismatch after activation when a contract was dropped" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("SEC2-dropped")
        val dropped = createSharedContract(scenario)
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        val frozenAt = scenario.freeze(da)
        expectFrozen(scenario)

        val incomplete = changeSnapshot(scenario.readAcs(frozenAt))(
          _.filterNot(_.getCreatedEvent.contractId == dropped.contractId)
        )
        scenario.importAcs(da, incomplete)
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            onboardAndActivate(scenario, da)
            // CP shares the counterparty's contract with DA, but also the dropped one
            assertCommitmentsMismatch(da, cp, env.daId, since = scenario.latestChangeTime)
          },
          forAll(_)(_.warningMessage should include("digest")),
        )
    }

    "SEC2: cause an ACS commitment mismatch after activation when an archived contract was added" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        val scenario = newScenario("SEC2-added")
        val archived = createSharedContract(scenario)
        val withArchived = scenario.readAcs(sp.testing.fetch_synchronizer_time(daId))
        sp.ledger_api.javaapi.commands
          .submit(Seq(scenario.party), archived.exerciseArchive().commands.asScala.toSeq)
          .discard
        scenario.nominate(Seq(beta -> Confirmation), PositiveInt.one)
        val frozenAt = scenario.freeze(beta)
        expectFrozen(scenario)

        val archivedContract = repair.acs
          .read_from_file(withArchived.path)
          .filter(_.getCreatedEvent.contractId == archived.contractId)
        val extended = changeSnapshot(scenario.readAcs(frozenAt))(_ ++ archivedContract)
        scenario.importAcs(beta, extended)
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            onboardAndActivate(scenario, beta)
            assertCommitmentsMismatch(beta, cp, env.daId, since = scenario.latestChangeTime)
          },
          forAll(_)(_.warningMessage should include("digest")),
        )
    }
  }
}
