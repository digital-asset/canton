// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Entry,
  Event,
  Hosting,
  Request,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.TopologyManagerError.{
  InvalidTopologyMapping,
  NoAppropriateSigningKeyInStore,
}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.{Namespace, Party, PhysicalSynchronizerId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.canton.{BaseTest, config}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.LoneElement.*
import org.scalatest.matchers.should.Matchers.*

import scala.jdk.CollectionConverters.*

/** The environment and the helpers of the party freeze suites `PartyFreeze*IntegrationTest`, which
  * check HyPR (Hybrid Party Replication) end to end on the topology and protocol side.
  *
  * In each case, a party hosted on SP nominates targets. Each target freezes the party, imports its
  * ACS, and becomes Onboarding and then Active. While any entry is Freezing, the party is frozen:
  * it has no hosts, so no request with the party as a stakeholder can be committed. After every
  * step, [[PartyFreezeScenario.expectStep]] checks on every participant the hosting states in the
  * mapping, the hosting views with the effective confirmation threshold, the exact Ledger API
  * topology events of the party, and whether a request that needs the party's confirmation is
  * approved and by which hosts.
  *
  * Setup:
  *   - 4 participants, 2 synchronizers `da` and `acme`, with a simulated clock: SP (participant1),
  *     DA (participant2), MPCH (participant3), and CP (participant4), which hosts Bob.
  *   - Each case uses its own party, hosted on SP only, with Submission permission and threshold 1.
  *     Bob is the actor of a nonconsuming choice on a contract with the party as signatory. Bob's
  *     exercise of it is the request that needs the party's confirmation.
  *   - The party signs nominations and threshold changes alone; each participant signs its own
  *     forward moves alone. Topology validation checks only who signed and that no entry moves
  *     backward, not the order of the forward moves: a participant may skip states, e.g. move from
  *     Nominated straight to Onboarding without a freeze. The admin endpoints of the replication
  *     enforce the order; the cases write the mapping directly.
  *   - The remote ACS read and the import of its snapshot are simulated with the repair service: SP
  *     exports the party's ACS as of t_F + 1µs, where t_F is the effective time of the target's
  *     freeze, and the disconnected target imports it in Validation mode.
  *
  * Registers the plugins of `test` on construction.
  */
private[hypr] final class PartyFreezeIntegrationTestSetup(
    test: CommunityIntegrationTest with SharedEnvironment with HasProgrammableSequencer
) {

  test.registerPlugin(new UsePostgres(test.loggerFactory))
  test.registerPlugin(
    new UseBftSequencer(
      test.loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  test.registerPlugin(new UseProgrammableSequencer(test.getClass.toString, test.loggerFactory))

  // Short timeouts keep the decision deadline, and thus the wait before activation, short
  private val confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(10)
  private val mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(10)

  def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
        ConfigTransforms.disableAutomaticAssignment,
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.synchronizers.connect_local(sequencer2, alias = acmeName)
          participant.dars.upload(BaseTest.CantonTestsPath, synchronizerId = daId)
          participant.dars.upload(BaseTest.CantonTestsPath, synchronizerId = acmeId)
        }
        Seq(sequencer1 -> daId, sequencer2 -> acmeId).foreach { case (sequencer, synchronizerId) =>
          sequencer.topology.synchronizer_parameters.propose_update(
            synchronizerId.logical,
            _.update(
              confirmationResponseTimeout = confirmationResponseTimeout,
              mediatorReactionTimeout = mediatorReactionTimeout,
            ),
          )
        }

        spRef = participant1
        daRef = participant2
        mpchRef = participant3
        cpRef = participant4
        bobRef = cp.parties.enable("Bob", synchronizer = Some(daName))
        cp.parties.enable("Bob", synchronizer = Some(acmeName)).discard
      }

  private var spRef: LocalParticipantReference = _
  private var daRef: LocalParticipantReference = _
  private var mpchRef: LocalParticipantReference = _
  private var cpRef: LocalParticipantReference = _
  private var bobRef: Party = _

  def sp: LocalParticipantReference = spRef
  def da: LocalParticipantReference = daRef
  def mpch: LocalParticipantReference = mpchRef
  def cp: LocalParticipantReference = cpRef
  def bob: Party = bobRef

  /** Allocates a party on SP, on `da` and, if `alsoOnAcme`, on `acme`, and starts a scenario for it
    * on `da`.
    */
  def newScenario(
      testCase: String,
      alsoOnAcme: Boolean = false,
      namespace: Option[Namespace] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): PartyFreezeScenario = {
    import env.*
    val party = sp.parties.enable(
      s"Party-$testCase",
      namespace = namespace.getOrElse(sp.namespace),
      synchronizer = Some(daName),
    )
    if (alsoOnAcme) sp.parties.enable(s"Party-$testCase", synchronizer = Some(acmeName)).discard
    startScenario(party, PartyFreezeScenario.PartySigner.Local(sp))
  }

  /** Allocates an external party on SP and `da`, with Confirmation permission and its namespace key
    * held outside of the participants, and starts a scenario for it on `da`.
    */
  def newExternalScenario(testCase: String)(implicit
      env: TestConsoleEnvironment,
      hashingSchemeVersion: HashingSchemeVersion,
  ): PartyFreezeScenario = {
    import env.*
    val party = sp.parties.testing.external.enable(s"Party-$testCase", synchronizer = Some(daName))
    startScenario(
      party,
      PartyFreezeScenario.PartySigner
        .External(NonEmpty.mk(Seq, party.partyId.namespace.fingerprint)),
    )
  }

  private def startScenario(
      party: Party,
      partySigner: PartyFreezeScenario.PartySigner,
  )(implicit env: TestConsoleEnvironment): PartyFreezeScenario = {
    import env.*
    PartyFreezeScenario(
      party,
      source = sp,
      partySigner = partySigner,
      counterparty = bob,
      counterpartyParticipant = cp,
      participants = Seq(sp, da, mpch, cp),
      synchronizerId = daId,
      sequencer = test.getProgrammableSequencer(sequencer1.name),
      loggerFactory = test.loggerFactory,
    )
  }

  /** Creates a contract that `submitter` submits on `participant`. */
  def createContract(
      participant: LocalParticipantReference,
      submitter: Party,
      synchronizerId: PhysicalSynchronizerId,
      signatories: Seq[Party],
      observers: Seq[Party] = Seq.empty,
      actors: Seq[Party] = Seq.empty,
  ): UniversalContract.ContractId = {
    val transaction = participant.ledger_api.javaapi.commands.submit(
      Seq(submitter),
      universalContract(signatories, observers, actors).create.commands.asScala.toSeq,
      synchronizerId = Some(synchronizerId.logical),
    )
    JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(transaction).loneElement.id
  }

  def universalContract(
      signatories: Seq[Party],
      observers: Seq[Party],
      actors: Seq[Party],
  ): UniversalContract =
    new UniversalContract(
      List.empty[String].asJava,
      signatories.map(_.toProtoPrimitive).asJava,
      observers.map(_.toProtoPrimitive).asJava,
      actors.map(_.toProtoPrimitive).asJava,
    )

  def touch(
      participant: LocalParticipantReference,
      submitter: Party,
      synchronizerId: PhysicalSynchronizerId,
      contractId: UniversalContract.ContractId,
      choiceObservers: Seq[Party] = Seq.empty,
  ): Unit =
    participant.ledger_api.javaapi.commands
      .submit(
        Seq(submitter),
        contractId
          .exerciseTouch(choiceObservers.map(_.toProtoPrimitive).asJava)
          .commands
          .asScala
          .toSeq,
        synchronizerId = Some(synchronizerId.logical),
      )
      .discard

  def unassign(
      party: Party,
      contractId: UniversalContract.ContractId,
      source: PhysicalSynchronizerId,
      target: PhysicalSynchronizerId,
  ): String =
    sp.ledger_api.commands
      .submit_unassign(
        party,
        Seq(LfContractId.assertFromString(contractId.contractId)),
        source.logical,
        target.logical,
      )
      .reassignmentId

  def expectRejected(code: ErrorCode, cause: String = "")(submit: => Unit): Unit =
    test.loggerFactory.assertThrowsAndLogs[CommandFailure](
      submit,
      entry => {
        entry.shouldBeCantonErrorCode(code)
        entry.errorMessage should include(cause)
      },
    )

  /** Expects the party to be frozen, with SP as its only host otherwise. */
  def expectFrozenWithHostSp(
      scenario: PartyFreezeScenario,
      threshold: PositiveInt = PositiveInt.one,
  ): Unit =
    scenario.expectStep(
      Hosting.Frozen(threshold, Set(sp.id)),
      events = Set.empty,
      Request.Rejected,
    )

  /** Expects a backward move of `participant`'s entry to `state` to be rejected: signed by the
    * participant alone, it lacks the party's signature; signed by the party, it is invalid.
    */
  def expectBackwardMoveRejected(
      scenario: PartyFreezeScenario,
      participant: LocalParticipantReference,
      state: HostingState,
  ): Unit = {
    expectRejected(NoAppropriateSigningKeyInStore)(scenario.move(participant, state).discard)
    expectRejected(InvalidTopologyMapping, "Invalid hosting state transitions")(
      scenario.moveSignedByParty(participant, state)
    )
  }

  /** Expects the pending operation of `target`'s import to move it from Freezing to Onboarding, and
    * the onboarding clearance it schedules to move it to Active once the decision deadline has
    * passed.
    */
  def expectOnboardedAndActivated(
      scenario: PartyFreezeScenario,
      target: LocalParticipantReference,
  ): Unit = {
    val onboardedAt = scenario.expectWritten(
      scenario.entries.updated(target.id, Entry(Confirmation, HostingState.Onboarding)),
      PositiveInt.one,
      signers = Seq(target),
    )
    scenario.expectStep(
      Hosting.Hosted(PositiveInt.one, Set(sp.id, target.id)),
      events = Set(Event.Onboarding(target.id)),
      Request.ApprovedBy(Set(sp.id)),
    )
    scenario.advancePastDecisionDeadline(onboardedAt)
    scenario.expectWritten(
      scenario.entries.updated(target.id, Entry(Confirmation, HostingState.Active)),
      PositiveInt.one,
      signers = Seq(target),
    )
    scenario.expectStep(
      Hosting.Hosted(PositiveInt.one, Set(sp.id, target.id)),
      events = Set(Event.Added(target.id)),
      Request.ApprovedBy(Set(sp.id, target.id)),
    )
    scenario.assertCommitmentsMatchAmongHosts()
  }

  /** Expects SP to be the only host of the party, with nothing else changed by the step. */
  def expectHostedBySp(scenario: PartyFreezeScenario): Unit =
    scenario.expectStep(
      Hosting.Hosted(PositiveInt.one, Set(sp.id)),
      events = Set.empty,
      Request.ApprovedBy(Set(sp.id)),
    )
}
