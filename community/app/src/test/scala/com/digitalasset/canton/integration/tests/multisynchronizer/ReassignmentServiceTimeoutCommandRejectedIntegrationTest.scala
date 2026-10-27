// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.{CantonTimestamp, UnassignmentData}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.HasCommandRunnersHelpers.{
  commandId as defaultCommandId,
  userId as defaultUserId,
}
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  AutomaticAssignment,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.submission.ChangeIdHash
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.store.ReassignmentStore.UnknownReassignmentId
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.{ReassignmentId, TestSynchronizerParameters}
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfCommandId,
  ReassignmentCounter,
  SynchronizerAlias,
}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.Assertion

import scala.collection.mutable

sealed trait ReassignmentServiceTimeoutCommandRejectedIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer
    with HasExecutionContext {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransform(ConfigTransforms.disableAutomaticAssignment)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

        def disableAutomaticAssignment(d: InitializedSynchronizer): Unit =
          AutomaticAssignment.disable(d.synchronizerOwners, d.synchronizerId)

        disableAutomaticAssignment(getInitializedSynchronizer(daName))
        disableAutomaticAssignment(getInitializedSynchronizer(acmeName))

        participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = acmeId)
        programmableSequencers.put(daName, getProgrammableSequencer(sequencer1.name))
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))

      }

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

  private lazy val defaultsParameters = TestSynchronizerParameters.defaultDynamic

  private def setSequencerPolicy(synchronizerName: SynchronizerAlias)(
      delayConfirmationResponse: NonNegativeFiniteDuration,
      delayMediator: NonNegativeFiniteDuration,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val delayPolicy = ProgrammableSequencerPolicies.delay(environment = environment)(
      confirmationResponses = Map(participant1.id -> delayConfirmationResponse),
      mediatorMessages = Some(delayMediator),
    )
    val simClock = environment.simClock.value
    val sequencerTimeTracker =
      (if (synchronizerName == daName) sequencer1
       else sequencer2).underlying.value.sequencer.timeTracker

    // With the BFT sequencer, a block's time lags behind the sim clock: a message submitted right
    // after the clock jump may still land in a block stamped with the pre-jump time, in which case
    // it would not be delayed at all. So after the delay policy has advanced the clock, hold the
    // message back until the sequencer has observed a time at or after the new clock time.
    programmableSequencers(synchronizerName).setPolicy("maximum delay policy") {
      traceContext => submissionRequest =>
        val decision = delayPolicy(traceContext)(submissionRequest)
        if (TimeProof.isTimeProofSubmission(submissionRequest)) decision
        else
          sequencerTimeTracker
            .awaitTick(simClock.now)(traceContext)
            .fold(decision)(tickObserved => SendDecision.HoldBack(tickObserved))
    }
  }

  private lazy val reassignmentFailureLogAssertions: Seq[LogEntry] => Assertion =
    LogEntry.assertLogSeq(
      Seq.empty,
      Seq(
        _.warningMessage should include regex "has exceeded the max-sequencing-time .* of the send request",
        _.warningMessage should include("Sequencing result message timed out"),
        _.warningMessage should include(
          "Rejected transaction due to a participant determined timeout"
        ),
      ),
    )

  private def getReassignmentData(
      participant: LocalParticipantReference,
      reassignmentId: ReassignmentId,
  )(implicit
      env: TestConsoleEnvironment
  ): Either[ReassignmentStore.ReassignmentLookupError, UnassignmentData] =
    participant.underlying.value.sync.syncPersistentStateManager
      .get(env.acmeId)
      .value
      .reassignmentStore
      .lookup(reassignmentId)
      .value
      .failOnShutdown
      .futureValue

  "ReassignmentService" should {
    "generate a CommandRejected in case of timeout around unassignment" in { implicit env =>
      import env.*

      val signatory = participant1.adminParty
      val observer = participant2.adminParty

      // Create contract
      val (contract, _, _) = runCommand(
        IouSyntax.createIou(participant1, Some(daId))(signatory, observer),
        daId,
        "CantonConsole",
        signatory.toLf,
        Some(participant1),
      )
      val cid = contract.id.toLf

      setSequencerPolicy(daName)(
        delayConfirmationResponse = defaultsParameters.confirmationResponseTimeout +
          NonNegativeFiniteDuration.tryOfSeconds(1),
        delayMediator = defaultsParameters.mediatorReactionTimeout,
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val submissionId = freshSubmissionId()
          // Trying to unassignment contract
          val unassignmentCompletion =
            failingUnassignment(
              cid = cid,
              source = daId,
              target = acmeId,
              signatory.toLf,
              Some(participant1),
              submissionId = submissionId,
            )

          participant1.testing.state_inspection
            .lookupDeduplicationData(
              ChangeIdHash(
                ChangeId(
                  userId = Ref.UserId.assertFromString(unassignmentCompletion.userId),
                  commandId = LfCommandId.assertFromString(unassignmentCompletion.commandId),
                  actAs = Set(signatory.toLf),
                )
              )
            )
            .futureValueUS shouldBe None

          val unassignmentTs = CantonTimestamp
            .fromProtoTimestamp(unassignmentCompletion.synchronizerTime.value.recordTime.value)
            .value

          val reassignmentId = ReassignmentId(
            Source(daId),
            Target(acmeId),
            unassignmentTs,
            Seq(cid -> ReassignmentCounter(0)),
          )

          // Entry is deleted upon timeout
          getReassignmentData(
            participant1,
            reassignmentId,
          ).left.value shouldBe UnknownReassignmentId(reassignmentId)
          eventually() {
            getReassignmentData(
              participant2,
              reassignmentId,
            ).left.value shouldBe UnknownReassignmentId(reassignmentId)
          }

          val status = unassignmentCompletion.status.value
          status.code should not be 0
          status.message should include(
            "Rejected transaction due to a participant determined timeout"
          )

          unassignmentCompletion.userId shouldBe defaultUserId
          unassignmentCompletion.submissionId shouldBe submissionId
          unassignmentCompletion.commandId shouldBe defaultCommandId

          // Cleaning
          setSequencerPolicy(daName)(
            delayConfirmationResponse = NonNegativeFiniteDuration.Zero,
            delayMediator = NonNegativeFiniteDuration.Zero,
          )
          IouSyntax.archive(participant1)(contract, signatory)
        },
        reassignmentFailureLogAssertions,
      )
    }

    "generate a CommandRejected in case of timeout around assignment" in { implicit env =>
      import env.*

      val signatory = participant1.adminParty
      val observer = participant2.adminParty

      // Create contract
      val (contract, _, _) = runCommand(
        IouSyntax.createIou(participant1, Some(daId))(signatory, observer),
        daId,
        "CantonConsole",
        signatory.toLf,
        Some(participant1),
      )
      val cid = contract.id.toLf

      // unassignment contract
      val (unassignedEvent, _) = unassign(
        cid = cid,
        source = daId,
        target = acmeId,
        submittingParty = signatory.toLf,
        participantOverride = Some(participant1),
      )

      // Check unassignment
      assertNotInLedgerAcsSync(
        partyId = signatory,
        participantRefs = List(participant1),
        synchronizerId = daId,
        cid = cid,
      )

      setSequencerPolicy(acmeName)(
        delayConfirmationResponse =
          defaultsParameters.confirmationResponseTimeout + NonNegativeFiniteDuration.tryOfSeconds(
            1
          ),
        delayMediator = defaultsParameters.mediatorReactionTimeout,
      )

      // Trying to assign contract
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val submissionId = freshSubmissionId()
          val failedAssignmentCompletion = failingAssignment(
            reassignmentId = unassignedEvent.reassignmentId,
            source = daId,
            target = acmeId,
            submittingParty = signatory.toLf,
            participantOverrideO = Some(participant1),
            submissionId = submissionId,
          )

          val status = failedAssignmentCompletion.status.value
          status.code should not be 0
          status.message should include(
            "Rejected transaction due to a participant determined timeout"
          )

          failedAssignmentCompletion.userId shouldBe defaultUserId
          failedAssignmentCompletion.submissionId shouldBe submissionId
          failedAssignmentCompletion.commandId shouldBe defaultCommandId

          // Cleaning
          setSequencerPolicy(acmeName)(
            delayConfirmationResponse = NonNegativeFiniteDuration.Zero,
            delayMediator = NonNegativeFiniteDuration.Zero,
          )

          assign(
            reassignmentId = unassignedEvent.reassignmentId,
            source = daId,
            target = acmeId,
            submittingParty = signatory.toLf,
            participantOverride = Some(participant1),
          ).discard

          IouSyntax.archive(participant1)(contract, signatory)
        },
        reassignmentFailureLogAssertions,
      )
    }
  }
}

//class ReassignmentServiceTimeoutCommandRejectedIntegrationTestDefault
//    extends ReassignmentServiceTimeoutCommandRejectedIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseBftSequencer(
//      loggerFactory,
//      sequencerGroups = Seq(Set("sequencer1"), Set("sequencer2"))
//        .map(_.map(InstanceName.tryCreate)),
//    )
//  )
//  // we need to register the ProgrammableSequencer after the BftSequencer
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}

class ReassignmentServiceTimeoutCommandRejectedIntegrationTestPostgres
    extends ReassignmentServiceTimeoutCommandRejectedIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the BftSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
