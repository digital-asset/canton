// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.admin.party.AcsReplicationTestInterceptor
import com.digitalasset.canton.participant.admin.party.AcsReplicationTestInterceptor.{
  Proceed,
  ProceedOrWait,
  Wait,
}
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.PartyNotInFreezingHostingState
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, SubmissionRequest}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*
import org.scalatest.{Assertion, Succeeded}
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

/** Objective: Test idempotent get_remote_acs remote reads.
  *
  * Verify correctness of the returned ACS snapshot by comparing the result against export_party-acs
  * result.
  *
  * Setup:
  *   - 2 participants: participant1 hosts an external party to remote read by participant2
  *   - 1 mediator/sequencer each
  *
  * Initialization steps:
  *   - Create a batch of contracts on participant1 and participant2 for the external party
  *   - Nominate the external party on participant2
  *   - Freeze the external party on participant2
  *
  * Scenario:
  *   - Start get_remote_acs on participant2 and stall it at some point during the replication (see
  *     sub-classes for details)
  *   - Call get_remote_acs on participant2 again, which should reset the ACS replication progress
  *     or pick up from where it stalled and the previously stalled get_remote_acs should fail (or
  *     has already failed, for example due to a command timeout)
  *   - Verify that the interrupted get_remote_acs fails
  *   - Verify that the restarted get_remote_acs completes successfully
  *   - Verify that a subsequent get_remote_acs invocation fails with an error indicating that the
  *     previous invocation has already completed
  *   - Verify that the result of get_remote_acs matches the result of export_party_acs on
  *     participant1
  */
trait GetRemoteAcsIdempotencyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var aliceE: ExternalParty = _
  private var bob: PartyId = _
  private var carol: PartyId = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  private val numContractsInCreateBatch = 100

  private val className: String = this.getClass.getSimpleName
  private val remoteReadFileName = s"canton-remote-acs-$className.gz"
  private val exportAcsFileName = s"canton-acs-export-$className.gz"

  // Lets the test stall the TP at some point during ACS replication
  protected val stallTargetParticipant = new AtomicBoolean(false)
  protected val hasTargetParticipantStalled = new AtomicBoolean(false)
  // Counts how often the TP processor attempts to progress ACS replication
  protected val targetParticipantProgressAttempts = new AtomicInteger(0)

  // Lets the test have the TP ignore the end of the ACS sent by the SP
  protected val ignoreEndOfAcs = new AtomicBoolean(false)
  protected val hasIgnoredEndOfAcs = new AtomicBoolean(false)

  private def stallingInterceptor(): AcsReplicationTestInterceptor =
    new AcsReplicationTestInterceptor {
      override def onTargetParticipantProgress(
          progress: AcsReplicationStatus.AcsReplicationProgress
      )(implicit traceContext: TraceContext): ProceedOrWait = {
        targetParticipantProgressAttempts.incrementAndGet().discard
        if (stallTargetParticipant.get() && progress.processedContractCount.unwrap > 0) {
          hasTargetParticipantStalled.set(true)
          Wait
        } else Proceed
      }

      override def onTargetParticipantReceivedEndOfAcs()(implicit
          traceContext: TraceContext
      ): ProceedOrWait =
        if (ignoreEndOfAcs.get()) {
          hasIgnoredEndOfAcs.set(true)
          Wait
        } else Proceed
    }

  /** Starts a remote read with `remoteReadToInterrupt`, stalls it, then calls it again with
    * `subsequentRemoteRead`, which must complete before this returns.
    *
    * @return
    *   the interrupted remote read
    */
  protected def interruptAndCallAgainRemoteRead(
      remoteReadToInterrupt: => Unit,
      subsequentRemoteRead: => Unit,
  )(implicit env: TestConsoleEnvironment): Future[Unit]

  /** Console timeout of the remote read to interrupt, unbounded if undefined. */
  protected def interruptedRemoteReadTimeout: Option[NonNegativeDuration] = None

  /** Log entries captured while interrupting and restarting the remote read. Entries below WARN are
    * checked by [[checkInterruptionInfoLogs]].
    */
  protected def interruptionLogRule: SuppressionRule = SuppressionRule.LevelAndAbove(Level.WARN)

  /** The error logged by the console for the failed interrupted remote read. */
  protected def interruptedRemoteReadExpectedError: (LogEntry => Assertion, String) =
    (
      _.errorMessage should include("has been restarted by another request"),
      "interrupted remote read failure",
    )

  protected def checkInterruptionInfoLogs: Seq[LogEntry] => Assertion = _ => Succeeded

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAlphaAcsReplicationSupport(
          Map("participant2" -> (() => stallingInterceptor()))
        )*
      )
      // Request contracts in multiple batches so that the TP can be stalled mid-transfer.
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.alphaAcsReplicationSupport)
            .modify(_.map(_.focus(_.target.acsBatchSize).replace(PositiveInt.tryCreate(10))))
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        aliceE = participant1.parties.testing.external
          .enable("aliceE", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("bob", synchronizeParticipants = Seq(participant1))
        carol = participant1.parties.enable("carol", synchronizeParticipants = Seq(participant2))
      }

  private def awaitHostingState(state: HostingState)(implicit env: TestConsoleEnvironment) = {
    import env.*
    eventually() {
      val ptp1 = participant1.topology.party_to_participant_mappings
        .list(daId, filterParty = aliceE.filterString)
        .loneElement
      val ptp2 = participant2.topology.party_to_participant_mappings
        .list(daId, filterParty = aliceE.filterString)
        .loneElement

      logger.debug(s"PTP on P1: $ptp1")
      logger.debug(s"PTP on P2: $ptp2")

      ptp1.item.participants.map(p => (p.participantId, p.state)).toSet shouldBe Set(
        participant1.id -> HostingState.Active,
        participant2.id -> state,
      )
      ptp2.item.participants.map(p => (p.participantId, p.state)).toSet shouldBe Set(
        participant1.id -> HostingState.Active,
        participant2.id -> state,
      )
    }
  }

  // TODO(#37839): Switch this and subsequent tests to pv37 along with marking acs_replication.proto
  //  replication protocol messages to stable/pv37
  "Create external party with contracts" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    val amounts = 1 to numContractsInCreateBatch
    ListBuffer.from(IouSyntax.createIous(participant1, carol, aliceE, amounts))
    // Create some contracts shared with a party (Bob) on the target participant P2.
    ListBuffer.from(IouSyntax.createIous(participant2, bob, aliceE, amounts))

    clue("Nominating Alice on P2") {
      aliceE.topology.party_to_participant_mappings.propose_delta(
        participant1,
        adds = Seq(
          participant2.id -> ParticipantPermission.Confirmation
        ),
        mustFullyAuthorize = true, // since submitting with external party keys on TP
        store = daId,
        participantStates = Map(participant2.id -> HostingState.Nominated),
      )
      awaitHostingState(HostingState.Nominated)
    }

    clue("Not ready to read Alice P2 freezing time") {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant2.parties.get_party_freezing_time(aliceE, daId),
        entry => {
          entry shouldBeCantonErrorCode PartyNotInFreezingHostingState
          entry.errorMessage should include(
            PartyNotInFreezingHostingState.Error(aliceE, daId, participant2.id).cause
          )
        },
      )
    }

    clue("P2 freezing Alice") {
      participant2.topology.party_to_participant_mappings.propose_delta(
        aliceE,
        mustFullyAuthorize = true, // since submitting with external party keys on TP
        store = daId,
        participantStates = Map(participant2.id -> HostingState.Freezing),
      )
      awaitHostingState(HostingState.Freezing)
    }
  }

  "Restart get_remote_acs on P2 after interruption" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val asOf = clue("Read Alice P2 freezing time") {
        participant2.parties.get_party_freezing_time(aliceE, daId)
      }

      def callGetRemoteAcs(timeout: Option[NonNegativeDuration]): Unit =
        participant2.parties.get_remote_acs(
          Seq(aliceE),
          daId,
          participant1,
          asOf,
          getRemoteAcsFilePath = remoteReadFileName,
          timeout = timeout.getOrElse(env.commandTimeouts.unbounded),
        )

      loggerFactory.assertLogsSeq(interruptionLogRule)(
        {
          val interruptedRemoteRead =
            interruptAndCallAgainRemoteRead(
              callGetRemoteAcs(interruptedRemoteReadTimeout),
              callGetRemoteAcs(timeout = None),
            )

          clue("Interrupted remote read fails") {
            interruptedRemoteRead.failed.futureValue shouldBe a[CommandFailure]
          }
        },
        entries => {
          val (warningsAndErrors, infos) =
            entries.partition(_.level.toInt >= Level.WARN.toInt)
          checkInterruptionInfoLogs(infos)
          LogEntry.assertLogSeq(
            Seq(interruptedRemoteReadExpectedError),
            // The TP aborts the sequencer channel of the interrupted remote read with an error.
            Seq(
              _.warningMessage should include("aborted: ACS replication"),
              _.warningMessage should (include("Member message handler received error") or include(
                "has terminated connection"
              )),
              _.warningMessage should include("CANCELLED"),
            ),
          )(warningsAndErrors)
        },
      )
  }

  "get_remote_acs on P2 cannot be restarted after completion" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val asOf = participant2.parties.get_party_freezing_time(aliceE, daId)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        a[CommandFailure] shouldBe thrownBy(
          participant2.parties.get_remote_acs(
            Seq(aliceE),
            daId,
            participant1,
            asOf,
            getRemoteAcsFilePath = remoteReadFileName,
          )
        ),
        LogEntry.assertLogSeq(
          Seq((_.errorMessage should include("already exists"), "already exists failure")),
          Seq(_.warningMessage should include("already exists")),
        ),
      )
  }

  "Invoke export_party_acs on P1 for comparison" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val exportComparisonLedgerOffset = participant1.ledger_api.state.end()

      clue("Onboarding Alice on P2") {
        participant2.topology.party_to_participant_mappings.propose_delta(
          aliceE,
          mustFullyAuthorize = true, // since submitting with external party keys on TP
          store = daId,
          participantStates = Map(participant2.id -> HostingState.Onboarding),
        )
        awaitHostingState(HostingState.Onboarding)
      }

      // Ensure the LAPI observes the Onboarding transaction on P1.
      eventually() {
        val txWrapperO = participant1.ledger_api.updates
          .topology_transactions(
            completeAfter = PositiveInt.one,
            partyIds = Seq(aliceE),
            beginOffsetExclusive = exportComparisonLedgerOffset,
          )
          .headOption

        val topologyEventO = txWrapperO
          .flatMap(_.topologyTransaction.events.headOption)
          .map(_.event)
        logger.debug(s"Source LAPI topology event: $topologyEventO")
        topologyEventO.map(_.isParticipantAuthorizationOnboarding) shouldBe Some(true)
      }

      clue("Exporting ACS of Alice from P1") {
        participant1.parties.export_party_acs(
          aliceE,
          daId,
          participant2,
          exportComparisonLedgerOffset,
          exportFilePath = exportAcsFileName,
        )
      }
  }

  "Result of P2.get_remote_acs(P1) matches P1.export_party_acs" onlyRunWith ProtocolVersion.dev in {
    _ =>
      import better.files.File

      val remoteReadFile = File.apply(remoteReadFileName)
      val exportAcsFile = File.apply(exportAcsFileName)

      assert(
        remoteReadFile.isSameContentAs(exportAcsFile),
        s"get_remote_acs file $remoteReadFileName unexpectedly differs from export_party_acs file $exportAcsFileName",
      )

  }
}

/** Stalls the target participant mid-transfer, i.e. after it has replicated some contracts. */
trait GetRemoteAcsRestartMidTransferIntegrationTest extends GetRemoteAcsIdempotencyIntegrationTest {
  override protected def interruptAndCallAgainRemoteRead(
      remoteReadToInterrupt: => Unit,
      subsequentRemoteRead: => Unit,
  )(implicit env: TestConsoleEnvironment): Future[Unit] = {
    import env.*

    stallTargetParticipant.set(true)
    val interrupted = clue("P2 starts remote-reading from P1 and stalls mid-transfer") {
      val remoteRead = Future(remoteReadToInterrupt)
      eventually()(hasTargetParticipantStalled.get() shouldBe true)
      remoteRead
    }

    clue("P2 restarts remote-read from P1") {
      stallTargetParticipant.set(false)
      subsequentRemoteRead
    }

    interrupted
  }
}

/** Stalls the target participant after it has proposed the ACS replication sequencer channel, by
  * holding back the source participant's acceptance of the proposal at the sequencer. The target
  * participant thus waits for the agreement and has not connected to the sequencer channel yet.
  */
trait GetRemoteAcsRestartAfterChannelProposalIntegrationTest
    extends GetRemoteAcsIdempotencyIntegrationTest
    with HasProgrammableSequencer {

  private def isConfirmationRequest(submission: SubmissionRequest): Boolean =
    !ProgrammableSequencerPolicies.isConfirmationResponse(submission) &&
      submission.batch.envelopes.exists(_.recipients.allRecipients.exists {
        case MediatorGroupRecipient(_) => true
        case _ => false
      })

  override protected def interruptAndCallAgainRemoteRead(
      remoteReadToInterrupt: => Unit,
      subsequentRemoteRead: => Unit,
  )(implicit env: TestConsoleEnvironment): Future[Unit] = {
    import env.*

    val sequencer = getProgrammableSequencer(sequencer1.name)
    val releaseAgreement = Promise[Unit]()
    val isAgreementHeld = new AtomicBoolean(false)

    sequencer.setPolicy_("hold back P1's acceptance of the ACS replication proposal")(
      SendPolicy.processTimeProofs_ { submission =>
        if (submission.sender == participant1.id && isConfirmationRequest(submission)) {
          isAgreementHeld.set(true)
          SendDecision.HoldBack(releaseAgreement.future)
        } else SendDecision.Process
      }
    )

    try {
      val interrupted = clue("P2 starts remote-reading from P1 and stalls after the proposal") {
        val remoteRead = Future(remoteReadToInterrupt)
        eventually()(isAgreementHeld.get() shouldBe true)
        remoteRead
      }

      val restarted = clue("P2 restarts remote-read from P1") {
        val remoteRead = Future(subsequentRemoteRead)
        // The interrupted remote read fails as soon as it has been restarted.
        eventually()(interrupted.isCompleted shouldBe true)
        remoteRead
      }

      clue("P1 accepts the proposal and the restarted remote read completes") {
        sequencer.resetPolicy()
        releaseAgreement.success(())
        restarted.futureValue
      }

      interrupted
    } finally {
      sequencer.resetPolicy()
      releaseAgreement.trySuccess(()).discard
    }
  }
}

/** Stalls the TP after the SP has sent the EndOfAcs. The SP thus considers its part done and needs
  * the target participant's abort to reconnect upon restart.
  */
trait GetRemoteAcsRestartAfterEndOfAcsIntegrationTest
    extends GetRemoteAcsIdempotencyIntegrationTest {
  override protected def interruptAndCallAgainRemoteRead(
      remoteReadToInterrupt: => Unit,
      subsequentRemoteRead: => Unit,
  )(implicit env: TestConsoleEnvironment): Future[Unit] = {
    import env.*

    ignoreEndOfAcs.set(true)
    val interrupted = clue("P2 starts remote-reading from P1 and ignores the end of the ACS") {
      val remoteRead = Future(remoteReadToInterrupt)
      eventually()(hasIgnoredEndOfAcs.get() shouldBe true)
      remoteRead
    }

    clue("P2 restarts remote-read from P1") {
      ignoreEndOfAcs.set(false)
      subsequentRemoteRead
    }

    interrupted
  }
}

/** Stalls the target participant mid-transfer, so that the interrupted remote read times out on the
  * console. The resulting gRPC cancellation must be detected by the target participant and stop the
  * ACS replication before the remote read is restarted.
  */
trait GetRemoteAcsRestartAfterTimeoutIntegrationTest
    extends GetRemoteAcsIdempotencyIntegrationTest {
  private val grpcCancelledMessage =
    "gRPC request is cancelled, not waiting for remote read result anymore"
  private val replicationStoppedMessage = "is cancelled by the get_remote_acs caller"

  override protected def interruptedRemoteReadTimeout: Option[NonNegativeDuration] =
    Some(NonNegativeDuration.ofSeconds(10))

  override protected def interruptionLogRule: SuppressionRule =
    super.interruptionLogRule ||
      (SuppressionRule.LoggerNameContains("AcsReplicator") && SuppressionRule.Level(Level.INFO))

  override protected def interruptedRemoteReadExpectedError: (LogEntry => Assertion, String) =
    (
      _.errorMessage should include("Condition never became true"),
      "interrupted remote read console timeout",
    )

  override protected def checkInterruptionInfoLogs: Seq[LogEntry] => Assertion = entries => {
    entries.map(_.message) should contain(grpcCancelledMessage)
    exactly(1, entries.map(_.message)) should (include("Stopping ACS replication") and include(
      replicationStoppedMessage
    ))
    forAll(entries)(_.message should not include "has been restarted by another request")
  }

  override protected def interruptAndCallAgainRemoteRead(
      remoteReadToInterrupt: => Unit,
      subsequentRemoteRead: => Unit,
  )(implicit env: TestConsoleEnvironment): Future[Unit] = {
    import env.*

    stallTargetParticipant.set(true)
    val interrupted = clue("P2 starts remote-reading from P1 and stalls mid-transfer") {
      val remoteRead = Future(remoteReadToInterrupt)
      eventually()(hasTargetParticipantStalled.get() shouldBe true)
      remoteRead
    }

    clue("Interrupted remote read times out and P2 detects the gRPC cancellation") {
      eventually(timeUntilSuccess = 1.minute)(interrupted.isCompleted shouldBe true)
      eventually() {
        loggerFactory.fetchRecordedLogEntries.map(_.message) should contain(grpcCancelledMessage)
      }
    }

    clue("P2 stops the cancelled ACS replication even though it is still stalled") {
      eventually() {
        val attemptsBefore = targetParticipantProgressAttempts.get()
        Threading.sleep(2.seconds.toMillis)
        targetParticipantProgressAttempts.get() shouldBe attemptsBefore
      }
    }

    clue("P2 restarts remote-read from P1") {
      stallTargetParticipant.set(false)
      subsequentRemoteRead
    }

    interrupted
  }
}

class GetRemoteAcsRestartMidTransferIntegrationTestH2
    extends GetRemoteAcsRestartMidTransferIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}

class GetRemoteAcsRestartAfterChannelProposalIntegrationTestPostgres
    extends GetRemoteAcsRestartAfterChannelProposalIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}

class GetRemoteAcsRestartAfterEndOfAcsIntegrationTestH2
    extends GetRemoteAcsRestartAfterEndOfAcsIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}

class GetRemoteAcsRestartAfterTimeoutIntegrationTestPostgres
    extends GetRemoteAcsRestartAfterTimeoutIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
