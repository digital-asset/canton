// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalMediatorReference, LocalSequencerReference}
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.integration.plugins.UseBftSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  AutomaticAssignment,
  EntitySyntax,
  PartiesAllocator,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SequencerTestHelper,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentProcessor,
  UnassignmentProcessor,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  LocalRejectError,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ManagedChannel
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.reflect.ClassTag

/** The goal is to check that the replay of a reassignment request that was rejected is discarded,
  * after the mediator has forgotten the request uuid.
  *
  * The reassignment is rejected because the confirmation responses never reach the mediator. The
  * captured request is then sent again by another member, once the mediator deduplication timeout
  * has elapsed.
  *
  * Topology:
  *   - Synchronizers: da (source) and acme (target)
  *   - signatory -> P1, connected to both
  *   - P2 only sends the replayed requests, connected to both
  */
final class ReplayedRejectedReassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with SecurityTestSuite
    with EntitySyntax {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // The ProgrammableSequencer wraps the sequencer configured by the BftSequencer plugin
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var signatory: PartyId = _

  private lazy val replayedRejectedReassignment: SecurityTest = SecurityTest(
    property = Integrity,
    asset = "ledger",
    attack = Attack(
      actor = "a malicious network participant",
      threat = "replays a rejected reassignment request after the mediator has forgotten its uuid",
      mitigation = "the submitting participant rejects the replay and emits no completion",
    ),
  )
  private val sequencerChannels: AtomicReference[Seq[ManagedChannel]] = new AtomicReference(
    Seq.empty
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.disableAutomaticAssignment,
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participant1.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        participant2.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)

        PartiesAllocator(Set(participant1))(
          Seq("signatory" -> participant1),
          Map(
            "signatory" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> Submission)),
              acmeId -> (PositiveInt.one, Set(participant1.id -> Submission)),
            )
          ),
        )

        Seq(sequencer1, sequencer2).foreach { sequencer =>
          AutomaticAssignment.disable(Seq(sequencer), sequencer.synchronizer_id)
        }

        signatory = "signatory".toPartyId(participant1)
        pureCryptoRef.set(sequencer1.crypto.pureCrypto)
      }

  override def afterAll(): Unit = {
    sequencerChannels
      .get()
      .foreach(
        SequencerTestHelper.closeChannel(_, logger, getClass.getSimpleName)
      )
    super.afterAll()
  }

  private def createContract()(implicit env: TestConsoleEnvironment): ContractInstance = {
    import env.*

    val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, signatory, 1000)
    participant1.testing
      .acs_search(daName, exactId = iou.id.contractId, limit = PositiveInt.one)
      .loneElement
  }

  /** Stub that sends submission requests to the sequencer as participant2. The token is requested
    * when the stub is created, so it is created after the sim clock has advanced.
    */
  private def participant2SequencerStub(
      sequencer: LocalSequencerReference,
      synchronizerId: PhysicalSynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit
      env: TestConsoleEnvironment
  ): SequencerServiceStub = {
    import env.*

    val sequencerChannel =
      SequencerTestHelper.createChannel(sequencer, loggerFactory, executionContext)
    sequencerChannels.updateAndGet(_ :+ sequencerChannel)

    val token = SequencerTestHelper
      .requestToken(
        sequencerChannel,
        synchronizerId,
        participant2.id,
        SynchronizerCrypto(participant2.crypto, staticSynchronizerParameters),
        testedProtocolVersion,
        loggerFactory,
      )
      .futureValueUS

    new SequencerServiceStub(sequencerChannel).withCallCredentials(
      SequencerTestHelper.mkCallCredentials(synchronizerId, participant2.id, token.value)
    )
  }

  /** Submits a reassignment and lets the mediator time it out, by dropping the confirmation
    * responses. Returns the confirmation request sent by participant1.
    */
  private def submitAndTimeOut[Processor: ClassTag](
      sequencer: LocalSequencerReference,
      mediator: LocalMediatorReference,
  )(submit: => Unit)(implicit env: TestConsoleEnvironment): SubmissionRequest = {
    import env.*

    val clock = environment.simClock.value
    val parameters = sequencer.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(sequencer.synchronizer_id)
    val minObservationDuration =
      mediator.underlying.value.replicaManager.mediatorRuntime.value.mediator.timeTracker.config.minObservationDuration

    val programmableSequencer: ProgrammableSequencer = getProgrammableSequencer(sequencer.name)
    val requestP = Promise[SubmissionRequest]()
    programmableSequencer.setPolicy_(
      "capture the reassignment request and drop the confirmation responses"
    ) { request =>
      if (request.isConfirmationRequest && request.sender == participant1.id) {
        requestP.trySuccess(request)
        SendDecision.Process
      } else if (isConfirmationResponse(request)) SendDecision.Drop
      else SendDecision.Process
    }

    val request = loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.forLogger[Processor] && SuppressionRule.Level(Level.WARN)
    )(
      {
        submit
        val request = requestP.future.futureValue

        // The mediator times out the request, so the reassignment is rejected
        clock.advance(
          parameters.confirmationResponseTimeout.asJava
            .plusSeconds(1)
            .plus(parameters.mediatorReactionTimeout.asJava)
            .plus(minObservationDuration.asJava)
            .plusSeconds(1)
        )
        request
      },
      LogEntry.assertLogSeq(
        Seq(
          (
            _.warningMessage should include regex "Response message for request \\[.*\\] timed out at",
            "the mediator times out the request",
          )
        )
      ),
    )

    programmableSequencer.resetPolicy()
    request
  }

  /** Replays the request as participant2 once the mediator has forgotten its uuid, and waits until
    * the decision time of the replay has passed.
    */
  private def replayAndConclude(
      request: SubmissionRequest,
      sequencer: LocalSequencerReference,
      synchronizerId: PhysicalSynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      processingStepsLoggerName: String,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val clock = environment.simClock.value
    val parameters = sequencer.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(synchronizerId)

    // The mediator forgets the uuid of the request
    clock.advance(parameters.mediatorDeduplicationTimeout.asJava.plusSeconds(1))

    val replayRequest = request.copy(
      sender = participant2.id,
      maxSequencingTime = clock.now.plusSeconds(60),
    )
    val signature = signModifiedSubmissionRequestForParticipant(
      replayRequest,
      participant2.underlying.value,
      synchronizerId,
      staticSynchronizerParameters,
    ).signature

    logger.info("Now replaying the rejected reassignment request...")

    loggerFactory.assertLogs(
      SuppressionRule.LoggerNameContains(processingStepsLoggerName) && SuppressionRule
        .Level(Level.WARN)
    )(
      {
        SequencerTestHelper
          .sendSubmissionRequest(
            participant2SequencerStub(sequencer, synchronizerId, staticSynchronizerParameters),
            replayRequest,
            signature,
          )
          .futureValue

        // The ping is sequenced after the replayed request
        assertPingSucceeds(participant1, participant2, synchronizerId = Some(synchronizerId))
      },
      _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.MalformedRequest),
    )

    // Advance in steps below the confirmation timeouts until the decision time of the replay has
    // passed, so that the replay is concluded before the completions are checked
    val replayDecisionTime = clock.now
      .plus(parameters.confirmationResponseTimeout.asJava)
      .plus(parameters.mediatorReactionTimeout.asJava)
      .plusSeconds(1)
    val step = java.time.Duration.ofSeconds(10)
    val remaining =
      java.time.Duration.between(clock.now.toInstant, replayDecisionTime.toInstant)
    (1 to Math.ceil(remaining.toMillis.toDouble / step.toMillis).toInt).foreach { _ =>
      clock.advance(step)
      participants.all.foreach(_.testing.fetch_synchronizer_times())
    }

    // The ping is sequenced after the decision time of the replay
    assertPingSucceeds(participant1, participant2, synchronizerId = Some(synchronizerId))
  }

  "A replayed unassignment that was rejected" should {
    "be discarded" taggedAs replayedRejectedReassignment onlyRunWithOrGreaterThan ProtocolVersion.reassignmentReplayProtection in {
      implicit env =>
        import env.*

        val contract = createContract()
        val offsetAtBeginning = participant1.ledger_api.state.end()

        val unassignmentRequest = submitAndTimeOut[UnassignmentProcessor](sequencer1, mediator1) {
          participant1.ledger_api.commands.submit_unassign_async(
            signatory,
            Seq(contract.contractId),
            daId,
            acmeId,
          )
        }

        val rejection = participant1.ledger_api.completions
          .list(signatory, 1, offsetAtBeginning)
          .loneElement
        rejection.status.value.code should not be 0

        participant1.testing
          .acs_search(daName, exactId = contract.contractId.coid)
          .map(_.contractId.coid) should contain(contract.contractId.coid)

        replayAndConclude(
          unassignmentRequest,
          sequencer1,
          daId,
          staticSynchronizerParameters1,
          "UnassignmentProcessingSteps",
        )

        val contractsOnSource = participant1.testing
          .acs_search(daName, exactId = contract.contractId.coid)
          .map(_.contractId.coid)

        contractsOnSource should contain(contract.contractId.coid)
        assertNoExtraCompletion(participant1, signatory, offsetAtBeginning)
    }
  }

  "A replayed assignment that was rejected" should {
    "be discarded" taggedAs replayedRejectedReassignment onlyRunWithOrGreaterThan ProtocolVersion.reassignmentReplayProtection in {
      implicit env =>
        import env.*

        val contract = createContract()
        val unassigned = participant1.ledger_api.commands.submit_unassign(
          signatory,
          Seq(contract.contractId),
          daId,
          acmeId,
        )
        val offsetAtBeginning = participant1.ledger_api.state.end()

        val assignmentRequest = submitAndTimeOut[AssignmentProcessor](sequencer2, mediator2) {
          participant1.ledger_api.commands.submit_assign_async(
            signatory,
            unassigned.reassignmentId,
            daId,
            acmeId,
          )
        }

        val rejection = participant1.ledger_api.completions
          .list(signatory, 1, offsetAtBeginning)
          .loneElement
        rejection.status.value.code should not be 0

        participant1.testing
          .acs_search(acmeName, exactId = contract.contractId.coid) shouldBe empty

        replayAndConclude(
          assignmentRequest,
          sequencer2,
          acmeId,
          staticSynchronizerParameters2,
          "AssignmentProcessingSteps",
        )

        val contractsOnTarget = participant1.testing
          .acs_search(acmeName, exactId = contract.contractId.coid)

        contractsOnTarget shouldBe empty
        assertNoExtraCompletion(participant1, signatory, offsetAtBeginning)
    }
  }
}
