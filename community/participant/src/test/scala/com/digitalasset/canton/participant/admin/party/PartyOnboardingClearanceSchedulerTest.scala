// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.sync.{ConnectedSynchronizer, SyncEphemeralState}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreTestData
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.mockito.ArgumentMatchers.eq as isEq
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

class PartyOnboardingClearanceSchedulerTest
    extends AnyWordSpec
    with BaseTest
    with MockitoSugar
    with HasExecutionContext {

  /** A fake implementation of the workflow for stable testing. */
  private class FakePartyReplicationTopologyWorkflow(loggerFactory: NamedLoggerFactory)
      extends PartyReplicationTopologyWorkflow(
        participantId,
        timeouts,
        loggerFactory,
      ) {

    // Queue of predefined responses (success or failure) for the fake workflow.
    private val responses = new mutable.Queue[Either[String, PartyOnboardingFlagStatus]]()

    // Records the arguments passed to `authorizeOnboardedTopology` for assertion.
    val calls = new ConcurrentLinkedQueue[(PartyId, EffectiveTime)]

    def addResponse(response: PartyOnboardingFlagStatus): Unit =
      responses.enqueue(Right(response))

    def addFailure(error: String): Unit = responses.enqueue(Left(error))

    override def authorizeClearingOnboardingFlag(
        partyId: PartyId,
        targetParticipantId: ParticipantId,
        onboardingEffectiveAt: EffectiveTime,
        connectedSynchronizer: ConnectedSynchronizer,
        requestId: Option[Hash] = None,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, PartyOnboardingFlagStatus] = {
      calls.add((partyId, onboardingEffectiveAt))
      val result = if (responses.isEmpty) {
        EitherT.leftT[FutureUnlessShutdown, PartyOnboardingFlagStatus](
          "FakePartyReplicationTopologyWorkflow has no more responses"
        )
      } else {
        EitherT.fromEither[FutureUnlessShutdown](responses.dequeue())
      }
      result
    }
  }

  // Helper for generating standard topology test data (parties, participants, keys).
  private lazy val testData = new TopologyStoreTestData(
    testedProtocolVersion,
    loggerFactory,
    this.directExecutionContext,
  )
  private lazy val partyId = testData.party1
  private lazy val participantId = testData.p1Id
  private lazy val psid = testData.synchronizer1_p1p2_physicalSynchronizerId
  private val earliestClearanceTime = CantonTimestamp.now()
  private val onboardingEffectiveAt = EffectiveTime(CantonTimestamp.Epoch)

  // A sample task, used for comparing against the scheduler's pending clearances.
  private lazy val task = OnboardingClearanceTask(
    partyId = partyId,
    earliestClearanceTime = earliestClearanceTime,
    onboardingEffectiveAt = onboardingEffectiveAt,
  )

  /** Uses a dedicated fixture to instantiate fresh mocks for every test.
    *
    * This ensures isolation and prevents race conditions (such as ClassCastExceptions) caused by
    * lingering asynchronous tasks from previous tests accessing shared mocks, as Mockito is not
    * thread-safe in such scenarios.
    */
  private class PartyOnboardingClearanceSchedulerTestFixture {
    val mockConnectedSynchronizer = mock[ConnectedSynchronizer]
    val mockEphemeralState = mock[SyncEphemeralState]
    val mockTimeTracker = mock[SynchronizerTimeTracker]
    val fakeWorkflow = new FakePartyReplicationTopologyWorkflow(loggerFactory)

    // Default setup for mocks
    when(mockConnectedSynchronizer.ephemeral).thenReturn(mockEphemeralState)
    when(mockConnectedSynchronizer.psid).thenReturn(psid)
    when(mockEphemeralState.timeTracker).thenReturn(mockTimeTracker)

    // Properly stub the lifecycle methods used by the Backoff retry policy
    when(mockConnectedSynchronizer.isClosing).thenReturn(false)
    when(
      mockConnectedSynchronizer.synchronizeWithClosingUS(
        any[String]
      )(any[FutureUnlessShutdown[Any]])(
        any[TraceContext],
        any[Thereafter[FutureUnlessShutdown]],
      )
    ).thenAnswer { (inv: InvocationOnMock) =>
      val actionResult = inv.getArgument[FutureUnlessShutdown[Any]](1)
      UnlessShutdown.Outcome(actionResult)
    }

    // The scheduler instance under test.
    val scheduler =
      new OnboardingClearanceScheduler(
        participantId,
        psid,
        () => Some(mockConnectedSynchronizer),
        loggerFactory,
        fakeWorkflow,
        timeouts,
        retryInitialDelay = 1.millisecond,
        retryMaxDelay = 5.milliseconds,
      )
  }

  "PartyOnboardingClearanceScheduler" when {

    "requestOnboardingFlagClearance is called" should {

      "fail if the synchronizer psid mismatches" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        // A mock synchronizer configured with an incorrect psid.
        val mockWrongSync = mock[ConnectedSynchronizer]
        // A different psid used to trigger the mismatch error.
        val wrongPsid = DefaultTestIdentities.physicalSynchronizerId
        when(mockWrongSync.psid).thenReturn(wrongPsid)

        // Create a scheduler that provides the *wrong* synchronizer
        val scheduler = new OnboardingClearanceScheduler(
          participantId,
          psid,
          () => Some(mockWrongSync), // Provider returns the wrong sync
          loggerFactory,
          fixture.fakeWorkflow,
          timeouts,
        )

        val error = scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .leftOrFailShutdown("Request should have failed due to psid mismatch")
          .futureValue

        error should include(s"Psid mismatch: Expected $psid, got $wrongPsid")
      }

      "fail if the synchronizer provider returns None" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture

        // Create a scheduler whose provider returns None
        val scheduler = new OnboardingClearanceScheduler(
          participantId,
          psid,
          () => None, // <-- Provider returns None
          loggerFactory,
          fixture.fakeWorkflow,
          timeouts,
        )

        val error = scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .leftOrFailShutdown("Request should have failed as synchronizer provider returned None")
          .futureValue

        error shouldBe s"Synchronizer connection is not ready (absent): Onboarding flag clearance request for $psid (party: $partyId, participant: $participantId)."
      }

      "not schedule a task if the flag is already FlagNotSet" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        fixture.fakeWorkflow.addResponse(FlagNotSet)

        fixture.scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("Initial check failed")
          .map { result =>
            result shouldBe FlagNotSet
            fixture.fakeWorkflow.calls.size shouldBe 1
            fixture.scheduler.pendingClearances.size shouldBe 0
            verify(fixture.mockTimeTracker, never).awaitTick(any[CantonTimestamp])(
              any[TraceContext]
            )
            succeed
          }
          .futureValue
      }

      "schedule a task only once for the same party" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()
        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        val flagSet = FlagSet(earliestClearanceTime)
        fixture.fakeWorkflow.addResponse(flagSet)
        fixture.fakeWorkflow.addResponse(flagSet)

        val f1 = fixture.scheduler.requestClearance(partyId, onboardingEffectiveAt).value
        val f2 = fixture.scheduler.requestClearance(partyId, onboardingEffectiveAt).value

        FutureUnlessShutdown
          .sequence(Seq(f1, f2))
          .map { results =>
            results.foreach(_ shouldBe Right(flagSet))
            fixture.scheduler.pendingClearances.size shouldBe 1
            fixture.scheduler.pendingClearances(partyId) shouldBe task
            verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(
              any[TraceContext]
            )
            fixture.fakeWorkflow.calls.size shouldBe 2
            succeed
          }
          .failOnShutdown
          .futureValue
      }

      "handle concurrent requests correctly" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()
        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        val flagSet = FlagSet(earliestClearanceTime)
        (1 to 10).foreach(_ => fixture.fakeWorkflow.addResponse(flagSet))

        val allCalls = Future
          .traverse((1 to 10).toList) { _ =>
            fixture.scheduler
              .requestClearance(
                partyId,
                onboardingEffectiveAt,
              )
              .value
              .unwrap
          }

        allCalls.map { _ =>
          fixture.scheduler.pendingClearances.size shouldBe 1
          verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(
            any[TraceContext]
          )
          fixture.fakeWorkflow.calls.size shouldBe 10
          succeed
        }.futureValue
      }

      "propose a transaction when the trigger fires" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()

        when(
          fixture.mockTimeTracker.awaitTick(
            isEq(task.earliestClearanceTime.immediateSuccessor)
          )(
            any[TraceContext]
          )
        )
          .thenReturn(Some(tickPromise.future))

        fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))
        fixture.fakeWorkflow.addResponse(FlagNotSet)

        fixture.scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("make the initial request")
          .flatMap { firstResult =>
            firstResult shouldBe FlagSet(earliestClearanceTime)
            fixture.scheduler.pendingClearances.size shouldBe 1
            fixture.fakeWorkflow.calls.size shouldBe 1

            tickPromise.success(())

            eventuallyAsync() {
              fixture.fakeWorkflow.calls.size shouldBe 2
            }.unwrap.map { _ =>
              fixture.fakeWorkflow.calls.poll() shouldBe (partyId, onboardingEffectiveAt)
              fixture.scheduler.pendingClearances.size shouldBe 1
              verify(fixture.mockTimeTracker, times(1))
                .awaitTick(isEq(task.earliestClearanceTime.immediateSuccessor))(any[TraceContext])
              succeed
            }
          }
          .futureValue
      }

      "log an error if the triggered proposal fails" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()

        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

        // Provide enough failures to exhaust the background 3-retry loop (1 initial + 3 retries)
        (1 to 4).foreach(_ => fixture.fakeWorkflow.addFailure("Topology workflow failed"))

        fixture.scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("Initial onboarding request failed")
          .flatMap { firstResult =>
            firstResult shouldBe FlagSet(earliestClearanceTime)
            fixture.scheduler.pendingClearances.size shouldBe 1
            fixture.fakeWorkflow.calls.size shouldBe 1

            loggerFactory.suppress(SuppressionRule.Level(Level.ERROR)) {
              tickPromise.success(())

              eventuallyAsync() {
                val logs = loggerFactory.fetchRecordedLogEntries
                logs.loneElement.errorMessage should include(
                  s"Onboarding flag clearance attempt for party $partyId failed"
                )
                logs.loneElement.errorMessage should include("Topology workflow failed")
              }.unwrap
            }
          }
          .map { assertionAsUnlessShutdown =>
            assertionAsUnlessShutdown shouldBe UnlessShutdown.Outcome(succeed)
            // 1 initial + 4 background attempts
            fixture.fakeWorkflow.calls.size shouldBe 5
            fixture.scheduler.pendingClearances.size shouldBe 1
            succeed
          }
          .futureValue
      }

      "allow manual recovery via requestClearance after a transient background failure" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()

        // We only expect one tick promise to ever be requested since the map deduplicates
        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        // Queue up the sequence of events:
        // 1. Initial successful flag check (schedules task)
        fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))
        // 2. The async trigger fails (4 failures to exhaust the background retry loop)
        (1 to 4).foreach(_ => fixture.fakeWorkflow.addFailure("Transient network error"))
        // 3. A later manual retry or reconnection attempt
        fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

        fixture.scheduler
          .requestClearance(partyId, onboardingEffectiveAt)
          .valueOrFailShutdown("Initial onboarding request failed")
          .flatMap { _ =>
            // Initial task is scheduled
            fixture.scheduler.pendingClearances.size shouldBe 1
            verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(
              any[TraceContext]
            )

            // Fire the trigger, which will fail after exhausting retries
            loggerFactory.suppress(SuppressionRule.Level(Level.ERROR)) {
              tickPromise.success(())
              eventuallyAsync() {
                fixture.fakeWorkflow.calls.size shouldBe 5
              }.unwrap
            }
          }
          .flatMap { _ =>
            // The task remains in the map after the background failure
            fixture.scheduler.pendingClearances.size shouldBe 1

            // Simulate manual recovery: admin hits the endpoint, calling requestClearance
            fixture.scheduler
              .requestClearance(partyId, onboardingEffectiveAt)
              .valueOrFailShutdown("Recovery request failed")
          }
          .map { _ =>
            // Manual recovery should work: The workflow was successfully evaluated again
            fixture.fakeWorkflow.calls.size shouldBe 6

            // Task idempotency: The deduplication map prevented a redundant time tracker thread
            verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(
              any[TraceContext]
            )
            fixture.scheduler.pendingClearances.size shouldBe 1
            succeed
          }
          .futureValue
      }

      "retry the initial clearance request if maxInitialRetries > 0" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()
        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        // Queue up the sequence of events for the Fake Workflow:
        // 1. First attempt fails
        fixture.fakeWorkflow.addFailure("Transient network error 1")
        // 2. Second attempt fails
        fixture.fakeWorkflow.addFailure("Transient network error 2")
        // 3. Third attempt succeeds
        val successResponse = FlagSet(earliestClearanceTime)
        fixture.fakeWorkflow.addResponse(successResponse)

        // Execute with maxInitialRetries = 2.
        // This gives it exactly enough retries (1 initial + 2 retries = 3 attempts) to succeed.
        loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
          fixture.scheduler
            .requestClearance(
              partyId,
              onboardingEffectiveAt,
              maxInitialRetries = NonNegativeInt.tryCreate(2),
            )
            .valueOrFailShutdown("Request should have recovered via retries")
            .futureValue shouldBe successResponse
        }

        // Assert that the workflow was indeed called 3 times
        fixture.fakeWorkflow.calls.size shouldBe 3

        // Assert that the successful third attempt correctly scheduled the background task
        fixture.scheduler.pendingClearances.size shouldBe 1
        verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(any[TraceContext])
      }

      "retry the triggered clearance request and eventually succeed" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise = Promise[Unit]()

        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        // 1. Initial request evaluates the flag and schedules the background task
        fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

        // 2. The trigger fires. The first two background attempts fail due to transient errors.
        fixture.fakeWorkflow.addFailure("Transient network error 1")
        fixture.fakeWorkflow.addFailure("Transient network error 2")

        // 3. The third background attempt succeeds!
        val successResponse = FlagSet(earliestClearanceTime)
        fixture.fakeWorkflow.addResponse(successResponse)

        fixture.scheduler
          .requestClearance(partyId, onboardingEffectiveAt)
          .valueOrFailShutdown("Initial onboarding request failed")
          .flatMap { _ =>
            fixture.scheduler.pendingClearances.size shouldBe 1
            fixture.fakeWorkflow.calls.size shouldBe 1

            // Trigger the background task
            loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
              tickPromise.success(())

              eventuallyAsync() {
                // 1 initial + 3 background attempts (2 failed, 1 success) = 4 total calls
                fixture.fakeWorkflow.calls.size shouldBe 4
              }.unwrap
            }
          }
          .map { _ =>
            // The task remains pending until `observed` detects the effective transaction
            fixture.scheduler.pendingClearances.size shouldBe 1
            succeed
          }
          .futureValue
      }

    }

    "observing transactions" should {
      // Helper for creating "Replace" transactions (onboarding/clearance)
      def createTx(
          party: PartyId,
          participant: ParticipantId,
          onboarding: Boolean,
      ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
        val mapping = PartyToParticipant
          .tryCreate(
            partyId = party,
            threshold = PositiveInt.one,
            participants =
              Seq(HostingParticipant(participant, ParticipantPermission.Submission, onboarding)),
          )
        testData.makeSignedTx(mapping, isProposal = false)(testData.p1Key, testData.p2Key)
      }

      // Helper for creating "Remove" transactions (offboarding)
      def createRemoveTx(
          party: PartyId,
          participant: ParticipantId,
      ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
        val mapping = PartyToParticipant
          .tryCreate(
            partyId = party,
            threshold = PositiveInt.one,
            participants = Seq(
              HostingParticipant(participant, ParticipantPermission.Submission, onboarding = false)
            ),
          )
        // Create a Remove transaction
        testData.makeSignedTx(mapping, TopologyChangeOp.Remove, isProposal = false)(
          testData.p1Key,
          testData.p2Key,
        )
      }

      "remove a pending task if clearance is effective" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        fixture.scheduler.pendingClearances.put(task.partyId, task)
        fixture.scheduler.pendingClearances.size shouldBe 1

        val clearanceTx = createTx(partyId, participantId, onboarding = false)

        fixture.scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        fixture.scheduler.pendingClearances.size shouldBe 0
        succeed
      }

      "not remove a task if the onboarding flag is still true" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        fixture.scheduler.pendingClearances.put(task.partyId, task)
        val clearanceTx = createTx(partyId, participantId, onboarding = true)

        fixture.scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        fixture.scheduler.pendingClearances.size shouldBe 1
        succeed
      }

      "not remove a task if the participantId does not match" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        fixture.scheduler.pendingClearances.put(task.partyId, task)
        val otherParticipant = testData.p2Id
        val clearanceTx = createTx(partyId, otherParticipant, onboarding = false)

        fixture.scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        fixture.scheduler.pendingClearances.size shouldBe 1
        succeed
      }

      "only remove the task matching the partyId" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val otherParty = testData.party2
        val taskOtherParty = task.copy(partyId = otherParty)

        fixture.scheduler.pendingClearances.put(task.partyId, task)
        fixture.scheduler.pendingClearances.put(taskOtherParty.partyId, taskOtherParty)
        fixture.scheduler.pendingClearances.size shouldBe 2

        val clearanceTx = createTx(partyId, participantId, onboarding = false)

        fixture.scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        fixture.scheduler.pendingClearances.size shouldBe 1
        fixture.scheduler.pendingClearances.get(task.partyId) shouldBe None
        fixture.scheduler.pendingClearances.get(taskOtherParty.partyId) shouldBe Some(
          taskOtherParty
        )
      }

      /** "onboard, offboard, onboard" the same party in a sequence:
        *
        *   1. Onboard (1): First, onboard a party and verify that a clearance task is successfully
        *      scheduled.
        *   1. Offboard: Then simulate the party being offboarded by feeding a `Remove` topology
        *      transaction into the `observed` method. – Assert that this action correctly removes
        *      the pending task.
        *   1. Onboard (2): Finally, onboard the same party again.
        *   1. Verification: It checks that a *new* clearance task is successfully scheduled,
        *      proving that the stale task from step 1 was properly cleaned up.
        *
        * The goal is to assert that the implementation prevents a stale clearance task from
        * blocking future onboarding.
        *
        * The original implementation had this issue: If a party is onboarded (scheduling task 1),
        * then offboarded (which wasn't clearing the task), and then onboarded again (scheduling
        * task 2), the stale task 1 would remain. This would cause the scheduler to ignore task 2,
        * and the party's flag would never be cleared.
        */
      "remove a pending task on offboarding to allow subsequent onboarding" in {
        val fixture = new PartyOnboardingClearanceSchedulerTestFixture
        val tickPromise1 = Promise[Unit]()
        val tickPromise2 = Promise[Unit]()

        when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise1.future), Some(tickPromise2.future))

        val flagSet = FlagSet(earliestClearanceTime)
        fixture.fakeWorkflow.addResponse(flagSet) // For first request
        fixture.fakeWorkflow.addResponse(flagSet) // For second request

        // 1. Onboard (1)
        fixture.scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("First onboarding request failed")
          .futureValue

        fixture.scheduler.pendingClearances.size shouldBe 1
        verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(any[TraceContext])

        // 2. Offboard
        val removeTx = createRemoveTx(partyId, participantId)
        fixture.scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(removeTx),
          )
          .futureValueUS

        fixture.scheduler.pendingClearances.size shouldBe 0

        // 3. Onboard (2)
        fixture.scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("Second onboarding request failed")
          .futureValue

        fixture.scheduler.pendingClearances.size shouldBe 1
        verify(fixture.mockTimeTracker, times(2)).awaitTick(any[CantonTimestamp])(any[TraceContext])

        succeed
      }
    }
  }
}
