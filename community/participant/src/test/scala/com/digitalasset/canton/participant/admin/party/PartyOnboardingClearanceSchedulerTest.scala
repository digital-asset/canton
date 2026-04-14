// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTopologyWorkflow.AuthorizeClearanceError
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.store.PendingOperation
import com.digitalasset.canton.store.memory.InMemoryPendingOperationStore
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshotLoader,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreTestData}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksAsyncWordSpec,
  SequencerCounter,
}
import org.mockito.ArgumentMatchers.eq as isEq
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

class PartyOnboardingClearanceSchedulerTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
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
    private val responses =
      new ConcurrentLinkedQueue[Either[AuthorizeClearanceError, PartyOnboardingFlagStatus]]()

    // Records the arguments passed to `authorizeClearingOnboardingFlag` for assertion.
    val calls = new ConcurrentLinkedQueue[(PartyId, EffectiveTime)]

    def addResponse(response: PartyOnboardingFlagStatus): Unit =
      responses.add(Right(response))

    def addFailure(error: AuthorizeClearanceError): Unit =
      responses.add(Left(error))

    override def authorizeClearingOnboardingFlag(
        partyId: PartyId,
        targetParticipantId: ParticipantId,
        onboardingEffectiveAt: EffectiveTime,
        context: PartyReplicationTopologyWorkflow.SynchronizerTopologyContext,
        requestId: Option[Hash] = None,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, AuthorizeClearanceError, PartyOnboardingFlagStatus] = {
      calls.add((partyId, onboardingEffectiveAt))

      val result = Option(responses.poll()).getOrElse(
        Left(
          AuthorizeClearanceError.ProposeError(
            "FakePartyReplicationTopologyWorkflow has no more responses"
          )
        )
      )

      EitherT.fromEither[FutureUnlessShutdown](result)
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
    val mockTimeTracker = mock[SynchronizerTimeTracker]
    val mockTopologyClient = mock[SynchronizerTopologyClientWithInit]
    val mockTopologyManager = mock[SynchronizerTopologyManager]
    val mockTopologyStore = mock[TopologyStore[SynchronizerStore]]

    // We must mock TopologySnapshotLoader instead of TopologySnapshot because
    // SynchronizerTopologyClientWithInit.currentSnapshotApproximation expects the Loader type.
    val mockSnapshot = mock[TopologySnapshotLoader]
    val fakeWorkflow = new FakePartyReplicationTopologyWorkflow(loggerFactory)

    // Use the actual in-memory store instead of a mock
    val inMemoryStore =
      new InMemoryPendingOperationStore[OnboardingClearanceOperation, SynchronizerId](
        OnboardingClearanceOperation,
        loggerFactory,
      )

    // Create a valid, real operation to avoid Mockito issues with final classes
    val rpv = OnboardingClearanceOperation
      .protocolVersionRepresentativeFor(ProtoVersion(30))
      .valueOr(err => fail(s"Failed to get RPV: $err"))

    val realOperation = OnboardingClearanceOperation(Some(onboardingEffectiveAt))(rpv)

    val pendingOp = PendingOperation(
      name = OnboardingClearanceOperation.operationName,
      key = OnboardingClearanceOperation.operationKey(partyId),
      operation = realOperation,
      synchronizer = psid.logical,
    )

    // Setup topology mocks to return "No LSU Announced" by default to keep existing tests green.
    // Explicitly typed as Option.empty to satisfy the invariant FutureUnlessShutdown Mockito macro.
    when(mockTopologyClient.currentSnapshotApproximation(any[TraceContext]))
      .thenReturn(FutureUnlessShutdown.pure(mockSnapshot))
    when(mockSnapshot.announcedLsu()(any[TraceContext]))
      .thenReturn(FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)]))

    // Build the resolved context for the active synchronizer mock
    val mockSynchronizerContext = new PartyReplicationTopologyWorkflow.SynchronizerTopologyContext {
      override def topologyClient = mockTopologyClient
      override def topologyManager = mockTopologyManager
      override def topologyStore = mockTopologyStore
      override def timeTracker = mockTimeTracker
    }

    // Build the outer static context
    val workflowContext = new PartyReplicationTopologyWorkflow.TopologyWorkflowContext {
      override def psid: PhysicalSynchronizerId = PartyOnboardingClearanceSchedulerTest.this.psid
      override def workflow: PartyReplicationTopologyWorkflow = fakeWorkflow
      override def synchronizerContext
          : Option[PartyReplicationTopologyWorkflow.SynchronizerTopologyContext] =
        Some(mockSynchronizerContext)
    }

    // The scheduler instance under test.
    val scheduler =
      new OnboardingClearanceScheduler(
        participantId,
        workflowContext,
        loggerFactory,
        inMemoryStore, // Inject the active in-memory store
        timeouts,
        retryInitialDelay = 1.millisecond,
        retryMaxDelay = 5.milliseconds,
      )
  }

  /** Factory method to safely and asynchronously build and initialize the fixture.
    */
  private def createFixture(): Future[PartyOnboardingClearanceSchedulerTestFixture] = {
    val fixture = new PartyOnboardingClearanceSchedulerTestFixture
    fixture.inMemoryStore
      .insert(fixture.pendingOp)
      .value
      .failOnShutdown
      .map(_ => fixture)
  }

  "PartyOnboardingClearanceScheduler" when {

    "requestClearance is called synchronously" should {

      "fail if the synchronizer provider returns None" in {
        createFixture().flatMap { fixture =>
          // Create a context that returns None for the synchronizer context
          val failingWorkflowContext =
            new PartyReplicationTopologyWorkflow.TopologyWorkflowContext {
              override def psid: PhysicalSynchronizerId =
                PartyOnboardingClearanceSchedulerTest.this.psid
              override def workflow: PartyReplicationTopologyWorkflow = fixture.fakeWorkflow
              override def synchronizerContext
                  : Option[PartyReplicationTopologyWorkflow.SynchronizerTopologyContext] = None
            }

          val scheduler = new OnboardingClearanceScheduler(
            participantId,
            failingWorkflowContext,
            loggerFactory,
            fixture.inMemoryStore,
            timeouts,
          )

          scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .leftOrFailShutdown("Request should have failed as synchronizer provider returned None")
            .map { error =>
              error should include("Synchronizer connection is not yet available")
            }
        }
      }

      "abort clearance and remove task if no pending clearance record is found (PV >= 35)" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
        createFixture().flatMap { fixture =>
          // Clear the fake store to simulate a missing record in the DB
          fixture.inMemoryStore
            .delete(
              psid.logical,
              OnboardingClearanceOperation.operationKey(partyId),
              OnboardingClearanceOperation.operationName,
            )
            .failOnShutdown
            .flatMap { _ =>
              // Put a dummy task in the map to ensure it actually gets actively removed
              fixture.scheduler.pendingClearances.put(partyId, task)

              fixture.scheduler
                .requestClearance(partyId, onboardingEffectiveAt)
                .value
                .failOnShutdown
                .map { result =>
                  result shouldBe Left(
                    OnboardingClearanceScheduler.ClearanceAttemptError.NoPendingClearanceRecord.message
                  )

                  // Workflow should NOT be called since it was aborted early
                  fixture.fakeWorkflow.calls shouldBe empty

                  // The task should be cleanly removed from the map
                  fixture.scheduler.pendingClearances.get(partyId) shouldBe None
                }
            }
        }
      }

      "return FlagSet with the upgrade time's immediate successor and schedule a background task if an LSU is announced" in {
        createFixture().flatMap { fixture =>
          val successor = SynchronizerSuccessor(psid, CantonTimestamp.now())
          // Define a specific future upgrade time to assert against
          val upgradeTime = EffectiveTime(CantonTimestamp.now().plusSeconds(60))
          val expectedSafeTime = upgradeTime.value.immediateSuccessor

          val tickPromise = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Mock the topology to simulate an active LSU.
          when(fixture.mockSnapshot.announcedLsu()(any[TraceContext]))
            .thenReturn(
              FutureUnlessShutdown.pure(Some((successor, upgradeTime)))
            )

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("Request should have succeeded with FlagSet")
            .map { result =>
              // It should return FlagSet with the immediate successor of the LSU upgrade time
              result shouldBe FlagSet(expectedSafeTime)

              // Ensure the workflow is never called if an LSU is active
              fixture.fakeWorkflow.calls shouldBe empty

              // Ensure the background task was successfully scheduled for the new time
              fixture.scheduler.pendingClearances.size shouldBe 1
              verify(fixture.mockTimeTracker, times(1)).awaitTick(isEq(expectedSafeTime))(
                any[TraceContext]
              )
              succeed
            }
        }
      }

      "update an existing scheduled task if polled again and the safe time has changed (e.g. LSU cancelled)" in {
        createFixture().flatMap { fixture =>
          val successor = SynchronizerSuccessor(psid, CantonTimestamp.now())

          // 1st time: LSU is active.
          val lsuUpgradeTime = EffectiveTime(CantonTimestamp.now().plusSeconds(60))
          val lsuSafeTime = lsuUpgradeTime.value.immediateSuccessor

          // 2nd time: LSU is cancelled, workflow is called, and a new (earlier) safe time is returned.
          val newSafeTime = CantonTimestamp.now().plusSeconds(10)

          // Mock topology to return LSU active, then LSU cancelled
          when(fixture.mockSnapshot.announcedLsu()(any[TraceContext]))
            .thenReturn(FutureUnlessShutdown.pure(Some((successor, lsuUpgradeTime))))
            .andThen(
              FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)])
            )

          // Mock workflow to return a completely new safe time when finally called
          fixture.fakeWorkflow.addResponse(FlagSet(newSafeTime))

          val tickPromise1 = Promise[Unit]()
          val tickPromise2 = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise1.future), Some(tickPromise2.future))

          // Request 1: Polling during LSU
          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("First request failed")
            .flatMap { res1 =>
              res1 shouldBe FlagSet(lsuSafeTime)

              // Ensure the task was scheduled for the LSU safe time
              fixture.scheduler
                .pendingClearances(partyId)
                .earliestClearanceTime shouldBe lsuSafeTime
              verify(fixture.mockTimeTracker, times(1)).awaitTick(isEq(lsuSafeTime))(
                any[TraceContext]
              )

              // Request 2: Polling after LSU is cancelled
              fixture.scheduler
                .requestClearance(partyId, onboardingEffectiveAt)
                .valueOrFailShutdown("Second request failed")
                .map { res2 =>
                  res2 shouldBe FlagSet(newSafeTime)

                  // Ensure the task was UPDATED to the new safe time, and a new trigger was scheduled
                  fixture.scheduler.pendingClearances.size shouldBe 1
                  fixture.scheduler
                    .pendingClearances(partyId)
                    .earliestClearanceTime shouldBe newSafeTime
                  verify(fixture.mockTimeTracker, times(1)).awaitTick(isEq(newSafeTime))(
                    any[TraceContext]
                  )
                  succeed
                }
            }
        }
      }

      "not schedule a task if the flag is already FlagNotSet" in {
        createFixture().flatMap { fixture =>
          fixture.fakeWorkflow.addResponse(FlagNotSet)

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
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
        }
      }

      "schedule a task only once for the same party" in {
        createFixture().flatMap { fixture =>
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
            .failOnShutdown
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
        }
      }

      "handle concurrent requests correctly" in {
        createFixture().flatMap { fixture =>
          val tickPromise = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          val flagSet = FlagSet(earliestClearanceTime)
          (1 to 10).foreach(_ => fixture.fakeWorkflow.addResponse(flagSet))

          val allCalls = Future
            .traverse((1 to 10).toList) { _ =>
              fixture.scheduler
                .requestClearance(partyId, onboardingEffectiveAt)
                .value
                .failOnShutdown
            }

          allCalls.map { _ =>
            fixture.scheduler.pendingClearances.size shouldBe 1
            verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(
              any[TraceContext]
            )
            fixture.fakeWorkflow.calls.size shouldBe 10
            succeed
          }
        }
      }
    }

    "requestClearanceInBackground is called asynchronously" should {

      "evaluate the flag and schedule a task seamlessly in the background" in {
        createFixture().map { fixture =>
          val tickPromise = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          // Fire and "forget"
          fixture.scheduler.requestClearanceInBackground(partyId, onboardingEffectiveAt)

          // Wait for the background task to update the observable state safely
          eventually() {
            fixture.fakeWorkflow.calls.size shouldBe 1
            fixture.scheduler.pendingClearances.size shouldBe 1
          }

          // Verify the side-effect outside the previous eventually block (flake prevention)
          // Safely wait for the asynchronous mock invocation (up to 3 seconds)
          verify(fixture.mockTimeTracker, timeout(millis = 3000).times(1))
            .awaitTick(any[CantonTimestamp])(
              any[TraceContext]
            )
          succeed
        }
      }

      "treat an announced LSU as a transient error and silently retry until cancelled" in {
        createFixture().map { fixture =>
          val successor = SynchronizerSuccessor(psid, CantonTimestamp.now())

          // 1st attempt: LSU is announced (should backoff safely without spamming INFO/WARN logs)
          // 2nd attempt: LSU is cancelled (None) -> Proceeds to call workflow
          when(fixture.mockSnapshot.announcedLsu()(any[TraceContext]))
            .thenReturn(
              FutureUnlessShutdown.pure(Some((successor, EffectiveTime(CantonTimestamp.now()))))
            )
            .andThen(
              FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)])
            )

          // Once LSU cancels, the workflow is called and succeeds
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          val tickPromise = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Note: Because of our custom retry policy, we suppress DEBUG/TRACE,
          // ensuring the standard INFO logging is clean.
          loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
            fixture.scheduler.requestClearanceInBackground(partyId, onboardingEffectiveAt)

            eventually() {
              // Workflow is only called ONCE because the LSU block prevented it the first time
              fixture.fakeWorkflow.calls.size shouldBe 1
              fixture.scheduler.pendingClearances.size shouldBe 1
            }
          }
        }
      }

      "retry transient failures infinitely until it succeeds" in {
        createFixture().map { fixture =>
          val tickPromise = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Queue 2 transient failures, followed by a success
          fixture.fakeWorkflow.addFailure(
            AuthorizeClearanceError.ProposeError("Transient network error 1")
          )
          fixture.fakeWorkflow.addFailure(
            AuthorizeClearanceError.ProposeError("Transient network error 2")
          )
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
            fixture.scheduler.requestClearanceInBackground(partyId, onboardingEffectiveAt)

            eventually() {
              // It should have executed the workflow 3 times (2 fails + 1 success)
              fixture.fakeWorkflow.calls.size shouldBe 3
              // The task should be successfully scheduled after the loop breaks
              fixture.scheduler.pendingClearances.size shouldBe 1
            }
          }
        }
      }

      "abort the retry loop immediately on a terminal failure" in {
        createFixture().map { fixture =>
          // Queue a transient failure to trigger a retry, then a terminal failure
          fixture.fakeWorkflow.addFailure(
            AuthorizeClearanceError.ProposeError("Transient network error 1")
          )
          fixture.fakeWorkflow.addFailure(
            AuthorizeClearanceError.PartyNotHosted("Party is not hosted by target participant")
          )

          loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
            fixture.scheduler.requestClearanceInBackground(partyId, onboardingEffectiveAt)

            eventually() {
              // It should evaluate exactly twice, then safely abort the infinite loop
              fixture.fakeWorkflow.calls.size shouldBe 2
            }
          }

          // Ensure it did not schedule anything
          fixture.scheduler.pendingClearances.size shouldBe 0
          succeed
        }
      }

      "prevent redundant concurrent background loops using the idempotency lock" in {
        createFixture().map { fixture =>
          // Race flake/prevention: Set up the mock before releasing the pause!
          val tickPromise = Promise[Unit]()
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Create a promise to artificially pause the background task.
          // This ensures the lock remains held while we fire subsequent concurrent requests.
          val holdExecutionPromise = Promise[UnlessShutdown[TopologySnapshotLoader]]()

          when(fixture.mockTopologyClient.currentSnapshotApproximation(any[TraceContext]))
            .thenReturn(FutureUnlessShutdown(holdExecutionPromise.future))

          // Fire 5 concurrent background requests
          (1 to 5).foreach { _ =>
            fixture.scheduler.requestClearanceInBackground(partyId, onboardingEffectiveAt)
          }

          // Assert the lock is currently held
          fixture.scheduler.inFlightBackgroundRequests.contains(partyId) shouldBe true

          // Prevent flakiness: Queue the success response before releasing the pause!
          // This prevents the background task from waking up, finding an empty queue, and triggering a retry.
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))
          // Release the pause, allowing the single background task to complete
          holdExecutionPromise.success(UnlessShutdown.Outcome(fixture.mockSnapshot))

          eventually() {
            // Only one request should have made it through the lock to hit the workflow
            fixture.fakeWorkflow.calls.size shouldBe 1

            // The lock must be cleanly released after completion
            fixture.scheduler.inFlightBackgroundRequests.contains(partyId) shouldBe false
          }
        }
      }
    }

    "the scheduled trigger fires" should {

      "propose a transaction successfully in the background" in {
        createFixture().flatMap { fixture =>
          val tickPromise = Promise[Unit]()

          when(
            fixture.mockTimeTracker.awaitTick(
              isEq(
                task.earliestClearanceTime
              ) // NOTE: Trigger awaits the *exact* earliestClearanceTime now
            )(any[TraceContext])
          ).thenReturn(Some(tickPromise.future))

          // Initial setup response
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))
          // Trigger response
          fixture.fakeWorkflow.addResponse(FlagNotSet)

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("make the initial request")
            .flatMap { firstResult =>
              firstResult shouldBe FlagSet(earliestClearanceTime)
              fixture.scheduler.pendingClearances.size shouldBe 1
              fixture.fakeWorkflow.calls.size shouldBe 1

              // Release the timer tick
              tickPromise.success(())

              eventuallyAsync() {
                fixture.fakeWorkflow.calls.size shouldBe 2
              }.unwrap.map { _ =>
                fixture.fakeWorkflow.calls.poll() shouldBe (partyId, onboardingEffectiveAt)
                fixture.scheduler.pendingClearances.size shouldBe 1
                verify(fixture.mockTimeTracker, times(1))
                  .awaitTick(isEq(task.earliestClearanceTime))(any[TraceContext])
                succeed
              }
            }
        }
      }

      "gracefully ignore a stale trigger if the task time was updated" in {
        createFixture().flatMap { fixture =>
          val oldTime = earliestClearanceTime
          val newTime = CantonTimestamp.now().plusSeconds(60)

          val tickPromise1 = Promise[Unit]() // Stale trigger
          val tickPromise2 = Promise[Unit]() // New trigger

          when(fixture.mockTimeTracker.awaitTick(isEq(oldTime))(any[TraceContext]))
            .thenReturn(Some(tickPromise1.future))
          when(fixture.mockTimeTracker.awaitTick(isEq(newTime))(any[TraceContext]))
            .thenReturn(Some(tickPromise2.future))

          // Initial setup response creates the task at oldTime
          fixture.fakeWorkflow.addResponse(FlagSet(oldTime))

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("Initial request failed")
            .flatMap { _ =>
              // Now, a second request updates the task to newTime
              fixture.fakeWorkflow.addResponse(FlagSet(newTime))
              fixture.scheduler
                .requestClearance(partyId, onboardingEffectiveAt)
                .valueOrFailShutdown("Update request failed")
                .map { _ =>
                  // Fire the FIRST (stale) trigger
                  tickPromise1.success(())

                  // The stale trigger should be completely ignored by `triggerClearanceAttempt`.
                  // The workflow should NOT be called again, remaining at exactly 2 calls (from the 2 sync requests).
                  eventually() {
                    fixture.fakeWorkflow.calls.size shouldBe 2
                  }
                  succeed
                }
            }
        }
      }

      "abort the trigger retry loop and remove task if no pending clearance record is found" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
        createFixture().flatMap { fixture =>
          val tickPromise = Promise[Unit]()

          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Initial setup response
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("make the initial request")
            .flatMap { _ =>
              // Now, before the tick fires, simulate the record disappearing from the store
              fixture.inMemoryStore
                .delete(
                  psid.logical,
                  OnboardingClearanceOperation.operationKey(partyId),
                  OnboardingClearanceOperation.operationName,
                )
                .failOnShutdown
                .map { _ =>
                  loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
                    // Release the timer tick, allowing the background trigger to fire
                    tickPromise.success(())

                    eventually() {
                      // Task should be removed, and no further workflow calls made
                      fixture.scheduler.pendingClearances.get(partyId) shouldBe None
                      fixture.fakeWorkflow.calls.size shouldBe 1 // only the initial one
                    }
                  }
                  succeed
                }
            }
        }
      }

      "retry transient errors during the background trigger" in {
        createFixture().flatMap { fixture =>
          val tickPromise = Promise[Unit]()

          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Initial setup response
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          // Background trigger responses
          fixture.fakeWorkflow.addFailure(AuthorizeClearanceError.ProposeError("Transient error 1"))
          fixture.fakeWorkflow.addFailure(AuthorizeClearanceError.ProposeError("Transient error 2"))
          fixture.fakeWorkflow.addResponse(FlagNotSet)

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("make the initial request")
            .map { _ =>
              fixture.fakeWorkflow.calls.size shouldBe 1

              loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
                tickPromise.success(())

                eventually() {
                  // 1 initial + 3 trigger attempts (2 fail, 1 success) = 4
                  fixture.fakeWorkflow.calls.size shouldBe 4
                }
              }
              succeed
            }
        }
      }

      "treat an announced LSU as a transient error during the background trigger" in {
        createFixture().flatMap { fixture =>
          val tickPromise = Promise[Unit]()

          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Initial setup response (LSU is not active yet)
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("make the initial request")
            .map { _ =>
              // Initial setup succeeded
              fixture.fakeWorkflow.calls.size shouldBe 1

              // Now, simulate the LSU activating while the timer was ticking
              val successor = SynchronizerSuccessor(psid, CantonTimestamp.now())
              when(fixture.mockSnapshot.announcedLsu()(any[TraceContext]))
                .thenReturn(
                  FutureUnlessShutdown
                    .pure(Some((successor, EffectiveTime(CantonTimestamp.now()))))
                )
                .andThen(
                  FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)])
                )

              // Background trigger responses (once the LSU drops, it succeeds)
              fixture.fakeWorkflow.addResponse(FlagNotSet)

              loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
                // Fire the timer tick to kick off the background trigger
                tickPromise.success(())

                eventually() {
                  // 1 initial + 1 trigger attempt (LSU blocked the first, then succeeded)
                  fixture.fakeWorkflow.calls.size shouldBe 2
                }
              }
              succeed
            }
        }
      }

      "abort the trigger retry loop on a terminal error" in {
        createFixture().flatMap { fixture =>
          val tickPromise = Promise[Unit]()

          // We only expect one tick promise to ever be requested since the map deduplicates
          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise.future))

          // Initial setup response
          fixture.fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))

          // Background trigger responses
          fixture.fakeWorkflow.addFailure(AuthorizeClearanceError.ProposeError("Transient error 1"))
          fixture.fakeWorkflow.addFailure(
            AuthorizeClearanceError.PartyNotHosted("Party is not hosted by target participant")
          )
          // If the loop doesn't break, it would hit this and fail the assertion
          fixture.fakeWorkflow.addFailure(
            AuthorizeClearanceError.ProposeError("SHOULD NOT BE REACHED")
          )

          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("make the initial request")
            .map { _ =>
              loggerFactory.suppress(SuppressionRule.LevelAndAbove(Level.INFO)) {
                tickPromise.success(())

                eventually() {
                  // 1 initial + 2 trigger attempts (1 transient, 1 terminal) = 3
                  fixture.fakeWorkflow.calls.size shouldBe 3
                }
              }
              succeed
            }
        }
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
        createFixture().flatMap { fixture =>
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
            .failOnShutdown
            .map { _ =>
              fixture.scheduler.pendingClearances.size shouldBe 0
              succeed
            }
        }
      }

      "not remove a task if the onboarding flag is still true" in {
        createFixture().flatMap { fixture =>
          fixture.scheduler.pendingClearances.put(task.partyId, task)
          val clearanceTx = createTx(partyId, participantId, onboarding = true)

          fixture.scheduler
            .observed(
              SequencedTime(CantonTimestamp.now()),
              EffectiveTime(CantonTimestamp.now()),
              SequencerCounter.One,
              Seq(clearanceTx),
            )
            .failOnShutdown
            .map { _ =>
              fixture.scheduler.pendingClearances.size shouldBe 1
              succeed
            }
        }
      }

      "not remove a task if the participantId does not match" in {
        createFixture().flatMap { fixture =>
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
            .failOnShutdown
            .map { _ =>
              fixture.scheduler.pendingClearances.size shouldBe 1
              succeed
            }
        }
      }

      "only remove the task matching the partyId" in {
        createFixture().flatMap { fixture =>
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
            .failOnShutdown
            .map { _ =>
              fixture.scheduler.pendingClearances.size shouldBe 1
              fixture.scheduler.pendingClearances.get(task.partyId) shouldBe None
              fixture.scheduler.pendingClearances.get(taskOtherParty.partyId) shouldBe Some(
                taskOtherParty
              )
              succeed
            }
        }
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
        */
      "remove a pending task on offboarding to allow subsequent onboarding" in {
        createFixture().flatMap { fixture =>
          val tickPromise1 = Promise[Unit]()
          val tickPromise2 = Promise[Unit]()

          when(fixture.mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
            .thenReturn(Some(tickPromise1.future), Some(tickPromise2.future))

          val flagSet = FlagSet(earliestClearanceTime)
          fixture.fakeWorkflow.addResponse(flagSet) // For first request
          fixture.fakeWorkflow.addResponse(flagSet) // For second request

          // 1. Onboard (1)
          fixture.scheduler
            .requestClearance(partyId, onboardingEffectiveAt)
            .valueOrFailShutdown("First onboarding request failed")
            .flatMap { _ =>
              fixture.scheduler.pendingClearances.size shouldBe 1
              verify(fixture.mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(
                any[TraceContext]
              )

              // 2. Offboard
              val removeTx = createRemoveTx(partyId, participantId)
              fixture.scheduler
                .observed(
                  SequencedTime(CantonTimestamp.now()),
                  EffectiveTime(CantonTimestamp.now()),
                  SequencerCounter.One,
                  Seq(removeTx),
                )
                .failOnShutdown
                .flatMap { _ =>
                  fixture.scheduler.pendingClearances.size shouldBe 0

                  // 3. Onboard (2)
                  fixture.scheduler
                    .requestClearance(partyId, onboardingEffectiveAt)
                    .valueOrFailShutdown("Second onboarding request failed")
                    .map { _ =>
                      fixture.scheduler.pendingClearances.size shouldBe 1
                      verify(fixture.mockTimeTracker, times(2)).awaitTick(any[CantonTimestamp])(
                        any[TraceContext]
                      )
                      succeed
                    }
                }
            }
        }
      }
    }
  }
}
