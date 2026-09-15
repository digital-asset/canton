// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  ShutdownFailedException,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.participant.commitment.DigestProcessorManagerTest.{
  TestReinitializingDigestProcessor,
  TestRunningDigestProcessor,
}
import com.digitalasset.canton.participant.commitment.DigestProcessorState.Stopped
import com.digitalasset.canton.participant.commitment.DigestProcessorTestBase.PromiseKillSwitch
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.metrics.{CommitmentMetrics, ParticipantTestMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, SynchronizerAlias}
import org.apache.pekko.stream.KillSwitch
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class DigestProcessorManagerTest
    extends AnyWordSpec
    with DigestProcessorTestBase
    with HasExecutionContext
    with HasActorSystem {

  "DigestProcessorManager" should {

    "starting a running processor on top of a running digest processor does nothing" in {
      val fixture =
        new Fixture(reinitializingTimepoint = tp(10))
      import fixture.*

      get() shouldBe empty

      mgr.startRunningDigestProcessor().futureValueUS
      val proc1 = get().value

      mgr.startRunningDigestProcessor().futureValueUS
      always() {
        val proc2 = get().value
        proc2 shouldBe proc1
      }
      mgr.close()
    }

    "not start a new running digest processor if the current running digest processor stops with a failure" in {
      val fixture = new Fixture(reinitializingTimepoint = tp(10))
      import fixture.*

      get() shouldBe empty

      mgr.startRunningDigestProcessor().futureValueUS
      val proc1 = get().value.asInstanceOf[TestRunningDigestProcessor]

      val exception = new RuntimeException("expected failure")
      proc1.promiseKillSwitch.abort(exception)

      always() {
        val proc = get().value
        proc shouldBe proc1
      }

      loggerFactory.assertThrowsAndLogs[ShutdownFailedException](
        mgr.close(),
        entry => {
          entry.warningMessage should include regex ("current digest processor.* failed!")
          entry.throwable.value shouldBe exception
        },
      )
    }

    "not start a new running digest processor if the current running digest processor stops with a failure during startup" in {
      val fixture = new Fixture(reinitializingTimepoint = tp(10), immediatePipelineStartUp = false)
      import fixture.*

      get() shouldBe empty

      mgr.startRunningDigestProcessorAsync()
      val proc1 = eventually() {
        get().value.asInstanceOf[TestRunningDigestProcessor]
      }

      val failure = Failure(new RuntimeException("expected startup failure"))
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.ERROR))(
        {
          proc1.startingPromise.complete(failure)
          // explicitly return unit, so that assertEventuallyLogsSeq doesn't detect the promise
          // as a (failed) future and subsequently skip the log assertions
          ()
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              entry => {
                entry.errorMessage should include("Failed to start digest processor")
              },
              "logged startup failure",
            ),
            (
              entry => {
                entry.errorMessage should include("Failed to start running digest processor")
              },
              "logged async startup failure",
            ),
          )
        ),
      )

      eventually() {
        val proc = get().value
        proc shouldBe proc1
        proc.stateInternal shouldBe Stopped(failure, failure)
      }
      always() {
        get().value shouldBe proc1
      }

      loggerFactory.assertThrowsAndLogs[ShutdownFailedException](
        mgr.close(),
        entry => {
          entry.warningMessage should include regex ("current digest processor.* failed!")
          entry.throwable.value shouldBe failure.exception
        },
      )
    }

    "start a new running digest processor if the current running digest processor is shut down orderly" in {
      val fixture = new Fixture(reinitializingTimepoint = tp(10))
      import fixture.*

      get() shouldBe empty

      mgr.startRunningDigestProcessor().futureValueUS
      val proc1 = get().value.asInstanceOf[TestRunningDigestProcessor]

      proc1.promiseKillSwitch.shutdown()

      val proc2 = eventually() {
        val proc = get().value
        proc should not be proc1
        proc shouldBe a[RunningDigestProcessor]
        proc
      }

      proc2.stop().futureValueUS

      mgr.close()
    }

    "start a new running digest processor if the current running digest processor is shut down during startup" in {
      val fixture = new Fixture(reinitializingTimepoint = tp(10), immediatePipelineStartUp = false)
      import fixture.*

      get() shouldBe empty

      mgr.startRunningDigestProcessorAsync()
      val proc1 = eventually() {
        get().value.asInstanceOf[TestRunningDigestProcessor]
      }

      proc1.startingPromise.shutdown_()

      val proc2 = eventually() {
        val proc = get().value
        proc should not be proc1
        proc shouldBe a[TestRunningDigestProcessor]
        proc.asInstanceOf[TestRunningDigestProcessor]
      }

      // stop the new processor to not hang during shutdown
      val exception = new RuntimeException("expected failure")

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          proc2.startingPromise.failure(exception)
          a[ShutdownFailedException] should be thrownBy (mgr.close())
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              {
                _.warningMessage should include regex ("Closing.*current digest processor.* failed!")
              },
              "Error reported during shutdown",
            ),
            (
              entry => {
                entry.errorMessage should include("Failed to start digest processor")
                entry.throwable.value shouldBe exception
              },
              "processor startup error",
            ),
          )
        ),
      )

    }

    "starting a reinitialization processor stops the current running digest processor" in {
      val reinitTimepoint = tp(10)
      val donePromise = Promise[Unit]()
      val fixture = new Fixture(
        reinitializingTimepoint = reinitTimepoint,
        donePromise = () => donePromise,
      )
      import fixture.*

      get() shouldBe empty

      // Start initial running digest processor
      mgr.startRunningDigestProcessor().futureValueUS

      val oldProc = get().value
      eventually() {
        oldProc.isStartingOrStarted shouldBe true
        oldProc shouldBe a[RunningDigestProcessor]
      }

      // Kicks off reinitialization and returns the configured target record time
      val reinitResult = mgr.startReinitializationDigestProcessor().futureValueUS
      reinitResult shouldBe reinitTimepoint.recordTime

      // Verify the old running processor was stopped
      eventually() {
        oldProc.stateInternal shouldBe Stopped.success
      }

      // Retrieve the newly created reinitialization processor directly from the manager
      val reinitProc = eventually() {
        val proc = get().value
        proc should not be oldProc
        proc shouldBe a[ReinitializingDigestProcessor]
        proc.isStartingOrStarted shouldBe true
        proc
      }

      // Complete the reinitialization pipeline
      donePromise.success(())
      reinitProc.completionFuture.futureValueUS
    }

    "starting a reinitialization processor does not stop the current reinitialization processor" in {
      val reinitDonePromise = Promise[Unit]()
      val reinitTimepoint = tp(10)

      val fixture = new Fixture(
        reinitializingTimepoint = reinitTimepoint,
        donePromise = () => reinitDonePromise,
      )
      import fixture.*

      get() shouldBe empty

      // Start first reinitialization (returns target timestamp immediately while pipeline runs in background)
      val firstReinitializationTimestamp = mgr
        .startReinitializationDigestProcessor()
        .futureValueUS

      // Verify proc1 is active and in progress
      val proc1 = get().value
      proc1.isStartingOrStarted shouldBe true

      // Start second reinitialization while first is still in progress
      val secondReinitializationTimestamp = mgr
        .startReinitializationDigestProcessor()
        .futureValueUS

      // Verify proc1 was NOT replaced or stopped.
      // The second attempt to start a reinitialization processor joined the ongoing reinitialization.
      get().value shouldBe proc1
      proc1.isStartingOrStarted shouldBe true

      // Both returned the exact target timestamp of the active run
      firstReinitializationTimestamp shouldBe reinitTimepoint.recordTime
      secondReinitializationTimestamp shouldBe reinitTimepoint.recordTime

      // Complete the background pipeline and verify clean shutdown
      reinitDonePromise.success(())

      eventually() {
        proc1.completionFuture.futureValueUS
        proc1.stateInternal should matchPattern { case Stopped(Success(()), Success(Outcome(()))) =>
        }
      }
    }

    def runningDigestProcessorAfterReinitialization(
        explicitlyTryToStartRunningDigestProcessor: Boolean
    ) = {
      val reinitTimepoint = tp(100)
      val donePromise = Promise[Unit]()
      val fixture =
        new Fixture(reinitializingTimepoint = reinitTimepoint, donePromise = () => donePromise)
      import fixture.*

      get() shouldBe empty

      // Start running digest processor
      mgr.startRunningDigestProcessor().futureValueUS
      val initialRunningProc = get().value

      eventually() {
        initialRunningProc.isStartingOrStarted shouldBe true
      }

      // Start and complete reinitialization (stops active running processor)
      mgr.startReinitializationDigestProcessor().futureValueUS shouldBe
        reinitTimepoint.recordTime

      val reinitProc = get().value
      reinitProc shouldBe a[ReinitializingDigestProcessor]

      if (explicitlyTryToStartRunningDigestProcessor)
        mgr.startRunningDigestProcessor().futureValueUS

      // the reinitialization is still going on
      get().value shouldBe reinitProc

      donePromise.success(())

      // Verify that a RunningDigestProcessor is automatically started
      eventually() {
        val proc2 = get().value
        proc2 shouldBe a[RunningDigestProcessor]
      }
    }

    "starting a running digest processor while reinitialization is ongoing does nothing" in {
      runningDigestProcessorAfterReinitialization(explicitlyTryToStartRunningDigestProcessor = true)
    }

    "a running digest processor is automatically started after reinitialization" in {
      runningDigestProcessorAfterReinitialization(explicitlyTryToStartRunningDigestProcessor =
        false
      )
    }
  }

  class Fixture(
      reinitializingTimepoint: Timepoint,
      immediatePipelineStartUp: Boolean = true,
      donePromise: () => Promise[Unit] = () => Promise.successful(()),
  ) {
    val factory = new TestDigestProcessorFactory(
      loggerFactory,
      timeouts,
      () =>
        new TestReinitializingDigestProcessor(
          DefaultTestIdentities.synchronizerId,
          timeouts,
          loggerFactory,
          reinitializingTimepoint,
          donePromise = donePromise(),
          immediatePipelineStartUp = immediatePipelineStartUp,
        ),
      immediatePipelineStartUp = immediatePipelineStartUp,
    )

    val mgr = new DigestProcessorManager(
      SynchronizerAlias.tryCreate("synchronizer1"),
      DefaultTestIdentities.synchronizerId,
      factory,
      mock[TickSignaller],
      exitOnFatalFailures = exitOnFatal,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

    def get(): Option[DigestProcessor] =
      mgr.currentProcessor
  }

  class TestDigestProcessorFactory(
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      makeReinitProcessor: () => TestReinitializingDigestProcessor,
      immediatePipelineStartUp: Boolean,
  )(implicit val executionContext: ExecutionContext)
      extends DigestProcessorFactory {

    override def createReinitializingDigestProcessor(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: SynchronizerId,
    )(implicit traceContext: TraceContext): ReinitializingDigestProcessor =
      makeReinitProcessor()

    override def createRunningDigestProcessor(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: SynchronizerId,
        tickSignaller: TickSignaller,
    )(implicit traceContext: TraceContext): RunningDigestProcessor =
      new TestRunningDigestProcessor(
        synchronizerId,
        timeouts,
        loggerFactory,
        immediatePipelineStartUp = immediatePipelineStartUp,
      )

    override def needsReinitialization(
        synchronizerId: SynchronizerId
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.pure(false)
  }
}

object DigestProcessorManagerTest {

  abstract class TestDigestProcessor(
      override val synchronizerId: SynchronizerId,
      override protected val timeouts: ProcessingTimeout,
      override protected val loggerFactory: NamedLoggerFactory,
  ) extends BaseDigestProcessor

  class TestReinitializingDigestProcessor(
      synchronizerId: SynchronizerId,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      override val reinitializingTimepoint: Timepoint,
      val donePromise: Promise[Unit] = Promise.successful(()),
      immediatePipelineStartUp: Boolean,
  )(implicit override protected val executionContext: ExecutionContext)
      extends TestDigestProcessor(synchronizerId, timeouts, loggerFactory)
      with ReinitializingDigestProcessor {

    override def thisParticipantId: ParticipantId = ???

    val startingPromise: PromiseUnlessShutdown[Unit] = PromiseUnlessShutdown.unsupervised[Unit]()

    override protected def startPipelineInternal()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
      if (immediatePipelineStartUp) startingPromise.outcome_(())
      startingPromise.futureUS.map { _ =>
        val ks = new PromiseKillSwitch()
        val completionF = Future.firstCompletedOf(Seq(ks.promise.future, donePromise.future))
        (ks, completionF)
      }
    }

    override private[canton] def metrics: CommitmentMetrics =
      ParticipantTestMetrics.synchronizer.commitments

    override protected def acsDigestStore: AcsDigestStore = ???
  }

  class TestRunningDigestProcessor(
      synchronizerId: SynchronizerId,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      immediatePipelineStartUp: Boolean,
  )(implicit override protected val executionContext: ExecutionContext)
      extends TestDigestProcessor(synchronizerId, timeouts, loggerFactory)
      with RunningDigestProcessor {

    override def thisParticipantId: ParticipantId = ???

    val promiseKillSwitch = new PromiseKillSwitch()
    val startingPromise: PromiseUnlessShutdown[Unit] = PromiseUnlessShutdown.unsupervised[Unit]()

    override protected def startPipelineInternal()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
      if (immediatePipelineStartUp) startingPromise.outcome_(())
      startingPromise.futureUS.map(_ => (promiseKillSwitch, promiseKillSwitch.promise.future))
    }

    override private[canton] def metrics: CommitmentMetrics =
      ParticipantTestMetrics.synchronizer.commitments

    override protected def acsDigestStore: AcsDigestStore = ???
  }

}
