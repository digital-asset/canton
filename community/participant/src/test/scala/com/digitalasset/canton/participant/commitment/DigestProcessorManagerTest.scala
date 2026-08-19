// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.DigestProcessorManagerTest.{
  TestReinitializingDigestProcessor,
  TestRunningDigestProcessor,
}
import com.digitalasset.canton.participant.commitment.DigestProcessorState.{Started, Stopped}
import com.digitalasset.canton.participant.commitment.DigestProcessorTestBase.PromiseKillSwitch
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.metrics.{CommitmentMetrics, ParticipantTestMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, SynchronizerAlias}
import org.apache.pekko.stream.KillSwitch
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

@AcsCommitmentTest
class DigestProcessorManagerTest
    extends AnyWordSpec
    with DigestProcessorTestBase
    with HasExecutionContext
    with HasActorSystem {

  "DigestProcessorManager" should {

    "starting a running processor waits for the currently running processor to stop" in {
      val fixture = new Fixture()
      import fixture.*

      get() shouldBe empty

      mgr.startRunningDigestProcessor().futureValueUS
      val proc1 = get().value

      mgr.startRunningDigestProcessor().futureValueUS
      val proc2 = get().value

      proc1.completionFuture.futureValueUS
      proc1.stateInternal should matchPattern { case Stopped(Success(())) => }

      proc2 should not be proc1
    }

    "starting a reinitialization processor stops the currently running processor" in {
      val reinitTimepoint = tp(10)
      val fixture = new Fixture(
        reinitializingTimepoint = Some(reinitTimepoint),
        donePromise = () => Promise[Unit](),
      )
      import fixture.*

      get() shouldBe empty

      // Start initial running digest processor
      mgr.startRunningDigestProcessor().futureValueUS

      def startReinitializationAndCheck(): Unit = {
        val oldProc = get().value
        eventually() {
          oldProc.isStartingOrStarted shouldBe true
          oldProc shouldBe a[RunningDigestProcessor]
        }

        // Kicks off reinitialization and returns the configured target record time
        val reinitResult = mgr.startReinitializationDigestProcessor().futureValueUS
        reinitResult shouldBe Some(reinitTimepoint.recordTime)

        // Verify the old running processor was stopped
        eventually() {
          oldProc.stateInternal shouldBe Stopped(TryUtil.unit)
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
        reinitProc.asInstanceOf[TestReinitializingDigestProcessor].donePromise.success(())

        // Verify the manager automatically restarted a RunningDigestProcessor
        eventually() {
          val proc = get().value
          proc shouldBe a[RunningDigestProcessor]
          proc.stateInternal should matchPattern { case Started(_, _) => }
        }
      }

      startReinitializationAndCheck()
      startReinitializationAndCheck()
      startReinitializationAndCheck()
    }

    "starting a reinitialization processor does not stop the current reinitialization processor" in {
      val reinitDonePromise = Promise[Unit]()
      val reinitTimepoint = tp(10)

      val fixture = new Fixture(
        reinitializingTimepoint = Some(reinitTimepoint),
        donePromise = () => reinitDonePromise,
      )
      import fixture.*

      get() shouldBe empty

      // Start first reinitialization (returns target timestamp immediately while pipeline runs in background)
      val firstReinitializationTimestamp = mgr
        .startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)
        .futureValueUS

      // Verify proc1 is active and in progress
      val proc1 = get().value
      proc1.isStartingOrStarted shouldBe true

      // Start second reinitialization while first is still in progress
      val secondReinitializationTimestamp = mgr
        .startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)
        .futureValueUS

      // Verify proc1 was NOT replaced or stopped.
      // The second attempt to start a reinitialization processor joined the ongoing reinitialization.
      get().value shouldBe proc1
      proc1.isStartingOrStarted shouldBe true

      // Both returned the exact target timestamp of the active run
      firstReinitializationTimestamp shouldBe Some(reinitTimepoint.recordTime)
      secondReinitializationTimestamp shouldBe Some(reinitTimepoint.recordTime)

      // Complete the background pipeline and verify clean shutdown
      reinitDonePromise.success(())

      eventually() {
        proc1.completionFuture.futureValueUS
        proc1.stateInternal should matchPattern { case Stopped(Success(())) => }
      }
    }

    "once the reinitialization processor completes with default 'runningDigestProcessorShouldStartAfter', a running digest processing should be started" in {
      val reinitTimepoint = tp(100)
      val fixture = new Fixture(reinitializingTimepoint = Some(reinitTimepoint))
      import fixture.*

      get() shouldBe empty

      // Start running digest processor
      mgr.startRunningDigestProcessor().futureValueUS
      val initialRunningProc = get().value

      eventually() {
        initialRunningProc.isStartingOrStarted shouldBe true
      }

      // Start and complete reinitialization (stops active running processor)
      mgr.startReinitializationDigestProcessor().futureValueUS shouldBe Some(
        reinitTimepoint.recordTime
      )

      // Verify running digest processor automatically restarts
      eventually() {
        val proc2 = get().value
        proc2 shouldBe a[RunningDigestProcessor]
      }
    }

    "not start a new running digest processor after reinitialization, if `runningDigestProcessorShouldStartAfter` is false" in {
      val reinitDonePromise = Promise[Unit]()
      val reinitTimepoint = tp(100)

      val fixture = new Fixture(
        reinitializingTimepoint = Some(reinitTimepoint),
        donePromise = () => reinitDonePromise,
      )
      import fixture.*

      get() shouldBe empty

      // Kick off reinitialization with running digest processor auto-restart disabled
      mgr
        .startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)
        .futureValueUS shouldBe Some(reinitTimepoint.recordTime)

      // Verify reinitialization processor is active in memory
      val reinitProc = get().value
      reinitProc.isStartingOrStarted shouldBe true

      // Complete the reinitialization pipeline
      reinitDonePromise.success(())

      // Verify pipeline completed, stopped, and no RunningDigestProcessor was started
      eventually() {
        reinitProc.completionFuture.futureValueUS
        reinitProc.stateInternal should matchPattern { case Stopped(Success(())) => }
        get().value shouldBe reinitProc // Ref remains on testReinitDp, not replaced
      }
    }

    "be able to start a processor if the previous processor has terminated" in {
      val fixture = new Fixture(
        donePromise = () => Promise[Unit]()
      )
      import fixture.*

      get() shouldBe empty

      def terminatePipeline(processor: BaseDigestProcessor): Unit =
        processor.stateInternal match {
          case Started(ks, completionFuture) =>
            ks.shutdown()
            completionFuture.futureValueUS
          case Stopped(_) => ()
          case unexpectedState => fail(s"unexpected processor state $unexpectedState")
        }

      def startAndTerminate(startProcessor: () => FutureUnlessShutdown[Unit]): Unit = {
        val oldProcO = get()

        startProcessor().futureValueUS

        val proc = get().value

        // Shutdown the killswitch and await completion
        terminatePipeline(proc)

        eventually() {
          proc.stateInternal shouldBe Stopped(TryUtil.unit)
        }

        oldProcO.foreach(_ should not be proc)
      }

      startAndTerminate(() =>
        mgr
          .startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)
          .map(_ => ())
      )
      startAndTerminate(() =>
        mgr
          .startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)
          .map(_ => ())
      )
      startAndTerminate(() => mgr.startRunningDigestProcessor())
      startAndTerminate(() => mgr.startRunningDigestProcessor())
      startAndTerminate(() =>
        mgr
          .startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)
          .map(_ => ())
      )
    }
  }

  class Fixture(
      reinitializingTimepoint: Option[Timepoint] = None,
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
          reinitializingTimepoint = reinitializingTimepoint,
          donePromise = donePromise(),
        ),
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

    def get(): Option[BaseDigestProcessor] =
      mgr.currentProcessor
  }

  class TestDigestProcessorFactory(
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      makeReinitProcessor: () => TestReinitializingDigestProcessor,
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
      new TestRunningDigestProcessor(synchronizerId, timeouts, loggerFactory)
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
      override val reinitializingTimepoint: Option[Timepoint] = None,
      val donePromise: Promise[Unit] = Promise.successful(()),
  )(implicit override protected val executionContext: ExecutionContext)
      extends TestDigestProcessor(synchronizerId, timeouts, loggerFactory)
      with ReinitializingDigestProcessor {

    override protected def startPipelineInternal()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
      val ks = new PromiseKillSwitch()
      val completionF = Future.firstCompletedOf(Seq(ks.promise.future, donePromise.future))
      FutureUnlessShutdown.pure((ks, completionF))
    }

    override private[canton] def metrics: CommitmentMetrics =
      ParticipantTestMetrics.synchronizer.commitments

    override protected def acsDigestStore: AcsDigestStore = ???
  }

  class TestRunningDigestProcessor(
      synchronizerId: SynchronizerId,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit override protected val executionContext: ExecutionContext)
      extends TestDigestProcessor(synchronizerId, timeouts, loggerFactory)
      with RunningDigestProcessor {

    override protected def startPipelineInternal()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
      val ks = new PromiseKillSwitch()
      FutureUnlessShutdown.pure((ks, ks.promise.future))
    }

    override private[canton] def metrics: CommitmentMetrics =
      ParticipantTestMetrics.synchronizer.commitments

    override protected def acsDigestStore: AcsDigestStore = ???
  }

}
