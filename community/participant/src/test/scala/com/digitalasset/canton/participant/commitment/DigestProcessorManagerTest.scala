// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
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

import java.util.concurrent.atomic.AtomicReference
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

    // TODO(#33422) - right now reinitializing is awaiting for the result, address this in a follow up PR
    "starting a reinitialization processor stops the currently running processor" ignore {
      val fixture = new Fixture()
      import fixture.*

      get() shouldBe empty

      // Start initial running digest processor
      mgr.startRunningDigestProcessor().futureValueUS

      def startReinitializationAndCheck() = {
        val oldProc = get().value
        val donePromise = Promise[Unit]()

        setNextReinitDonePromise(donePromise)

        val reinitF = mgr.startReinitializationDigestProcessor()

        eventually() {
          oldProc.stateInternal shouldBe Stopped(TryUtil.unit)
        }

        val newProc = get().value
        newProc should not be oldProc
        newProc shouldBe a[ReinitializingDigestProcessor]

        donePromise.success(())
        reinitF.futureValueUS shouldBe a[Right[?, ?]]

        // Verify the running digest processor automatically resumed after reinitialization
        eventually() {
          get().value.stateInternal should matchPattern { case Started(_, _) => }
        }
      }

      startReinitializationAndCheck()
      startReinitializationAndCheck()
      startReinitializationAndCheck()
    }

    "starting a reinitialization processor does not stop the current reinitialization processor" ignore {
      val statusPromise =
        PromiseUnlessShutdown.unsupervised[Either[String, Option[CantonTimestamp]]]()
      val testReinitDp = new TestReinitializingDigestProcessor(
        DefaultTestIdentities.synchronizerId,
        timeouts,
        loggerFactory,
      )

      val fixture = new Fixture(initialReinitProcessor = Some(testReinitDp))
      import fixture.*

      get() shouldBe empty

      // 1. Start first reinitialization (pauses waiting on statusPromise)
      val f1 =
        mgr.startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)

      // Verify proc1 is active
      val proc1 = eventually() {
        val proc = get().value
        proc shouldBe testReinitDp
        proc.isStartingOrStarted shouldBe true
        proc
      }

      // 2. Start second reinitialization while first is still in progress
      val f2 =
        mgr.startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)

      // 3. Verify proc1 was NOT replaced or stopped
      get().value shouldBe proc1
      proc1.isStartingOrStarted shouldBe true

      // 4. Complete promise to unblock both futures
      statusPromise.outcome_(Right(None))

      f1.futureValueUS shouldBe Right(None)
      f2.futureValueUS shouldBe Right(None)
    }

    "once the reinitialization processor completes, the previous running digest processing should continue" in {
      val fixture = new Fixture()
      import fixture.*

      get() shouldBe empty

      // 0. Start running digest processor
      mgr.startRunningDigestProcessor().futureValueUS
      val initialRunningProc = get().value

      eventually() {
        initialRunningProc.isStartingOrStarted shouldBe true
        initialRunningProc.stateInternal should matchPattern { case Started(_, _) =>
          ()
        }
      }

      // 1. Start and complete reinitialization (stops active running processor and pauses waiting on statusPromise)
      mgr.startReinitializationDigestProcessor().futureValueUS shouldBe Right(None)

      // 2. Verify running digest processor automatically restarts
      eventually() {
        val proc2 = get().value
        proc2 shouldBe a[RunningDigestProcessor]
      }
    }

    // TODO(#33422) - right now reinitializing is awaiting for the result, address this in a follow up PR
    "reinitialize, prior to any running digest run, and not start a new running digest processor" ignore {
      val statusPromise =
        PromiseUnlessShutdown.unsupervised[Either[String, Option[CantonTimestamp]]]()
      val testReinitDp = new TestReinitializingDigestProcessor(
        DefaultTestIdentities.synchronizerId,
        timeouts,
        loggerFactory,
      )

      val fixture = new Fixture(initialReinitProcessor = Some(testReinitDp))
      import fixture.*

      get() shouldBe empty

      // 1. Start first reinitialization (pauses waiting on statusPromise)
      val f1 =
        mgr.startReinitializationDigestProcessor(runningDigestProcessorShouldStartAfter = false)

      // Verify proc1 is active
      val proc1 = eventually() {
        val proc = get().value
        proc shouldBe testReinitDp
        proc.isStartingOrStarted shouldBe true
        proc
      }

      // 2. Complete status promise
      statusPromise.outcome_(Right(None))
      f1.futureValueUS shouldBe Right(None)

      // 3. Verify no auto-restart occurred and processor ref remains unchanged
      eventually() {
        val proc2 = get().value
        proc2 shouldBe proc1
      }
    }

    // TODO(#33422) - right now reinitializing is awaiting for the result, address this in a follow up PR
    "be able to start a processor if the previous processor has terminated" ignore {
      val fixture = new Fixture()
      import fixture.*

      get() shouldBe empty

      def terminatePipeline(processor: BaseDigestProcessor) =
        processor.stateInternal match {
          case Started(ks, completionFuture) =>
            ks.shutdown()
            completionFuture.futureValueUS
          case unexpectedState => fail(s"unexpected processor state $unexpectedState")
        }

      def startAndTerminate(startProcessor: () => FutureUnlessShutdown[Unit]): Unit = {
        val oldProcO = get()
        // start and terminate a running digest processor
        startProcessor().futureValueUS

        val proc = get().value

        // now stop the pipeline
        terminatePipeline(proc)

        // wait for the completion of the pipeline
        eventually() {
          proc.stateInternal shouldBe Stopped(TryUtil.unit)
        }

        oldProcO.foreach(_ should not be proc)
      }

      startAndTerminate(() => mgr.startReinitializationDigestProcessor().map(_ => ()))
      startAndTerminate(() => mgr.startReinitializationDigestProcessor().map(_ => ()))
      startAndTerminate(() => mgr.startRunningDigestProcessor())
      startAndTerminate(() => mgr.startRunningDigestProcessor())
      startAndTerminate(() => mgr.startReinitializationDigestProcessor().map(_ => ()))
    }
  }

  class Fixture(initialReinitProcessor: Option[ReinitializingDigestProcessor] = None) {
    val factory = new TestDigestProcessorFactory(loggerFactory, timeouts, initialReinitProcessor)

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

    def setNextReinitDonePromise(donePromise: Promise[Unit]): TestReinitializingDigestProcessor = {
      val proc = new TestReinitializingDigestProcessor(
        DefaultTestIdentities.synchronizerId,
        timeouts,
        loggerFactory,
        donePromise = donePromise,
      )
      factory.setNextReinitProcessor(proc)
      proc
    }
  }

  class TestDigestProcessorFactory(
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      initialReinitProcessor: Option[ReinitializingDigestProcessor] = None,
  )(implicit val executionContext: ExecutionContext)
      extends DigestProcessorFactory {

    private val nextReinitRef =
      new AtomicReference[Option[ReinitializingDigestProcessor]](initialReinitProcessor)

    def setNextReinitProcessor(proc: ReinitializingDigestProcessor): Unit =
      nextReinitRef.set(Some(proc))

    override def createReinitializingDigestProcessor(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: SynchronizerId,
    )(implicit traceContext: TraceContext): ReinitializingDigestProcessor =
      nextReinitRef
        .getAndSet(None)
        .getOrElse(
          new TestReinitializingDigestProcessor(synchronizerId, timeouts, loggerFactory)
        )

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
      FutureUnlessShutdown.pure((ks, donePromise.future))
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
