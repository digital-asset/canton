// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.DigestProcessorManagerTest.TestDigestProcessor
import com.digitalasset.canton.participant.commitment.DigestProcessorState.{Started, Stopped}
import com.digitalasset.canton.participant.commitment.DigestProcessorTestBase.PromiseKillSwitch
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext}
import org.apache.pekko.stream.KillSwitch
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
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
      val fixture = new Fixture()
      import fixture.*

      get() shouldBe empty

      // reinitialization successfully starts when there is no prior digest processor running
      mgr.startRunningDigestProcessor().futureValueUS

      // start reinitialization for lsid1 and run basic checks
      def startReinitializationAndCheck() = {
        val oldProc = get().value
        // starting another reinitialization processor stops the one currently running
        mgr.startReinitializationDigestProcessor().futureValueUS
        // once the future returned by startReinitialization is completed, the killswitch should've been triggered as well.
        oldProc.stateInternal shouldBe Stopped(TryUtil.unit)

        // the new processor should have a higher id
        val newProc = get().value
        newProc should not be oldProc

        // start another running digest processor, which interrupts the reinitialization
        mgr.startRunningDigestProcessor().futureValueUS
        get().value.stateInternal should matchPattern { case Started(_, _) => }
      }

      // start the reinitialization multiple times
      startReinitializationAndCheck()
      startReinitializationAndCheck()
      startReinitializationAndCheck()
    }

    "starting a reinitialization processor does not stop the current reinitialization processor" in {
      val fixture = new Fixture()
      import fixture.*

      get() shouldBe empty

      // reinitialization successfully starts when there is no prior digest processor running
      mgr.startReinitializationDigestProcessor().futureValueUS

      val proc1 = get().value

      mgr.startReinitializationDigestProcessor().futureValueUS

      get().value shouldBe proc1
      proc1.stateInternal should matchPattern { case Started(_, _) => }
    }

    "be able to start a processor if the previous processor has terminated" in {
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

      startAndTerminate(() => mgr.startReinitializationDigestProcessor())
      startAndTerminate(() => mgr.startReinitializationDigestProcessor())
      startAndTerminate(() => mgr.startRunningDigestProcessor())
      startAndTerminate(() => mgr.startRunningDigestProcessor())
      startAndTerminate(() => mgr.startReinitializationDigestProcessor())
    }
  }

  class Fixture() {
    val mgr = new DigestProcessorManager(
      DefaultTestIdentities.synchronizerId,
      new TestDigestProcessorFactory(loggerFactory, timeouts),
      mock[TickSignaller],
      exitOnFatalFailures = exitOnFatal,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )
    def get(): Option[BaseDigestProcessor] =
      mgr.currentProcessor
  }

  class TestDigestProcessorFactory(loggerFactory: NamedLoggerFactory, timeouts: ProcessingTimeout)
      extends DigestProcessorFactory {

    override def createRunningDigestProcessor(
        synchronizerId: SynchronizerId,
        tickSignaller: TickSignaller,
    )(implicit
        traceContext: TraceContext
    ): BaseDigestProcessor = new TestDigestProcessor(
      synchronizerId,
      isReinitializingProcessor = false,
      timeouts,
      loggerFactory,
    ) {
      override protected def startPipelineInternal()(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
        val ks = new PromiseKillSwitch()
        FutureUnlessShutdown.pure((ks, ks.promise.future))
      }
    }

    override def createReinitializingDigestProcessor(synchronizerId: SynchronizerId)(implicit
        traceContext: TraceContext
    ): BaseDigestProcessor = new TestDigestProcessor(
      synchronizerId,
      isReinitializingProcessor = true,
      timeouts,
      loggerFactory,
    ) {
      override protected def startPipelineInternal()(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
        val ks = new PromiseKillSwitch()
        FutureUnlessShutdown.pure((ks, ks.promise.future))
      }
    }
  }
}

object DigestProcessorManagerTest {

  class TestDigestProcessor(
      override val synchronizerId: SynchronizerId,
      override val isReinitializingProcessor: Boolean,
      override protected val timeouts: ProcessingTimeout,
      override protected val loggerFactory: NamedLoggerFactory,
  )(implicit override protected val executionContext: ExecutionContext)
      extends BaseDigestProcessor {

    override protected def startPipelineInternal()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {
      val ks = new PromiseKillSwitch()
      FutureUnlessShutdown.pure((ks, ks.promise.future))
    }
  }

}
