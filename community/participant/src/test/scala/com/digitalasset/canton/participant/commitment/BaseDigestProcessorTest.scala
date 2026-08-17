// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  CheckpointToBeWritten,
  ContractChange,
  ContractChangeBatch,
}
import com.digitalasset.canton.participant.commitment.DigestProcessorState.{
  Started,
  Starting,
  Stopped,
  Stopping,
}
import com.digitalasset.canton.participant.commitment.DigestProcessorTestBase.PromiseKillSwitch
import com.digitalasset.canton.participant.metrics.{CommitmentMetrics, TestCommitmentMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  CheckpointType,
  allCheckpointsFilter,
}
import com.digitalasset.canton.participant.store.{AcsDigestStore, AcsDigestTestBase}
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ReassignmentCounter}
import org.apache.pekko.stream.KillSwitch
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

@AcsCommitmentTest
class BaseDigestProcessorTest
    extends AnyWordSpec
    with AcsDigestTestBase
    with BaseTest
    with HasExecutionContext {

  "BaseDigestProcessor" should {
    "not allow double start" in {
      // promise to delay the startup of the pipeline
      val startupPromise = PromiseUnlessShutdown.unsupervised[(KillSwitch, Future[Unit])]()
      val proc = new TestDigestProcessor(startupPromise.futureUS)

      val startingFuture = proc.start() // to be awaited only at the end of the test
      proc.stateInternal should matchPattern {
        case Starting(startingComplete) if !startingComplete.isCompleted =>
      }

      // starting again returns immediately, since the actual startup process is in progress
      proc.start().futureValueUS

      val exception = new RuntimeException("failure-on-startup")
      startupPromise.failure(exception)

      startingFuture.failed.futureValueUS shouldBe exception
    }

    "propagate failures while starting the pipeline" in {
      val exception = new RuntimeException("fail-on-startup")
      val proc = new TestDigestProcessor(FutureUnlessShutdown.failed(exception))

      proc.start().failed.futureValueUS shouldBe exception
      proc.completionFuture.failed.futureValueUS shouldBe exception

      proc.stateInternal shouldBe Stopped(Failure(exception))

      // stopping again should result in the same failure
      proc.stop().failed.futureValueUS shouldBe exception
    }

    "allow stopping when there are no errors while starting" in {
      // promise to control the startup of the pipeline
      val startupPromise = PromiseUnlessShutdown.unsupervised[(KillSwitch, Future[Unit])]()
      val proc = new TestDigestProcessor(startupPromise.futureUS)

      val startingF = proc.start() // to be awaited only at the end of the test
      proc.stateInternal should matchPattern { case Starting(_) => }
      val completionFutureAfterStart = proc.completionFuture

      // stop while the processor is still starting
      val stoppingF = proc.stop() // do not wait for the stopping to complete
      proc.stateInternal should matchPattern { case Stopping(_) => }
      val completionFutureAfterStop = proc.completionFuture

      // signal a successful startup
      val promiseKillSwitch = new PromiseKillSwitch()
      // the pipeline termination happens immediately and successfully after triggering the killswitch
      startupPromise.outcome_((promiseKillSwitch, promiseKillSwitch.promise.future))

      // the various futures should be completed
      startingF.futureValueUS
      completionFutureAfterStart.futureValueUS

      stoppingF.futureValueUS
      completionFutureAfterStop.futureValueUS

      // the killswitch must have been triggered
      promiseKillSwitch.promise.future.futureValue

      proc.stateInternal shouldBe Stopped(TryUtil.unit)
    }

    "allow stopping when there are errors while starting" in {
      // promise to delay the startup of the pipeline
      val startupPromise = PromiseUnlessShutdown.unsupervised[(KillSwitch, Future[Unit])]()
      val proc = new TestDigestProcessor(startupPromise.futureUS)

      val startingF = proc.start() // to be awaited only at the end of the test
      proc.stateInternal should matchPattern { case Starting(_) => }
      val completionFutureAfterStart = proc.completionFuture

      // stop while the processor is still starting up
      val stoppingF = proc.stop()
      proc.stateInternal should matchPattern { case Stopping(_) => }
      val completionFutureAfterStop = proc.completionFuture

      // signal a failed startup
      val startupException = new RuntimeException("failure-while-starting")
      startupPromise.failure(startupException)

      // the various futures should be completed
      startingF.failed.futureValueUS shouldBe startupException
      completionFutureAfterStart.failed.futureValueUS shouldBe startupException

      stoppingF.failed.futureValueUS shouldBe startupException
      completionFutureAfterStop.failed.futureValueUS shouldBe startupException

      proc.stateInternal shouldBe Stopped(Failure(startupException))
    }

    "propagate failures while running" in {
      // promise to delay the startup of the pipeline
      val pipelineCompletion = Promise[Unit]()
      val killSwitch = new PromiseKillSwitch()
      val proc =
        new TestDigestProcessor(FutureUnlessShutdown.pure((killSwitch, pipelineCompletion.future)))

      proc.start().futureValueUS
      proc.stateInternal should matchPattern { case Started(_, _) => }
      val completionFutureAfterStarted = proc.completionFuture

      val runningFailure = new RuntimeException("failure-while-running")

      // signal a successful startup
      logger.info(s"completing pipeline with $runningFailure")
      pipelineCompletion.failure(runningFailure)

      // the various futures should be completed
      completionFutureAfterStarted.failed.futureValueUS shouldBe runningFailure

      eventually() {
        proc.stateInternal shouldBe Stopped(Failure(runningFailure))
      }

      proc.completionFuture.failed.futureValueUS shouldBe runningFailure
    }

    "propagate failures while terminating the pipeline" in {
      // promise to delay the startup of the pipeline
      val pipelineCompletion = Promise[Unit]()
      val killSwitch = new PromiseKillSwitch()
      val proc =
        new TestDigestProcessor(FutureUnlessShutdown.pure((killSwitch, pipelineCompletion.future)))

      proc.start().futureValueUS
      proc.stateInternal should matchPattern { case Started(_, _) => }
      val completionFutureAfterStarted = proc.completionFuture

      // stop while the processor is still starting up
      val stoppingF = proc.stop()
      // killswitch was triggered
      killSwitch.promise.future.futureValue
      proc.stateInternal should matchPattern { case Stopping(_) => }
      val completionFutureAfterStop = proc.completionFuture

      // signal a successful startup
      val stoppingException = new RuntimeException("failure-while-starting")
      logger.info(s"completing pipeline with $stoppingException")
      pipelineCompletion.failure(stoppingException)

      // the various futures should be completed
      stoppingF.failed.futureValueUS shouldBe stoppingException
      completionFutureAfterStarted.failed.futureValueUS shouldBe stoppingException
      completionFutureAfterStop.failed.futureValueUS shouldBe stoppingException

      proc.stateInternal shouldBe Stopped(Failure(stoppingException))
    }

    "write checkpoint successfully via writeCheckpointFUS" in {
      val pipelineCompletion = Promise[Unit]()
      val killSwitch = new PromiseKillSwitch()
      val proc =
        new TestDigestProcessor(FutureUnlessShutdown.pure((killSwitch, pipelineCompletion.future)))
      val timepoint = tp(1)
      val cpToBeWritten =
        CheckpointToBeWritten(timepoint, CheckpointType.MaxEventsWithoutCheckpoint)

      proc.writeCheckpoint(cpToBeWritten).futureValueUS

      // Verify that it was actually written to the store
      val latest = proc.acsDigestStore
        .latestCheckpointUpTo(Offset.MaxValue, allCheckpointsFilter)
        .futureValueUS
      latest.value.timepoint shouldBe timepoint
      latest.value.checkpointType shouldBe CheckpointType.MaxEventsWithoutCheckpoint

      proc.metrics.checkpointWatermark.getValue shouldBe timepoint.recordTime.toMicros
    }

    "check consistency of correct contract batches" in {
      noException should be thrownBy
        ContractChangeBatch.tryCreate(
          Map(
            party1 -> Set(participant1),
            party2 -> Set(participant1, participant2),
            party3 -> Set(participant1, participant2),
          ),
          ContractChange(
            stakeholders = Set(party1, party2, party3),
            locallyHostedStakeholders = Seq(party1, party2, party3),
            cid = contractId1,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
          ContractChange(
            stakeholders = Set(party1, party3),
            locallyHostedStakeholders = Seq(party1, party3),
            cid = contractId2,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
          ContractChange(
            stakeholders = Set(party1, party2),
            locallyHostedStakeholders = Seq(party1, party2),
            cid = contractId2,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
        )
    }

    "check consistency of inconcistent contract batches" in {
      loggerFactory.assertThrowsAndLogs[IllegalArgumentException](
        // party3 is not in the party hostings map
        ContractChangeBatch.tryCreate(
          Map(
            party1 -> Set(participant1),
            party2 -> Set(participant1, participant2),
          ),
          ContractChange(
            stakeholders = Set(party1, party2, party3),
            locallyHostedStakeholders = Seq(party1, party2, party3),
            cid = contractId1,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
          ContractChange(
            stakeholders = Set(party1, party3),
            locallyHostedStakeholders = Seq(party1, party3),
            cid = contractId2,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
        ),
        _.throwable.value.getMessage should include(
          "Not all stakeholders are hosted or not all hosted parties are stakeholders"
        ),
      )

      loggerFactory.assertThrowsAndLogs[IllegalArgumentException](
        // party4 in the party hostings map is not a stakeholder in the contract changes
        ContractChangeBatch.tryCreate(
          Map(
            party1 -> Set(participant1),
            party2 -> Set(participant1, participant2),
            party3 -> Set(participant1, participant2),
            party4 -> Set(participant1),
          ),
          ContractChange(
            stakeholders = Set(party1, party2, party3),
            locallyHostedStakeholders = Seq(party1, party2, party3),
            cid = contractId1,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
          ContractChange(
            stakeholders = Set(party1, party3),
            locallyHostedStakeholders = Seq(party1, party3),
            cid = contractId2,
            rc = ReassignmentCounter.Genesis,
            isActivation = true,
          ),
        ),
        _.throwable.value.getMessage should include(
          "Not all stakeholders are hosted or not all hosted parties are stakeholders"
        ),
      )

    }

  }

  class TestDigestProcessor(
      startupResult: FutureUnlessShutdown[(KillSwitch, Future[Unit])]
  ) extends BaseDigestProcessor {
    override implicit protected val executionContext: ExecutionContext =
      BaseDigestProcessorTest.this.parallelExecutionContext

    override protected def timeouts: ProcessingTimeout = BaseDigestProcessorTest.this.timeouts

    override def synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId

    override protected def loggerFactory: NamedLoggerFactory =
      BaseDigestProcessorTest.this.loggerFactory

    override val acsDigestStore: AcsDigestStore =
      mkInMemoryDigestStore()(executionContext)

    override private[canton] val metrics: CommitmentMetrics = TestCommitmentMetrics()

    override protected def startPipelineInternal()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] =
      startupResult
  }

}
