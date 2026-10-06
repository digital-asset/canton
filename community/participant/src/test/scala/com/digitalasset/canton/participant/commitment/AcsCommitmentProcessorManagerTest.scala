// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, ShutdownFailedException}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext, SynchronizerAlias}
import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitch
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class AcsCommitmentProcessorManagerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem {

  private val synchronizerId = DefaultTestIdentities.synchronizerId
  private val synchronizerAlias = SynchronizerAlias.tryCreate("synchronizer1")

  /** Matcher factory whose matcher stream has already terminated with the given result */
  private class TerminatedMatcherFactory(streamResult: Future[Done])
      extends ReceivedAcsCommitmentMatcherFactory {
    override def startMatcherPipeline(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: SynchronizerId,
        tickSignaller: TickSignaller,
    )(implicit
        traceContext: TraceContext
    ): (AcsCommitmentComponentHealthReporter, FutureUnlessShutdown[(KillSwitch, Future[Done])]) = {
      val health = AcsCommitmentComponentHealthReporter(
        s"matcher-$synchronizerId",
        ParticipantTestMetrics.synchronizer.commitments.matcherHealth,
        loggerFactory,
      )
      val killSwitch = new KillSwitch {
        override def shutdown(): Unit = ()
        override def abort(ex: Throwable): Unit = ()
      }
      (health, FutureUnlessShutdown.pure((killSwitch, streamResult)))
    }
  }

  private def createManager(streamResult: Future[Done]): AcsCommitmentProcessorManager = {
    val mgr = new AcsCommitmentProcessorManager(
      mock[DigestProcessorFactory],
      new TerminatedMatcherFactory(streamResult),
      _ => Some(synchronizerAlias),
      _ => ParticipantTestMetrics.synchronizer.commitments,
      exitOnFatalFailures = false,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )
    mgr.getOrCreate(synchronizerId).discard
    mgr
  }

  "AcsCommitmentProcessorManager" should {

    "close successfully if the matcher stream completed normally" in {
      val mgr = createManager(Future.successful(Done))
      mgr.close()
      mgr.isClosing shouldBe true
    }

    "close successfully if the matcher stream failed because the storage became passive" in {
      val mgr = createManager(
        Future.failed(PassiveInstanceException("Connection pool is not active"))
      )
      mgr.close()
      mgr.isClosing shouldBe true
    }

    "fail to close if the matcher stream failed for another reason" in {
      val exception = new RuntimeException("matcher failure")
      val mgr = createManager(Future.failed(exception))

      loggerFactory.assertThrowsAndLogs[ShutdownFailedException](
        mgr.close(),
        entry => {
          entry.warningMessage should include regex "ReceivedAcsCommitmentMatcher.* failed!"
          entry.throwable.value shouldBe exception
        },
      )
    }
  }
}
