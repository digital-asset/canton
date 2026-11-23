// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.concurrent.DirectExecutionContext.directExecutionContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SynchronizerSnapshotSyncCryptoApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.NoMediatorError
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestProcessorError,
  TestViewType,
}
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionSynchronizerTracker
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend.TrafficRegistration
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.concurrent.TrieMap

class ActiveMediatorGroupSelectionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext {

  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::participant")
  )
  private val psid = DefaultTestIdentities.physicalSynchronizerId

  private val med0 = MediatorId(UniqueIdentifier.tryFromProtoPrimitive("mediator0::default"))
  private val med1 = MediatorId(UniqueIdentifier.tryFromProtoPrimitive("mediator1::default"))

  private val group0Active =
    MediatorGroup(NonNegativeInt.zero, NonEmpty(Seq, med0), Seq(), PositiveInt.one)
  // Group 0 has been removed (or is inactive); only group 1 exists and is active.
  private val group1Active =
    MediatorGroup(NonNegativeInt.one, NonEmpty(Seq, med1), Seq(), PositiveInt.one)

  private class Steps(explicitGroup: Int, chosen: TrieMap[Int, MediatorGroupRecipient])
      extends TestProcessingSteps(
        pendingSubmissionMap = TrieMap[Int, Unit](),
        pendingRequestData = None,
      ) {
    override def explicitMediatorGroup(param: Int): Option[MediatorGroupIndex] =
      Some(MediatorGroupIndex.tryCreate(explicitGroup))

    override def createSubmission(
        submissionParam: Int,
        mediator: MediatorGroupRecipient,
        ephemeralState: SyncEphemeralState,
        recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
        generateMaxSequencingTime: CantonTimestamp => CantonTimestamp,
    )(implicit traceContext: TraceContext): EitherT[
      FutureUnlessShutdown,
      TestProcessingSteps.TestProcessingError,
      (Submission, PendingSubmissionData),
    ] = {
      // Record the mediator group chosen by ProtocolProcessor.chooseMediator and stop there.
      chosen.put(submissionParam, mediator)
      EitherT.leftT[FutureUnlessShutdown, (Submission, PendingSubmissionData)](
        TestProcessorError(
          NoMediatorError(CantonTimestamp.Epoch)
        ): TestProcessingSteps.TestProcessingError
      )
    }
  }

  /** Runs `submit` with the given explicit group index and returns the mediator group actually
    * handed to createSubmission, or the submit result when no mediator could be chosen.
    */
  private def submitWith(
      groups: Set[MediatorGroup],
      explicitGroup: Int,
  ): (Option[MediatorGroupRecipient], Either[TestProcessingSteps.TestProcessingError, Unit]) = {
    val crypto = TestingIdentityFactory(
      TestingTopology(mediatorGroups = groups),
      loggerFactory,
      TestSynchronizerParameters.defaultDynamic,
    ).forOwnerAndSynchronizer(participant, psid)
    val chosen = TrieMap[Int, MediatorGroupRecipient]()
    val steps = new Steps(explicitGroup, chosen)
    val sut =
      new ProtocolProcessor[Int, Unit, TestViewType, TestProcessingSteps.TestProcessingError](
        steps,
        mock[InFlightSubmissionSynchronizerTracker],
        mock[SyncEphemeralState],
        crypto,
        mock[SequencerClientSend],
        new WallClock(timeouts, loggerFactory),
        loggerFactory,
        FutureSupervisor.Noop,
        FlagCloseable.withCloseContext(logger, timeouts),
      )(directExecutionContext) {
        override def testingConfig: TestingConfigInternal = TestingConfigInternal()
        override def participantId: ParticipantId = participant
        override def timeouts: ProcessingTimeout =
          ActiveMediatorGroupSelectionTest.this.timeouts
        override protected def metricsContextForSubmissionParam(p: Int): MetricsContext =
          MetricsContext.Empty
        override protected def registerTrafficCost(p: Int, c: Long)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, RpcError, Option[TrafficRegistration]] =
          EitherT.rightT(None)
        override protected def releaseTrafficRegistration(r: TrafficRegistration)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit
      }
    val topo = crypto.currentSnapshotApproximation.futureValueUS.ipsSnapshot
    val res =
      try sut.submit(1, topo).onShutdown(fail("shutdown")).value.futureValue
      finally sut.close()
    (chosen.get(1), res.map(_ => ()))
  }

  "ProtocolProcessor.chooseMediator" should {
    "honour the explicit index when groups 0 and 1 are active" in {
      val (chosen, _) = submitWith(Set(group0Active, group1Active), explicitGroup = 0)
      chosen shouldBe Some(MediatorGroupRecipient(MediatorGroupIndex.zero))
    }

    "look up the group by its index, not by its position in the active groups" in {
      val (chosen, _) = submitWith(Set(group1Active), explicitGroup = 1)
      chosen shouldBe Some(MediatorGroupRecipient(MediatorGroupIndex.one))
    }

    "fail with NoMediatorError instead of rerouting when the explicit group is not active" in {
      val (chosen, res) = submitWith(Set(group1Active), explicitGroup = 0)
      chosen shouldBe None
      res.left.value shouldBe a[TestProcessorError]
      res.left.value.asInstanceOf[TestProcessorError].err shouldBe a[NoMediatorError]
    }
  }
}
