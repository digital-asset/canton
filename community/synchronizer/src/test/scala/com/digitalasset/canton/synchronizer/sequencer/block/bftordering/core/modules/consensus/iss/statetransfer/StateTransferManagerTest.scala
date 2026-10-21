// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtocolVersionChecksAnyWordSpec
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Bootstrap.{
  bootstrapEpoch,
  bootstrapEpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferTestHelpers.{
  aBlockMetadata,
  aCommitCert,
  myId,
  otherId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  InMemoryUnitTestEpochStore,
  TimeoutManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  BaseIgnoringUnitTestEnv,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
  WorkflowId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
  OrderingMode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  OrderingTopologyInfo,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  StateTransferTimeout,
  UnverifiedStateTransferMessage,
  VerifiedStateTransferMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.TestBootstrapTopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
  fakeIgnoringModule,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.FiniteDuration

class StateTransferManagerTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with ProtocolVersionChecksAnyWordSpec {

  import StateTransferManagerTest.*

  class P2pNetworkOutRefExpectingSendToRandomAuthenticated
      extends ModuleRef[P2PNetworkOut.Message] {
    override def asyncSend(msg: P2PNetworkOut.Message)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): Unit =
      msg
        .asInstanceOf[P2PNetworkOut.SendToRandomAuthenticated]
        .onRecipientsDecision
        .foreach(
          _.apply(Seq(otherId))
        )
  }

  "StateTransferManager" should {
    "start catch-up and try to restart" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext

      val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      stateTransferManager.inStateTransfer shouldBe false

      val startEpoch = EpochNumber(7L)
      stateTransferManager.startCatchUp(
        aMembership,
        ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        latestCompletedEpoch = aBootstrapEpoch,
        startEpoch,
      )(abort = fail(_))
      context.runPipedMessages()

      stateTransferManager.inStateTransfer shouldBe true

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(startEpoch, from = myId)
        .fakeSign

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        possibleRecipients = Seq(otherId),
        numberOfTimes = 1,
      )

      // Try to start state transfer (with no effect) while another one is in progress.
      stateTransferManager.startCatchUp(
        aMembership,
        failingCryptoProvider,
        latestCompletedEpoch = aBootstrapEpoch,
        startEpoch,
      )(abort = fail(_))
      context.extractSelfMessages() shouldBe empty
    }

    "state transfer new epoch (start onboarding)" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext

      val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      stateTransferManager.inStateTransfer shouldBe false

      val startEpoch = EpochNumber(7L)
      stateTransferManager.stateTransferNewEpoch(
        startEpoch,
        aMembershipBeforeOnboarding,
        ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        nodesThatTimedOut = Seq.empty,
        None,
      )(abort = fail(_))
      context.runPipedMessages()

      stateTransferManager.inStateTransfer shouldBe true

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(startEpoch, from = myId)
        .fakeSign

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        possibleRecipients = Seq(otherId),
        numberOfTimes = 1,
      )
    }

    "schedule a retry and send block transfer request" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      // Initiate state transfer so that it's in progress.
      val latestCompletedEpoch = aBootstrapEpoch
      val startEpoch = EpochNumber(7L)
      stateTransferManager.startCatchUp(
        aMembership,
        ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        latestCompletedEpoch,
        startEpoch,
      )(abort = fail(_))
      context.runPipedMessages() shouldBe List()

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(startEpoch, from = myId)
        .fakeSign
      stateTransferManager.handleStateTransferMessage(
        StateTransferMessage
          .RetryBlockTransferRequest(blockTransferRequest, nodeThatTimedOut = Some(otherId)),
        aTopologyInfo,
        latestCompletedEpoch,
        aBootstrapEpoch.info,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue
      context.runPipedMessages()

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        possibleRecipients = Seq(otherId),
        numberOfTimes = 2, // +1 from start
      )
    }

    "send an empty block transfer response on a request for onboarding when the serving node is lagging behind" onlyRunLessThan ProtocolVersion.v37 in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      val latestCompletedEpochLocally = EpochStore.Epoch(
        aBootstrapEpoch.info, // the serving node is still at genesis
        lastBlockCommits = Seq.empty,
      )

      // Handle a block transfer request.
      stateTransferManager.handleStateTransferMessage(
        VerifiedStateTransferMessage(
          StateTransferMessage.BlockTransferRequest.create(
            epoch = EpochNumber.First,
            from = otherId,
          )
        ),
        aTopologyInfo,
        latestCompletedEpoch = latestCompletedEpochLocally,
        aBootstrapEpoch.info,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty
      context.runPipedMessages()

      // Should have sent a block transfer response with an empty commit certificate.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          eqTo(
            P2PNetworkOut.send(
              P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
                StateTransferMessage.BlockTransferResponse30
                  .create(commitCertificate = None, from = myId)
                  .fakeSign
              ),
              destinationBftNodeId = otherId,
            )
          )
        )(any[TraceContext], any[MetricsContext])
    }

    "send a non-empty block transfer response on a request for onboarding" onlyRunLessThan ProtocolVersion.v37 in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val epochStore = new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef,
          epochStore = epochStore,
        )

      // Store a block that will be sent by the serving node.
      val commitCert = aCommitCert()
      context.pipeToSelf(
        epochStore.addOrderedBlockAtomically(
          commitCert.prePrepare,
          commitCert.commits.map(Traced(_)),
        )
      )(
        _.map(_ => None).getOrElse(fail("Storing the pre-prepare failed"))
      )
      context.runPipedMessages() // store ordered block data

      val latestCompletedEpochLocally = EpochStore.Epoch(
        EpochInfo.forTesting(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommits = Seq.empty,
      )

      // Handle a block transfer request.
      stateTransferManager.handleStateTransferMessage(
        VerifiedStateTransferMessage(
          StateTransferMessage.BlockTransferRequest.create(
            epoch = EpochNumber.First,
            from = otherId,
          )
        ),
        aTopologyInfo,
        latestCompletedEpoch = latestCompletedEpochLocally,
        aBootstrapEpoch.info,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      context.runPipedMessages() // retrieve blocks
      context.runPipedMessages() // sign message

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty

      // Should have sent a block transfer response with a single commit certificate.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          eqTo(
            P2PNetworkOut.send(
              P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
                StateTransferMessage.BlockTransferResponse30
                  .create(Some(commitCert), from = myId)
                  .fakeSign
              ),
              destinationBftNodeId = otherId,
            )
          )
        )(any[TraceContext], any[MetricsContext])
    }

    "send block transfer responses carrying multiple commit certificates grouped by the configured " +
      "window size for protocol versions >= 37" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        implicit val context: ContextType = new ProgrammableUnitTestContext()

        val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
        val epochStore = new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv]
        val stateTransferManager =
          createStateTransferManager[ProgrammableUnitTestEnv](
            p2pNetworkOutModuleRef = p2pNetworkOutRef,
            epochStore = epochStore,
            consensusWindowSizeForStateTransferOfCommitCertificates = Some(PositiveInt.tryCreate(2)),
          )

        // Store 3 blocks in the epoch that will be sent by the serving node.
        val commitCerts = (0L to 2L).map { blockNumber =>
          aCommitCert(BlockMetadata.mk(EpochNumber.First, blockNumber))
        }
        commitCerts.foreach { commitCert =>
          context.pipeToSelf(
            epochStore.addOrderedBlockAtomically(
              commitCert.prePrepare,
              commitCert.commits.map(Traced(_)),
            )
          )(
            _.map(_ => None).getOrElse(fail("Storing the pre-prepare failed"))
          )
          context.runPipedMessages() // store ordered block data
        }

        val latestCompletedEpochLocally = EpochStore.Epoch(
          EpochInfo.forTesting(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 3),
          lastBlockCommits = Seq.empty,
        )

        stateTransferManager.handleStateTransferMessage(
          VerifiedStateTransferMessage(
            StateTransferMessage.BlockTransferRequest.create(
              epoch = EpochNumber.First,
              from = otherId,
            )
          ),
          aTopologyInfo,
          latestCompletedEpoch = latestCompletedEpochLocally,
          aBootstrapEpoch.info,
        )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

        context.runPipedMessages() // retrieve blocks
        context.runPipedMessages() // sign messages

        context.selfMessages shouldBe empty

        // With a window size of 2 and 3 commit certificates, 2 responses are expected: one with the first 2 certs
        //  and one with the remaining cert.
        verify(p2pNetworkOutRef, times(1))
          .asyncSend(
            eqTo(
              P2PNetworkOut.send(
                P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
                  StateTransferMessage.BlockTransferResponse31
                    .create(commitCerts.take(2), from = myId)
                    .fakeSign
                ),
                destinationBftNodeId = otherId,
              )
            )
          )(any[TraceContext], any[MetricsContext])
        verify(p2pNetworkOutRef, times(1))
          .asyncSend(
            eqTo(
              P2PNetworkOut.send(
                P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
                  StateTransferMessage.BlockTransferResponse31
                    .create(commitCerts.drop(2), from = myId)
                    .fakeSign
                ),
                destinationBftNodeId = otherId,
              )
            )
          )(any[TraceContext], any[MetricsContext])
      }

    "send an empty block transfer response reusing the retransmission response message for protocol " +
      "versions >= 37 when the serving node is lagging behind" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        implicit val context: ContextType = new ProgrammableUnitTestContext()

        val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
        val stateTransferManager =
          createStateTransferManager[ProgrammableUnitTestEnv](
            p2pNetworkOutModuleRef = p2pNetworkOutRef
          )

        val latestCompletedEpochLocally = EpochStore.Epoch(
          aBootstrapEpoch.info, // the serving node is still at genesis
          lastBlockCommits = Seq.empty,
        )

        stateTransferManager.handleStateTransferMessage(
          VerifiedStateTransferMessage(
            StateTransferMessage.BlockTransferRequest.create(
              epoch = EpochNumber.First,
              from = otherId,
            )
          ),
          aTopologyInfo,
          latestCompletedEpoch = latestCompletedEpochLocally,
          aBootstrapEpoch.info,
        )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

        context.selfMessages shouldBe empty
        context.runPipedMessages()

        // Should have sent a single empty block transfer response.
        verify(p2pNetworkOutRef, times(1))
          .asyncSend(
            eqTo(
              P2PNetworkOut.send(
                P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
                  StateTransferMessage.BlockTransferResponse31
                    .create(commitCertificates = Seq.empty, from = myId)
                    .fakeSign
                ),
                destinationBftNodeId = otherId,
              )
            )
          )(any[TraceContext], any[MetricsContext])
      }
  }

  "verify, store, send block to Output, and continue on response for onboarding" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext()

    val outputRef = mock[ModuleRef[Output.Message[ProgrammableUnitTestEnv]]]
    val timeoutManagerMock = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
      ProgrammableUnitTestEnv
    ], StateTransferTimeout, String]]
    val stateTransferManager =
      createStateTransferManager[ProgrammableUnitTestEnv](
        outputModuleRef = outputRef,
        p2pNetworkOutModuleRef = fakeIgnoringModule,
        maybeCustomTimeoutManager = Some(timeoutManagerMock),
      )

    // Initiate state transfer so that it's in progress.
    val latestCompletedEpochLocally = aBootstrapEpoch
    stateTransferManager.startCatchUp(
      aMembership,
      ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))
    context.runPipedMessages()

    val currentEpochInfo =
      EpochInfo(EpochNumber(1), BlockNumber(0), EpochLength(1), TestBootstrapTopologyActivationTime)
    val blockMetadata = aBlockMetadata
    val commitCert = aCommitCert(blockMetadata)

    // Handle a block transfer response with a single commit certificate.
    val blockTransferResponse =
      StateTransferMessage.BlockTransferResponse30.create(Some(commitCert), from = otherId)
    val topologyInfo = OrderingTopologyInfo(
      myId,
      currentTopology = aMembershipBeforeOnboarding.orderingTopology,
      currentCryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      currentLeaders = aMembershipBeforeOnboarding.leaders,
      currentBlacklistedNodes = Seq.empty,
      previousTopology = aMembershipBeforeOnboarding.orderingTopology,
      previousCryptoProvider = failingCryptoProvider,
      previousLeaders = aMembershipBeforeOnboarding.leaders,
      previousBlacklistedNodes = Seq.empty,
    )
    stateTransferManager.handleStateTransferMessage(
      VerifiedStateTransferMessage(blockTransferResponse),
      topologyInfo,
      latestCompletedEpochLocally,
      currentEpochInfo,
    )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

    // Verify the block.
    val blockVerifiedMessage = context.runPipedMessages()
    blockVerifiedMessage should contain only
      StateTransferMessage.BlockVerified(
        commitCert,
        currentEpochInfo,
        from = otherId,
        isForFutureEpoch = false,
      )
    stateTransferManager.handleStateTransferMessage(
      blockVerifiedMessage.headOption
        .getOrElse(fail("There should be just a single block verified message"))
        .asInstanceOf[StateTransferMessage.BlockVerified[ProgrammableUnitTestEnv]],
      topologyInfo,
      latestCompletedEpochLocally,
      currentEpochInfo,
    )(fail(_))

    verify(timeoutManagerMock, times(1)).cancelTimeoutIf(any[StateTransferTimeout => Boolean])(
      any[TraceContext]
    )

    // Store the block.
    val blockStoredMessage = context.runPipedMessages()
    blockStoredMessage should contain only StateTransferMessage.BlockStored(
      commitCert,
      currentEpochInfo,
      from = otherId,
    )

    val result = stateTransferManager.handleStateTransferMessage(
      blockStoredMessage.headOption
        .getOrElse(fail("There should be just a single block stored message"))
        .asInstanceOf[StateTransferMessage.BlockStored[ProgrammableUnitTestEnv]],
      topologyInfo,
      latestCompletedEpochLocally,
      currentEpochInfo,
    )(fail(_))

    result shouldBe StateTransferMessageResult.Continue

    // Should have sent an ordered block to the Output module.
    val prePrepare = commitCert.prePrepare.message
    verify(outputRef, times(1)).asyncSend(
      eqTo(
        Output.BlockOrdered(
          OrderedBlockForOutput(
            OrderedBlock(
              blockMetadata,
              prePrepare.block.proofs,
              prePrepare.canonicalCommitSet,
            ),
            prePrepare.viewNumber,
            originalLeader = commitCert.prePrepare.from,
            isLastInEpoch = true,
            orderingMode = OrderingMode.StateTransfer,
          )
        )
      )
    )(any[TraceContext], any[MetricsContext])
  }

  "handle empty block transfer response" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext()

    val stateTransferManager =
      createStateTransferManager[ProgrammableUnitTestEnv](
        p2pNetworkOutModuleRef = fakeIgnoringModule
      )

    // Initiate state transfer so that it's in progress.
    val latestCompletedEpochLocally = aBootstrapEpoch
    stateTransferManager.startCatchUp(
      aMembership,
      ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))

    // Handle an empty block transfer response.
    val blockTransferResponse =
      StateTransferMessage.BlockTransferResponse30.create(commitCertificate = None, from = otherId)
    context.runPipedMessages() shouldBe List()
    stateTransferManager
      .handleStateTransferMessage(
        VerifiedStateTransferMessage(blockTransferResponse),
        aTopologyInfo,
        latestCompletedEpochLocally,
        aBootstrapEpoch.info,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.NothingToStateTransfer(from = otherId)
  }

  "cancel a timeout" when {
    "an epoch is transferred" in {
      val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
        ProgrammableUnitTestEnv
      ], StateTransferTimeout, String]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = fakeIgnoringModule,
          maybeCustomTimeoutManager = Some(timeoutManager),
        )

      stateTransferManager.cancelTimeoutForEpoch(EpochNumber.First)

      verify(timeoutManager, times(1)).cancelTimeoutIf(any[StateTransferTimeout => Boolean])(
        any[TraceContext]
      )
      succeed
    }
  }

  "drop messages when not in state transfer" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext()

    // Create the manager and don't start any state transfer.
    val stateTransferManager =
      createStateTransferManager[ProgrammableUnitTestEnv](p2pNetworkOutModuleRef =
        fakeModuleExpectingSilence // we don't expect any outbound communication
      )

    val latestCompletedEpochLocally = aBootstrapEpoch
    val currentEpochInfo = bootstrapEpochInfo(TestBootstrapTopologyActivationTime)

    val aBlockTransferResponse =
      StateTransferMessage.BlockTransferResponse30.create(commitCertificate = None, from = otherId)

    forAll(
      List[StateTransferMessage](
        VerifiedStateTransferMessage(aBlockTransferResponse),
        StateTransferMessage.BlockStored(aCommitCert(), currentEpochInfo, from = otherId),
      )
    ) { message =>
      stateTransferManager.handleStateTransferMessage(
        message,
        aTopologyInfo,
        latestCompletedEpochLocally,
        currentEpochInfo,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      context.runPipedMessages() should be(empty)
    }
  }

  "download multiple epochs when activated" should {
    "send multiple requests" in {
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
      val parallelEpochs = 5
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef,
          epochStateTransferHowManyFutureEpochsToDownloadInParallel =
            NonNegativeLong.tryCreate(parallelEpochs.toLong),
        )

      val newEpochNumber = EpochNumber(7)
      stateTransferManager.stateTransferNewEpoch(
        newEpochNumber,
        aMembership,
        ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        Seq.empty,
        Some(EpochNumber(13)),
      )(fail(_))

      context.runPipedMessages()

      verify(
        p2pNetworkOutRef,
        times(parallelEpochs + 1),
      ).asyncSend(
        any[P2PNetworkOut.Message]
      )(any[TraceContext], any[MetricsContext])
    }

    "receiving future commit certificate" when {
      "within window" should {
        "early fetch" in {
          implicit val context
              : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext()

          val outputRef = mock[ModuleRef[Output.Message[ProgrammableUnitTestEnv]]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val parallelEpochs = 5
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              outputModuleRef = outputRef,
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val latestCompletedEpochLocally = aBootstrapEpoch
          val currentEpochInfo =
            EpochInfo(
              EpochNumber(1),
              BlockNumber(0),
              EpochLength(1),
              TestBootstrapTopologyActivationTime,
            )
          stateTransferManager.startCatchUp(
            aMembership,
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            latestCompletedEpochLocally,
            startEpoch = EpochNumber.First,
          )(abort = fail(_))
          context.runPipedMessages()

          val blockMetadataForFuture = BlockMetadata.mk(1, 10)
          val commitCert = aCommitCert(blockMetadataForFuture)

          // Handle a block transfer response with a single commit certificate.
          val blockTransferResponse =
            StateTransferMessage.BlockTransferResponse30
              .create(Some(commitCert), from = otherId)
              .fakeSign
          val topologyInfo = OrderingTopologyInfo(
            myId,
            currentTopology = aMembershipBeforeOnboarding.orderingTopology,
            currentCryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            currentLeaders = aMembershipBeforeOnboarding.leaders,
            currentBlacklistedNodes = Seq.empty,
            previousTopology = aMembershipBeforeOnboarding.orderingTopology,
            previousCryptoProvider = failingCryptoProvider,
            previousLeaders = aMembershipBeforeOnboarding.leaders,
            previousBlacklistedNodes = Seq.empty,
          )
          stateTransferManager.handleStateTransferMessage(
            UnverifiedStateTransferMessage(blockTransferResponse),
            topologyInfo,
            latestCompletedEpochLocally,
            currentEpochInfo,
          )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

          // Verify the block.
          val blockVerifiedMessage = context.runPipedMessages()
          blockVerifiedMessage should contain only
            StateTransferMessage.BlockVerified(
              commitCert,
              currentEpochInfo,
              from = otherId,
              isForFutureEpoch = true,
            )
          stateTransferManager.handleStateTransferMessage(
            blockVerifiedMessage.headOption
              .getOrElse(fail("There should be just a single block verified message"))
              .asInstanceOf[StateTransferMessage.BlockVerified[ProgrammableUnitTestEnv]],
            topologyInfo,
            latestCompletedEpochLocally,
            currentEpochInfo,
          )(fail(_))

          // Should have to Output module to early fetch block
          val prePrepare = commitCert.prePrepare.message
          verify(outputRef, times(1)).asyncSend(
            eqTo(
              Output.BlockConsensusStarted(
                blockMetadataForFuture.blockNumber,
                originalLeader = commitCert.prePrepare.from,
                OrderingBlock(prePrepare.block.proofs),
              )
            )
          )(any[TraceContext], any[MetricsContext])
          verifyNoMoreInteractions(outputRef)
        }
      }

      "outside window" should {
        "disregard" in {
          implicit val context
              : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext()

          val outputRef = mock[ModuleRef[Output.Message[ProgrammableUnitTestEnv]]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val parallelEpochs = 5
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              outputModuleRef = outputRef,
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val latestCompletedEpochLocally = aBootstrapEpoch
          val currentEpochInfo =
            EpochInfo(
              EpochNumber(1),
              BlockNumber(0),
              EpochLength(1),
              TestBootstrapTopologyActivationTime,
            )
          stateTransferManager.startCatchUp(
            aMembership,
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            latestCompletedEpochLocally,
            startEpoch = EpochNumber.First,
          )(abort = fail(_))
          context.runPipedMessages()

          val blockMetadataForFuture = BlockMetadata.mk(1 + parallelEpochs.toLong, 10)
          val commitCert = aCommitCert(blockMetadataForFuture)

          // Handle a block transfer response with a single commit certificate.
          val blockTransferResponse =
            StateTransferMessage.BlockTransferResponse30
              .create(Some(commitCert), from = otherId)
              .fakeSign
          val topologyInfo = OrderingTopologyInfo(
            myId,
            currentTopology = aMembershipBeforeOnboarding.orderingTopology,
            currentCryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            currentLeaders = aMembershipBeforeOnboarding.leaders,
            currentBlacklistedNodes = Seq.empty,
            previousTopology = aMembershipBeforeOnboarding.orderingTopology,
            previousCryptoProvider = failingCryptoProvider,
            previousLeaders = aMembershipBeforeOnboarding.leaders,
            previousBlacklistedNodes = Seq.empty,
          )
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            stateTransferManager.handleStateTransferMessage(
              UnverifiedStateTransferMessage(blockTransferResponse),
              topologyInfo,
              latestCompletedEpochLocally,
              currentEpochInfo,
            )(abort = fail(_)),
            LogEntry.assertLogSeq(
              Seq(
                (
                  { entry =>
                    entry.warningMessage should include(
                      "received a block transfer response from 'other' containing pre-prepare(s) with unexpected epoch(s) List(6), expected 0, dropping..."
                    )
                  },
                  "future block transfer",
                )
              )
            ),
          ) shouldBe StateTransferMessageResult.Continue

          context.runPipedMessages() shouldBe empty
        }
      }
    }
  }

  "speculatively receiving responses for a future epoch" should {
    val parallelEpochs = 5

    // Postpones a response for a future epoch by feeding it to the manager while it's within the future window.
    def postponeFutureResponse(
        stateTransferManager: StateTransferManager[ProgrammableUnitTestEnv],
        response: SignedMessage[StateTransferMessage.BlockTransferResponse],
        latestCompletedEpochLocally: EpochStore.Epoch,
    )(implicit context: ContextType): Unit = {
      val topologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
        myId,
        currentTopology = aMembershipBeforeOnboarding.orderingTopology,
        currentCryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        currentLeaders = aMembershipBeforeOnboarding.leaders,
        currentBlacklistedNodes = Seq.empty,
        previousTopology = aMembershipBeforeOnboarding.orderingTopology,
        previousCryptoProvider = failingCryptoProvider,
        previousLeaders = aMembershipBeforeOnboarding.leaders,
        previousBlacklistedNodes = Seq.empty,
      )
      stateTransferManager.handleStateTransferMessage(
        UnverifiedStateTransferMessage(response),
        topologyInfo,
        latestCompletedEpochLocally,
        currentEpochInfo = EpochInfo(
          EpochNumber(1),
          BlockNumber(0),
          EpochLength(1),
          TestBootstrapTopologyActivationTime,
        ),
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue
      context.runPipedMessages().discard // drain commit certificate signature verification
    }

    def verifyWaitedInsteadOfSendingRequest(
        timeoutManager: TimeoutManager[
          ProgrammableUnitTestEnv,
          Consensus.Message[ProgrammableUnitTestEnv],
          StateTransferTimeout,
          String,
        ],
        p2pNetworkOutRef: ModuleRef[P2PNetworkOut.Message],
        targetEpoch: EpochNumber,
    ): Unit = {
      // Should have scheduled a timeout to wait for the postponed responses to complete the epoch,
      //  rather than immediately sending a new block transfer request.
      verify(timeoutManager, times(1)).scheduleTimeout(
        argThat((event: StateTransferTimeout) =>
          event match {
            case StateTransferMessage.InitiateSendBlockTransferRequest(epoch, _, _) =>
              epoch == targetEpoch
            case _ => false
          }
        ),
        eqTo(Some(BftBlockOrdererConfig.DefaultEpochStateTransferTimeoutForFutureEpoch)),
      )(any[ContextType], any[TraceContext])
      verifyNoMoreInteractions(timeoutManager)
      // Should not have sent any block transfer request.
      verify(p2pNetworkOutRef, never).asyncSend(any[P2PNetworkOut.Message])(
        any[TraceContext],
        any[MetricsContext],
      )
    }

    "wait for postponed responses instead of sending a new request" when {
      "enough responses containing each " +
        "a single commit certificate for the epoch have already been received" onlyRunLessThan ProtocolVersion.v37 in {
          implicit val context: ContextType = new ProgrammableUnitTestContext()

          val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
            ProgrammableUnitTestEnv
          ], StateTransferTimeout, String]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              maybeCustomTimeoutManager = Some(timeoutManager),
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val targetEpoch = EpochNumber(2)
          val latestCompletedEpochLocally = aBootstrapEpoch

          // Speculatively receive both blocks of the (future) 2-block target epoch, each in its own response.
          Seq(BlockNumber.First, BlockNumber(1L)).foreach { blockNumber =>
            val response =
              StateTransferMessage.BlockTransferResponse30
                .create(
                  Some(aCommitCert(BlockMetadata.mk(targetEpoch, blockNumber))),
                  from = otherId,
                )
                .fakeSign
            postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)
          }

          stateTransferManager.stateTransferNewEpoch(
            targetEpoch,
            Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            nodesThatTimedOut = Seq.empty,
            targetEpochO = None,
          )(abort = fail(_))
          context.runPipedMessages().discard

          verifyWaitedInsteadOfSendingRequest(timeoutManager, p2pNetworkOutRef, targetEpoch)
        }

      "commit certificates are carried within a single response " +
        "message for protocol versions >= 37" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
          implicit val context: ContextType = new ProgrammableUnitTestContext()

          val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
            ProgrammableUnitTestEnv
          ], StateTransferTimeout, String]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              maybeCustomTimeoutManager = Some(timeoutManager),
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val targetEpoch = EpochNumber(2)
          val latestCompletedEpochLocally = aBootstrapEpoch

          // A single response carries both blocks of the (future) 2-block target epoch: the count of commit
          //  certificates (not of messages) must be used to detect that the epoch is already complete.
          val response =
            StateTransferMessage.BlockTransferResponse31
              .create(
                Seq(BlockNumber.First, BlockNumber(1L)).map(blockNumber =>
                  aCommitCert(BlockMetadata.mk(targetEpoch, blockNumber))
                ),
                from = otherId,
              )
              .fakeSign
          postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)

          stateTransferManager.stateTransferNewEpoch(
            targetEpoch,
            Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            nodesThatTimedOut = Seq.empty,
            targetEpochO = None,
          )(abort = fail(_))
          context.runPipedMessages().discard

          verifyWaitedInsteadOfSendingRequest(timeoutManager, p2pNetworkOutRef, targetEpoch)
        }
    }

    "send a new request instead of waiting" when {
      def verifySentRequestInsteadOfWaiting(
          timeoutManager: TimeoutManager[
            ProgrammableUnitTestEnv,
            Consensus.Message[ProgrammableUnitTestEnv],
            StateTransferTimeout,
            String,
          ],
          p2pNetworkOutRef: ModuleRef[P2PNetworkOut.Message],
          targetEpoch: EpochNumber,
      ): Unit = {
        val expectedRequest =
          StateTransferMessage.BlockTransferRequest.create(targetEpoch, from = myId).fakeSign
        // Should have sent a new block transfer request, as the epoch is not complete yet (blocks are missing).
        verify(p2pNetworkOutRef, times(1)).asyncSend(
          argThat((msg: P2PNetworkOut.Message) =>
            msg match {
              case P2PNetworkOut.SendToRandomAuthenticated(message, _, _, _, _, _, _) =>
                message == P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
                  expectedRequest
                )
              case _ => false
            }
          )
        )(any[TraceContext], any[MetricsContext])
        // Should have scheduled a retry for the request, rather than a wait for the postponed responses.
        verify(timeoutManager, times(1)).scheduleTimeout(
          argThat((event: StateTransferTimeout) =>
            event match {
              case StateTransferMessage.RetryBlockTransferRequest(request, _) =>
                request == expectedRequest
              case _ => false
            }
          ),
          any[Option[FiniteDuration]],
        )(any[ContextType], any[TraceContext])
        verifyNoMoreInteractions(timeoutManager)
      }

      "a response does not contain all the epoch's blocks" onlyRunLessThan ProtocolVersion.v37 in {
        implicit val context: ContextType = new ProgrammableUnitTestContext()

        val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
          ProgrammableUnitTestEnv
        ], StateTransferTimeout, String]]
        val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
        val stateTransferManager =
          createStateTransferManager[ProgrammableUnitTestEnv](
            p2pNetworkOutModuleRef = p2pNetworkOutRef,
            maybeCustomTimeoutManager = Some(timeoutManager),
            epochStateTransferHowManyFutureEpochsToDownloadInParallel =
              NonNegativeLong.tryCreate(parallelEpochs.toLong),
          )

        val targetEpoch = EpochNumber(2)
        val latestCompletedEpochLocally = aBootstrapEpoch

        // Receive only the first block of the 2-block target epoch: the epoch is not complete yet, so a new
        //  block transfer request must be sent rather than waiting for the postponed responses.
        val response =
          StateTransferMessage.BlockTransferResponse30
            .create(
              Some(aCommitCert(BlockMetadata.mk(targetEpoch, BlockNumber.First))),
              from = otherId,
            )
            .fakeSign
        postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)

        stateTransferManager.stateTransferNewEpoch(
          targetEpoch,
          Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
          ProgrammableUnitTestEnv.noSignatureCryptoProvider,
          nodesThatTimedOut = Seq.empty,
          targetEpochO = None,
        )(abort = fail(_))
        context.runPipedMessages().discard

        verifySentRequestInsteadOfWaiting(timeoutManager, p2pNetworkOutRef, targetEpoch)
      }

      "a single response does not contain all the epoch's blocks " +
        "for protocol versions >= 37" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
          implicit val context: ContextType = new ProgrammableUnitTestContext()

          val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
            ProgrammableUnitTestEnv
          ], StateTransferTimeout, String]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              maybeCustomTimeoutManager = Some(timeoutManager),
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val targetEpoch = EpochNumber(2)
          val latestCompletedEpochLocally = aBootstrapEpoch

          // A single response carries only the first block of the 2-block target epoch: the epoch is not
          //  complete yet, so a new block transfer request must be sent rather than waiting.
          val response =
            StateTransferMessage.BlockTransferResponse31
              .create(
                Seq(aCommitCert(BlockMetadata.mk(targetEpoch, BlockNumber.First))),
                from = otherId,
              )
              .fakeSign
          postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)

          stateTransferManager.stateTransferNewEpoch(
            targetEpoch,
            Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            nodesThatTimedOut = Seq.empty,
            targetEpochO = None,
          )(abort = fail(_))
          context.runPipedMessages().discard

          verifySentRequestInsteadOfWaiting(timeoutManager, p2pNetworkOutRef, targetEpoch)
        }

      "a speculative response for the same block is duplicated" onlyRunLessThan ProtocolVersion.v37 in {
        implicit val context: ContextType = new ProgrammableUnitTestContext()

        val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
          ProgrammableUnitTestEnv
        ], StateTransferTimeout, String]]
        val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
        val stateTransferManager =
          createStateTransferManager[ProgrammableUnitTestEnv](
            p2pNetworkOutModuleRef = p2pNetworkOutRef,
            maybeCustomTimeoutManager = Some(timeoutManager),
            epochStateTransferHowManyFutureEpochsToDownloadInParallel =
              NonNegativeLong.tryCreate(parallelEpochs.toLong),
          )

        val targetEpoch = EpochNumber(2)
        val latestCompletedEpochLocally = aBootstrapEpoch

        // Receive the same (single) block of the 2-block target epoch twice: counting messages (or certificates)
        //  rather than distinct blocks would wrongly consider the epoch complete and suppress the request.
        (1 to 2).foreach { _ =>
          val response =
            StateTransferMessage.BlockTransferResponse30
              .create(
                Some(aCommitCert(BlockMetadata.mk(targetEpoch, BlockNumber.First))),
                from = otherId,
              )
              .fakeSign
          postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)
        }

        stateTransferManager.stateTransferNewEpoch(
          targetEpoch,
          Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
          ProgrammableUnitTestEnv.noSignatureCryptoProvider,
          nodesThatTimedOut = Seq.empty,
          targetEpochO = None,
        )(abort = fail(_))
        context.runPipedMessages().discard

        verifySentRequestInsteadOfWaiting(timeoutManager, p2pNetworkOutRef, targetEpoch)
      }

      "a single response repeats the same block's certificate " +
        "for protocol versions >= 37" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
          implicit val context: ContextType = new ProgrammableUnitTestContext()

          val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
            ProgrammableUnitTestEnv
          ], StateTransferTimeout, String]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              maybeCustomTimeoutManager = Some(timeoutManager),
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val targetEpoch = EpochNumber(2)
          val latestCompletedEpochLocally = aBootstrapEpoch

          // A single response carries the same (single) block of the 2-block target epoch twice: distinct block
          //  numbers (not the number of certificates) must be counted to avoid suppressing the request.
          val response =
            StateTransferMessage.BlockTransferResponse31
              .create(
                Seq.fill(2)(aCommitCert(BlockMetadata.mk(targetEpoch, BlockNumber.First))),
                from = otherId,
              )
              .fakeSign
          postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)

          stateTransferManager.stateTransferNewEpoch(
            targetEpoch,
            Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            nodesThatTimedOut = Seq.empty,
            targetEpochO = None,
          )(abort = fail(_))
          context.runPipedMessages().discard

          verifySentRequestInsteadOfWaiting(timeoutManager, p2pNetworkOutRef, targetEpoch)
        }

      "a single response mixes in certificates from other epochs " +
        "for protocol versions >= 37" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
          implicit val context: ContextType = new ProgrammableUnitTestContext()

          val timeoutManager = mock[TimeoutManager[ProgrammableUnitTestEnv, Consensus.Message[
            ProgrammableUnitTestEnv
          ], StateTransferTimeout, String]]
          val p2pNetworkOutRef = spy(new P2pNetworkOutRefExpectingSendToRandomAuthenticated)
          val stateTransferManager =
            createStateTransferManager[ProgrammableUnitTestEnv](
              p2pNetworkOutModuleRef = p2pNetworkOutRef,
              maybeCustomTimeoutManager = Some(timeoutManager),
              epochStateTransferHowManyFutureEpochsToDownloadInParallel =
                NonNegativeLong.tryCreate(parallelEpochs.toLong),
            )

          val targetEpoch = EpochNumber(2)
          val latestCompletedEpochLocally = aBootstrapEpoch

          // A single (invalid) response carries the first block of the 2-block target epoch plus a certificate
          //  from a future epoch. The mixed-epoch batch is still queued under the target epoch, but only the
          //  target-epoch block must count: otherwise the foreign-epoch block would wrongly complete the epoch
          //  and suppress the immediate request, forcing a timeout wait.
          val response =
            StateTransferMessage.BlockTransferResponse31
              .create(
                Seq(
                  aCommitCert(BlockMetadata.mk(targetEpoch, BlockNumber.First)),
                  aCommitCert(BlockMetadata.mk(EpochNumber(targetEpoch + 1), BlockNumber(1L))),
                ),
                from = otherId,
              )
              .fakeSign
          postponeFutureResponse(stateTransferManager, response, latestCompletedEpochLocally)

          stateTransferManager.stateTransferNewEpoch(
            targetEpoch,
            Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(2)),
            ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            nodesThatTimedOut = Seq.empty,
            targetEpochO = None,
          )(abort = fail(_))
          context.runPipedMessages().discard

          verifySentRequestInsteadOfWaiting(timeoutManager, p2pNetworkOutRef, targetEpoch)
        }
    }
  }

  private def createStateTransferManager[E <: BaseIgnoringUnitTestEnv[E]](
      outputModuleRef: ModuleRef[Output.Message[E]] = fakeModuleExpectingSilence,
      p2pNetworkOutModuleRef: ModuleRef[P2PNetworkOut.Message],
      epochStore: EpochStore[E] = new InMemoryUnitTestEpochStore[E],
      maybeCustomTimeoutManager: Option[
        TimeoutManager[E, Consensus.Message[E], StateTransferTimeout, String]
      ] = None,
      epochStateTransferHowManyFutureEpochsToDownloadInParallel: NonNegativeLong =
        NonNegativeLong.tryCreate(0L),
      consensusWindowSizeForStateTransferOfCommitCertificates: Option[PositiveInt] =
        BftBlockOrdererConfig.DefaultConsensusWindowSizeForStateTransferOfCommitCertificates,
      protocolVersion: ProtocolVersion = synchronizerProtocolVersion,
  ): StateTransferManager[E] = {
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    implicit val synchronizerProtocolVersion: ProtocolVersion = protocolVersion

    val dependencies = ConsensusModuleDependencies[E](
      availability = fakeIgnoringModule,
      outputModuleRef,
      p2pNetworkOutModuleRef,
    )

    val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
    implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig(
      epochStateTransferHowManyFutureEpochsToDownloadInParallel =
        epochStateTransferHowManyFutureEpochsToDownloadInParallel,
      consensusWindowSizeForStateTransferOfCommitCertificates =
        consensusWindowSizeForStateTransferOfCommitCertificates,
    )

    new StateTransferManager(
      myId,
      dependencies,
      epochStore,
      WorkflowId("test-workflow-id"),
      metrics,
      loggerFactory,
    )(maybeCustomTimeoutManager)
  }

  private def assertBlockTransferRequestHasBeenSent(
      p2pNetworkOutRef: ModuleRef[P2PNetworkOut.Message],
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      possibleRecipients: Seq[BftNodeId],
      numberOfTimes: Int,
  )(implicit context: ContextType): Unit = {
    // Should have scheduled a retry.
    context.lastDelayedMessage shouldBe Some(
      numberOfTimes -> StateTransferMessage.RetryBlockTransferRequest(
        blockTransferRequest,
        Some(otherId),
      )
    )
    // Should have sent a block transfer request to the other node only.
    val order = inOrder(p2pNetworkOutRef)
    order
      .verify(p2pNetworkOutRef, times(1))
      .asyncSend(
        argThat((msg: P2PNetworkOut.Message) =>
          msg match {
            case P2PNetworkOut.SendToRandomAuthenticated(
                  message,
                  `possibleRecipients`, // firstChoiceRecipientsPool
                  None, // secondChoiceRecipientsPool
                  Some(workflowId),
                  Seq(), // nodeThatFailed
                  Some(_), // onRecipientDecision
                  howManyRecipients,
                )
                if howManyRecipients.value == 1 &&
                  workflowId == WorkflowId(
                    "test-workflow-id"
                  ) && message == P2PNetworkOut.BftOrderingNetworkMessage
                    .StateTransferMessage(blockTransferRequest) =>
              true
            case _ => false
          }
        )
      )(any[TraceContext], any[MetricsContext])
    if (numberOfTimes > 1)
      order
        .verify(p2pNetworkOutRef, times(numberOfTimes - 1))
        .asyncSend(
          argThat((msg: P2PNetworkOut.Message) =>
            msg match {
              case P2PNetworkOut.SendToRandomAuthenticated(
                    message,
                    `possibleRecipients`, // firstChoiceRecipientsPool
                    None, // secondChoiceRecipientsPool
                    Some(workflowId),
                    Seq(`otherId`), // nodeThatFailed
                    Some(_), // onRecipientDecision
                    howManyRecipients,
                  )
                  if howManyRecipients.value == 1 &&
                    workflowId == WorkflowId(
                      "test-workflow-id"
                    ) && message == P2PNetworkOut.BftOrderingNetworkMessage
                      .StateTransferMessage(blockTransferRequest) =>
                true
              case _ => false
            }
          )
        )(any[TraceContext], any[MetricsContext])
    order
      .verify(p2pNetworkOutRef, never)
      .asyncSend(any[P2PNetworkOut.Message])(any[TraceContext], any[MetricsContext])
  }
}

object StateTransferManagerTest {
  private type ContextType = ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]

  private def aMembership(implicit pv: ProtocolVersion) =
    Membership.forTesting(myId, Set(otherId), epochLength = EpochLength(1))
  private def aMembershipBeforeOnboarding(implicit pv: ProtocolVersion) =
    Membership(
      myId,
      OrderingTopology
        .forTesting(
          Set(otherId),
          Option(SequencingParameters.Default),
          epochLength = EpochLength(1),
        ),
      leaders = Seq(otherId),
      blacklistedNodes = Seq.empty,
    )
  private def aTopologyInfo(implicit pv: ProtocolVersion) =
    OrderingTopologyInfo[ProgrammableUnitTestEnv](
      myId,
      currentTopology = aMembership.orderingTopology,
      currentCryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      currentLeaders = aMembership.leaders,
      currentBlacklistedNodes = Seq.empty,
      previousTopology = aMembership.orderingTopology,
      previousCryptoProvider = failingCryptoProvider,
      previousLeaders = aMembership.leaders,
      previousBlacklistedNodes = Seq.empty,
    )
  private val aBootstrapEpoch = bootstrapEpoch(TestBootstrapTopologyActivationTime)
}
