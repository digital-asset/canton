// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.SequencerCoreSubscriptionConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PekkoBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext}
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.scalatest.wordspec.AnyWordSpec

class PekkoBlockSubscriptionTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with HasActorSystem
    with HasExecutionContext {

  "PekkoBlockSubscription" should {

    "give blocks in correct order" in {
      val blockSubscription =
        new PekkoBlockSubscription[PekkoEnv](
          BlockNumber(0),
          fakeModuleExpectingSilence,
          timeouts,
          loggerFactory,
          SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
          SequencerCoreSubscriptionConfig( // Do not pause
            pekkoQueueSourceBufferSize = 10000,
            pauseOrdererThresholdBufferSize = 10000,
            resumeOrdererThresholdBufferSize = 10000,
          ),
        )(x => fail(x))(parallelExecutionContext, implicitly[Materializer])

      val numberOfBlocksToMake = 10000L

      val blocks = (0L until numberOfBlocksToMake).map { i =>
        BlockFormat.Block(i, 0, Seq.empty)
      }.toVector

      var heightToObserve = 0L
      val subscriberF = blockSubscription
        .subscription()
        .take(numberOfBlocksToMake)
        .map { block =>
          block.value.blockHeight shouldBe heightToObserve
          heightToObserve += 1
          ()
        }
        .run()

      blocks.foreach(blockSubscription.receiveBlock)

      subscriberF.futureValue shouldBe Done
    }

    "pause the orderer if the buffer size exceeds the pause threshold and resume it when it is lower to or equal than the " in {
      val outputMock = mock[ModuleRef[Output.ProcessNewEpochTopologyMessagesIfPossible.type]]
      val blockSubscription =
        new PekkoBlockSubscription[PekkoEnv](
          BlockNumber(0),
          outputMock,
          timeouts,
          loggerFactory,
          SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
          SequencerCoreSubscriptionConfig( // pause soon
            pekkoQueueSourceBufferSize = 0,
            pauseOrdererThresholdBufferSize = 1,
            resumeOrdererThresholdBufferSize = 1,
          ),
        )(x => fail(x))(parallelExecutionContext, implicitly[Materializer])

      always() {
        blockSubscription.sequencerCoreIsSlow shouldBe false
      }

      val unblockConsumerPromise = scala.concurrent.Promise[Unit]()
      blockSubscription.subscription().mapAsync(1)(_ => unblockConsumerPromise.future).run().discard

      // The Pekko queue source, even with no buffer and backpressure policy,
      //  takes a while to backpressure the producer by blocking the `offer` future
      (0L until 100L)
        .map { i =>
          BlockFormat.Block(i, 0, Seq.empty)
        }
        .foreach(blockSubscription.receiveBlock)

      eventually() {
        blockSubscription.sequencerCoreIsSlow shouldBe true
      }

      unblockConsumerPromise.success(())

      eventually() {
        blockSubscription.sequencerCoreIsSlow shouldBe false
      }
      verify(outputMock, timeout(millis = 1_000).atLeastOnce())
        .asyncSend(eqTo(Output.ProcessNewEpochTopologyMessagesIfPossible))(
          anyTraceContext,
          any[MetricsContext],
        )
    }
  }
}
