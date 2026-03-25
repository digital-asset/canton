// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing

import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PekkoBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
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
      val blockSubscription = new PekkoBlockSubscription[PekkoEnv](
        BlockNumber(0),
        timeouts,
        loggerFactory,
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
  }
}
