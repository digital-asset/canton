// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.SizeLimitedBoundedQueueTest.{
  Command,
  standardGenerator,
}
import com.digitalasset.canton.util.collection.BoundedQueue
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import org.scalacheck.Gen
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SizeLimitedBoundedQueueTest extends AnyWordSpec with BaseTest with ScalaCheckPropertyChecks {

  private def takeUntilSum(xs: Seq[Int], limit: Int): Seq[Int] = {
    var sum = 0
    xs.takeWhile { x =>
      sum += x
      sum <= limit
    }
  }

  "SizeLimitedBoundedQueue" should {
    "enqueue not over size" in {
      val totalItemSizeLimit = 2500
      val numberOfItemsLimit = 8
      forAll(
        (Gen.listOf(standardGenerator), "commands"),
        (
          Gen.oneOf(
            BoundedQueue.DropStrategy.DropNewest,
            BoundedQueue.DropStrategy.DropOldest,
          ),
          "dropStrategy",
        ),
        minSuccessful(10_000),
      ) { (commands: List[Command], dropStrategy) =>
        val queue = new SizeLimitedBoundedQueue[Int](
          totalItemSizeLimit = Some(PositiveInt.tryCreate(totalItemSizeLimit)),
          maxNumberOfItems = numberOfItemsLimit,
          dropStrategy = dropStrategy,
          itemSizeOf = x => x,
        )
        commands.foreach { command =>
          command match {
            case Command.Enqueue(item) =>
              val before = queue.toSeq
              val addItemBeforeAdjustment = before :+ item
              val howManyRemovedO = queue.enqueue(item)

              dropStrategy match {
                case DropStrategy.DropOldest =>
                  howManyRemovedO match {
                    case Some(value) =>
                      queue.toSeq shouldBe addItemBeforeAdjustment.drop(value)
                    case None => queue.toSeq shouldBe addItemBeforeAdjustment
                  }

                  takeUntilSum(addItemBeforeAdjustment.reverse, totalItemSizeLimit)
                    .take(numberOfItemsLimit)
                    .reverse shouldBe queue.toSeq

                case DropStrategy.DropNewest =>
                  howManyRemovedO match {
                    case Some(value) =>
                      queue.toSeq shouldBe addItemBeforeAdjustment.dropRight(value)
                    case None => queue.toSeq shouldBe addItemBeforeAdjustment
                  }

                  takeUntilSum(addItemBeforeAdjustment, totalItemSizeLimit).take(
                    numberOfItemsLimit
                  ) shouldBe queue.toSeq
              }

            case Command.Dequeue =>
              if (!queue.isEmpty) {
                val allItems = queue.toSeq
                queue.dequeue() shouldBe allItems.head
                queue.toSeq shouldBe allItems.drop(1)
              }
            case Command.RemoveHead =>
              val allItems = queue.toSeq
              queue.removeHead() shouldBe allItems.headOption
              queue.toSeq shouldBe allItems.drop(1)
            case Command.RemoveLast =>
              val allItems = queue.toSeq
              queue.removeLast() shouldBe allItems.lastOption
              queue.toSeq shouldBe allItems.dropRight(1)
          }
          val currentTotalSize = queue.toSeq.sum
          currentTotalSize should be <= totalItemSizeLimit
          queue.getCurrentTotalItemSize shouldBe currentTotalSize
          queue.toSeq.size should be <= numberOfItemsLimit
        }
      }
    }
  }
}

object SizeLimitedBoundedQueueTest {
  sealed trait Command
  object Command {
    final case class Enqueue(item: Int) extends Command
    case object Dequeue extends Command
    case object RemoveHead extends Command
    case object RemoveLast extends Command
  }

  val standardGenerator: Gen[Command] = Gen.frequency[Command](
    5 -> Gen.chooseNum(0, 1000).map(Command.Enqueue(_)),
    1 -> Gen.const(Command.Dequeue),
    1 -> Gen.const(Command.RemoveHead),
    1 -> Gen.const(Command.RemoveLast),
  )
}
