// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Parallel
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalactic.source.Position
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class FutureInstancesTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val repetitions = 20

  "FutureInstances.parallel" should {
    "execute Future tasks in parallel" in {
      parallelTest[Future](repetitions, f => Future(f()), _.futureValue)
    }

    "execute FutureUnlessShutdown tasks in parallel" in {
      parallelTest[FutureUnlessShutdown](
        20,
        f => FutureUnlessShutdown.outcomeF(Future(f())),
        _.futureValueUS,
      )
    }
  }

  def parallelTest[F[_]](
      repetitions: Int,
      async: (() => Int) => F[Int],
      await: F[Seq[Int]] => Seq[Int],
  )(implicit parallel: Parallel[F], pos: Position): Assertion = {
    val executionOrder = new AtomicReference[Seq[Int]](Seq.empty[Int])
    val f = MonadUtil.parTraverseWithLimit(PositiveInt.three)(1 to repetitions) { i =>
      async { () =>
        if (i % 4 == 0) {
          Threading.sleep(100)
        }
        executionOrder.getAndUpdate(_ :+ i)
        i
      }
    }
    // The results are still in order.
    await(f) shouldBe (1 to repetitions)
    // Due to the thread sleeps, some tasks should get executed out of order.
    executionOrder.get should not be (1 to repetitions)
  }
}
