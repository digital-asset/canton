// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.{BaseTest, DiscardedFuture, DiscardedFutureTest}
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

class FutureUnlessShutdownTest extends AnyWordSpec with BaseTest {
  "DiscardedFuture" should {
    "detect discarded FutureUnlessShutdown" in {
      val result = WartTestTraverser(DiscardedFuture) {
        FutureUnlessShutdown.pure(())
        ()
      }
      DiscardedFutureTest.assertErrors(result, 1)
    }

    "detect discarded FutureunlessShutdown when wrapped" in {
      val result = WartTestTraverser(DiscardedFuture) {
        EitherT(FutureUnlessShutdown.pure(Either.right(())))
        ()
      }
      DiscardedFutureTest.assertErrors(result, 1)
    }
  }
}
