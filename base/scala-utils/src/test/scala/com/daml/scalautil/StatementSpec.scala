// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StatementSpec extends AnyFlatSpec with Matchers {

  behavior of Statement.getClass.getSimpleName

  it should "evaluate passed expression for side effects" in {
    var counter: Int = 0

    def increment(): Int = {
      counter += 1
      counter
    }

    Statement.discard(increment()): Unit

    Statement.discard {
      counter += 1
    }: Unit

    counter shouldBe 2
  }

  it should "not evaluate passed lambda expression" in {
    var counter: Int = 0

    Statement.discard { () =>
      counter += 1
    }

    counter shouldBe 0
  }
}
