// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import cats.data.ContT
import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.speedy.SResult.SResultFinal
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import Speedy.Control

class ContTTest
    extends AnyWordSpec
    with Matchers
    with SuppressingLogging {

  "ContT[Control, _, _]" should {

    "drive a deep flatMap chain without overflowing stack" in {
      val N = 100000
      val chain: ContT[Control, Nothing, Int] =
        (1 to N).foldLeft(ContT.pure[Control, Nothing, Int](0)) { (acc, _) =>
          acc.flatMap(i => ContT.pure[Control, Nothing, Int](i + 1))
        }

      val control: Control[Nothing] = chain.run(n => Control.Value(SValue.SInt64(n.toLong)))

      val machine = Speedy.Machine.fromPureSExpr(
        PureCompiledPackages.Empty(Compiler.Config.Default),
        SExpr.SEValue(SValue.SUnit),
        MachineLogger(),
      )
      machine.setControl(control)

      machine.run() shouldBe SResultFinal(SValue.SInt64(N.toLong))
    }
  }
}
