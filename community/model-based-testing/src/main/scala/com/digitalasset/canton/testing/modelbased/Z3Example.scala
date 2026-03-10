// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.microsoft.z3.{Context, Status}

/** Uses Z3 to synthesize values satisfying constraints. */
object Z3Synthesizer {

  /** Synthesizes an integer strictly greater than [lowerBound].
    */
  def synthesizeGreaterThan(lowerBound: Long): Option[Long] = {
    val ctx = new Context()
    try {
      val solver = ctx.mkSolver()
      val x = ctx.mkIntConst("x")

      solver.add(ctx.mkGt(x, ctx.mkInt(lowerBound)))

      if (solver.check() == Status.SATISFIABLE) {
        val model = solver.getModel
        Some(model.eval(x, true).toString.toLong)
      } else {
        None
      }
    } finally {
      ctx.close()
    }
  }
}
