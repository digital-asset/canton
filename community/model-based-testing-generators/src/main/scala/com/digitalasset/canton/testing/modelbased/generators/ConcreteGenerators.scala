// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.generators

import com.digitalasset.canton.testing.modelbased.ast.{Concrete, Skeleton}
import com.digitalasset.canton.testing.modelbased.genlib.Spaces
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.daml.lf.language.LanguageVersion

class ConcreteGenerators(
    languageVersion: LanguageVersion,
    readOnlyRollbacks: Boolean,
    generateQueryByKey: Boolean = false,
    keyMode: KeyMode = SymbolicSolver.KeyMode.NonUniqueContractKeys,
) {

  import ConcreteGenerators.*

  private val skeletonEnumerator =
    new SkeletonEnumerator(languageVersion, readOnlyRollbacks, generateQueryByKey)

  @scala.annotation.tailrec
  private def randomBigIntLessThan(n: BigInt): BigInt = {
    val res = BigInt(n.bitLength, new scala.util.Random())
    if (res >= n) randomBigIntLessThan(n)
    else res
  }

  /** Returns a generator of valid scenarios. */
  def validScenarioGenerator(
      numParties: Int,
      numPackages: Int,
      numParticipants: Int,
      numCommands: Option[Int] = None,
  ): Generator[Concrete.Scenario] = {
    val scenarioSpace: Spaces.Space[Skeleton.Scenario] =
      numCommands match {
        case Some(n) =>
          skeletonEnumerator.scenarios(numParticipants, n)
        case None =>
          skeletonEnumerator.scenarios(numParticipants)
      }
    new Generator[Concrete.Scenario]() {
      def generate(size: Int, distinctKeyToContractRatio: Double): Concrete.Scenario = {
        val scenarios = scenarioSpace(size)
        LazyList
          .continually(randomBigIntLessThan(scenarios.cardinal))
          .map(i => scenarios(i))
          .filter(s => s.ledger.nonEmpty)
          .flatMap(s =>
            SymbolicSolver.solve(s, numPackages, numParties, distinctKeyToContractRatio, keyMode)
          )
          .headOption
          .getOrElse(throw new IllegalStateException("failed to generate a valid scenario"))
      }
    }
  }
}

object ConcreteGenerators {
  abstract class Generator[A] {

    /* Generates a valid concrete scenario by repeatedly sampling random skeletons of the given
     * size and solving them with the Z3-based [[SymbolicSolver]].
     *
     * @param size
     *   roughly controls the number of constructors of the generated scenario.
     * @param distinctKeyToContractRatio
     *   ratio between the number of distinct contract keys and the number of `CreateWithKey`
     *   actions. A value of `1.0` (the default) maximises key diversity — every keyed contract can
     *   get its own key. Lower values (e.g. `0.3`) force more key sharing, producing scenarios
     *   with higher key contention.
     */
    def generate(size: Int, distinctKeyToContractRatio: Double = 1): A
  }
}
