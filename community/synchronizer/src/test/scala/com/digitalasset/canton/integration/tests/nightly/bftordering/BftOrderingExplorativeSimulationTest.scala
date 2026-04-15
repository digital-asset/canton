// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.bftordering

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.PartitionSymmetry.{
  ASymmetric,
  Symmetric,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.BftOrderingSimulationTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.{
  SimulationTestSettings,
  SimulationTestStageSettings,
  TopologySettings,
}
import org.scalatest.Assertion

import scala.collection.immutable.TreeMap
import scala.concurrent.duration
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.util.Random

class BftOrderingExplorativeSimulationTest extends BftOrderingSimulationTest {

  override def numberOfRuns: Int = 50

  private val randomSourceToCreateSettings = new Random()

  private val durationOfFirstPhaseWithFaults = 5.minute

  override def allowedWarnings: Seq[LogEntry => Assertion] = Seq(
    // We might get messages from off boarded nodes, don't count these as errors.
    { logEntry =>
      logEntry.message should include(
        "but it cannot be verified in the currently known dissemination topology"
      )
      logEntry.loggerName should include("AvailabilityModule")
    }
  )

  private val zeroProbability: Probability = Probability(0)
  private def generateProb(low: Double, high: Double): Probability = Probability(
    randomSourceToCreateSettings.between(low, high)
  )

  private def randomWeightedNonEmptyOneOf[T](items: NonEmpty[Seq[(Int, T)]]): T = {
    // inspired by frequency from scalacheck
    // https://github.com/typelevel/scalacheck/blob/67a8463f4d092ddfc1233874fda707d74ae85353/core/shared/src/main/scala/org/scalacheck/Gen.scala#L907
    var total = 0L
    val builder = TreeMap.newBuilder[Long, T]
    items.foreach { case (weight, value) =>
      total += weight
      builder += ((total, value))
    }
    val tree = builder.result()

    val ix = randomSourceToCreateSettings.between(1, total + 1)
    tree.rangeFrom(ix).head._2
  }

  private def randomWeightedOneOf[T](head: (Int, T), tail: (Int, T)*): T =
    randomWeightedNonEmptyOneOf(
      NonEmpty.mk(Seq, head, tail*)
    )

  private def randomEquallyWeightedOneOf[T](head: T, tail: T*): T = randomWeightedNonEmptyOneOf(
    NonEmpty.mk(Seq, head, tail*).map(1 -> _)
  )

  private val shortTime: PowerDistribution = PowerDistribution(0.milliseconds, 100.milliseconds)
  private val longTime: PowerDistribution = PowerDistribution(1.second, 5.seconds)

  private def generateStage(epochLength: EpochLength): SimulationTestStageSettings = {
    val numberOfNodesToOnboard = randomWeightedOneOf(
      10 -> 0,
      3 -> 1,
    )
    SimulationTestStageSettings(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          crashRestartChance = randomEquallyWeightedOneOf(
            zeroProbability,
            generateProb(0.0, 0.5),
          ),
          crashRestartGracePeriod = randomWeightedOneOf(
            1 -> shortTime,
            2 -> longTime,
          ),
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          packetLoss = randomEquallyWeightedOneOf(zeroProbability, generateProb(0.0, 0.33)),
          packetReplay = randomEquallyWeightedOneOf(zeroProbability, generateProb(0.0, 0.33)),
          partitionProbability =
            randomEquallyWeightedOneOf(generateProb(0.0, 0.2), generateProb(0.0, 0.5)),
          partitionMode = randomEquallyWeightedOneOf(
            PartitionMode.NoPartition,
            PartitionMode.UniformSize,
            PartitionMode.RandomlyDropConnections(generateProb(0, 0.5)),
          ),
          partitionStability = randomEquallyWeightedOneOf(2.seconds, 5.seconds, 10 seconds),
          partitionSymmetry = randomEquallyWeightedOneOf(Symmetric, ASymmetric),
          unPartitionProbability =
            randomEquallyWeightedOneOf(generateProb(0.0, 0.2), generateProb(0.0, 0.5)),
          unPartitionStability = randomEquallyWeightedOneOf(2.seconds, 5.seconds, 10 seconds),
        ),
        FutureSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        phaseDurations = PhaseDurations(
          faulty = durationOfFirstPhaseWithFaults,
          recovery =
            if (numberOfNodesToOnboard > 0)
              (epochLength / 2) seconds
            else {
              0 seconds
            },
        ),
      ),
      TopologySettings(
        randomSourceToCreateSettings.nextLong(),
        nodeOnboardingDelays = (0 until numberOfNodesToOnboard).map(_ =>
          FiniteDuration.apply(
            randomSourceToCreateSettings.nextLong(
              (0.8 * durationOfFirstPhaseWithFaults.toNanos).toLong
            ),
            duration.NANOSECONDS,
          )
        ),
      ),
    )
  }

  override def generateSettings: SimulationTestSettings = {
    val epochLength = EpochLength(
      randomWeightedOneOf[Long](
        10 -> 16L,
        5 -> 128L,
        1 -> 1L,
        2 -> randomSourceToCreateSettings.between(2L, 128L),
      )
    )
    SimulationTestSettings(
      numberOfInitialNodes = randomEquallyWeightedOneOf(2, 4, 5),
      epochLength = epochLength,
      stages = NonEmpty(
        Seq,
        generateStage(epochLength),
        generateStage(epochLength),
      ),
    )
  }
}
