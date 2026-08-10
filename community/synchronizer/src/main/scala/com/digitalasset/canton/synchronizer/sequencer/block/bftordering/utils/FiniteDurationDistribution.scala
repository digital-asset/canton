// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.WeightedDistribution.WeightedDuration

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

sealed trait FiniteDurationDistribution extends Product with Serializable {

  def generateRandomDuration(rng: Random): FiniteDuration
}

final case class ConstantDistribution(duration: FiniteDuration) extends FiniteDurationDistribution {

  override def generateRandomDuration(rng: Random): FiniteDuration = duration
}

final case class WeightedDistribution(weightedDurations: Seq[WeightedDuration])
    extends FiniteDurationDistribution {

  require(weightedDurations.nonEmpty, "WeightedDistribution must have at least one duration")

  private val maxSelectorExclusive = weightedDurations.map(_.weight.unwrap.toLong).sum + 1L

  private val navigableDurationsByCumulativeWeights = {
    val navigableMap = new java.util.TreeMap[Long, FiniteDuration]()
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var cumulativeWeight = 0L
    weightedDurations.foreach { case WeightedDuration(weight, duration) =>
      cumulativeWeight += weight.unwrap
      navigableMap.put(cumulativeWeight, duration).discard
    }
    navigableMap
  }

  override def generateRandomDuration(rng: Random): FiniteDuration =
    navigableDurationsByCumulativeWeights
      .ceilingEntry(rng.between(1L, maxSelectorExclusive))
      .getValue
}
object WeightedDistribution {
  final case class WeightedDuration(weight: PositiveInt, duration: FiniteDuration)
}

final case class LinearDistribution(low: FiniteDuration, high: FiniteDuration)
    extends FiniteDurationDistribution {

  require(low <= high, "Low must be less than or equal to high")

  override def generateRandomDuration(rng: Random): FiniteDuration = {
    val range = high.minus(low).toMicros
    val sample = rng.nextDouble() * range
    FiniteDuration(sample.toLong, TimeUnit.MICROSECONDS).plus(low)
  }
}

// following: https://github.com/DACH-NY/simulation-testing-demo/tree/main

final case class PowerDistribution(low: FiniteDuration, mean: FiniteDuration)
    extends FiniteDurationDistribution {

  override def generateRandomDuration(rng: Random): FiniteDuration = {
    // the nextDouble function has a range of [0, 1)
    val domain = rng.nextDouble() + Double.MinPositiveValue
    // the log function has a range of (-inf, 0] in the domain of (0, 1]
    // so we negate to get the range of [0, inf)
    // 0 is excluded from the domain to eliminate potential inf calculation blowing up FiniteDuration construction
    val sample = -Math.log(domain)
    // we adjust the mean, since we will add `low` afterward
    // to guarantee we are at least `low`
    val adjustedMean = mean.minus(low).max(0.microseconds).toMicros

    FiniteDuration((adjustedMean * sample).toLong, TimeUnit.MICROSECONDS).plus(low)
  }

  def copyWithMaxLow(otherLow: FiniteDuration): PowerDistribution =
    this.copy(low = low.max(otherLow))
}
