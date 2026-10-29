// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.digitalasset.canton.admin.sequencer.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeDouble, PositiveDouble}
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.PositiveFiniteDuration

/** Configuration of the penalty mechanism for sequencer connections.
  *
  * @param enabled
  *   Whether the penalty mechanism is enabled. If disabled, penalties are still tracked but ignored
  *   when selecting sequencers.
  * @param decayInterval
  *   Interval at which the penalties of the sequencers in the connection pool are decayed.
  * @param halfLife
  *   Half-life of a sequencer's penalty. The penalty is reduced by half over this duration.
  * @param zeroThreshold
  *   Decayed penalties below this value are reset to 0 so that a sequencer whose penalty has been
  *   increased can start over after a bounded number of decays.
  */
final case class SequencerConnectionPenaltyConfig(
    enabled: Boolean,
    decayInterval: PositiveFiniteDuration,
    halfLife: PositiveFiniteDuration,
    zeroThreshold: PositiveDouble,
) extends PrettyPrintingFromCompanion {

  /** Fraction of a penalty that is retained at each decay, in [0, 1). */
  lazy val decayFactor: NonNegativeDouble = {
    def nanos(duration: PositiveFiniteDuration): Double = duration.toScala.toNanos.toDouble

    // `create` should never fail because Double overflows to Infinity, not a negative value,
    // so at worst we will get a decay factor of 0.
    NonNegativeDouble
      .create(math.pow(0.5, nanos(decayInterval) / nanos(halfLife)))
      .valueOr(err => throw new IllegalStateException(s"Illegal decayFactor: $err"))
  }

  private[sequencing] def toProtoV30: v30.SequencerConnectionPenaltyConfig =
    v30.SequencerConnectionPenaltyConfig(
      enabled = enabled,
      decayInterval = Some(decayInterval.toProtoPrimitive),
      halfLife = Some(halfLife.toProtoPrimitive),
      zeroThreshold = zeroThreshold.value,
    )

  override def prettyCompanion: PrettyPrintingCompanion[SequencerConnectionPenaltyConfig] =
    SequencerConnectionPenaltyConfig
}

object SequencerConnectionPenaltyConfig
    extends PrettyPrintingCompanion[SequencerConnectionPenaltyConfig] {
  override protected val pretty: Pretty[SequencerConnectionPenaltyConfig] = prettyOfClass(
    param("enabled", _.enabled),
    param("decayInterval", _.decayInterval),
    param("halfLife", _.halfLife),
    param("zeroThreshold", _.zeroThreshold),
  )

  val default: SequencerConnectionPenaltyConfig = SequencerConnectionPenaltyConfig(
    enabled = true,
    decayInterval = PositiveFiniteDuration.tryOfSeconds(10),
    halfLife = PositiveFiniteDuration.tryOfSeconds(60),
    zeroThreshold = PositiveDouble.tryCreate(0.01),
  )

  private[sequencing] def fromProtoV30(
      proto: v30.SequencerConnectionPenaltyConfig
  ): ParsingResult[SequencerConnectionPenaltyConfig] = {
    val v30.SequencerConnectionPenaltyConfig(enabledP, decayIntervalP, halfLifeP, zeroThresholdP) =
      proto

    for {
      decayInterval <- ProtoConverter.parseRequired(
        PositiveFiniteDuration.fromProtoPrimitive("decay_interval"),
        "decay_interval",
        decayIntervalP,
      )
      halfLife <- ProtoConverter.parseRequired(
        PositiveFiniteDuration.fromProtoPrimitive("half_life"),
        "half_life",
        halfLifeP,
      )
      zeroThreshold <- ProtoConverter.parsePositiveDouble("zero_threshold", zeroThresholdP)
    } yield SequencerConnectionPenaltyConfig(
      enabled = enabledP,
      decayInterval = decayInterval,
      halfLife = halfLife,
      zeroThreshold = zeroThreshold,
    )
  }
}
