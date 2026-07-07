// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.admin.sequencer.v30
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration

/** Configuration to determine subscription liveness.
  *
  * In order to avoid false positive in case of either rapid bursts of events, or long periods of
  * inactivity on the synchronizer, both limits must be exceeded for a subscription to be considered
  * silent.
  *
  * @param maxTimestampDelta
  *   Defines the maximum time difference between the latest aggregated event and the latest event
  *   provided by a subscription.
  * @param maxOrdinalDelta
  *   Events received on a subscription and events processed by the [[SequencerAggregator]] are
  *   associated to an ordinal number. This defines the maximum difference between the ordinal of
  *   the latest aggregated event and the ordinal of the latest event provided by a subscription.
  *
  * Setting both [[maxTimestampDelta]] and [[maxOrdinalDelta]] to 0 disables the silent subscription
  * detection.
  */
final case class SubscriptionLivenessLimits(
    maxTimestampDelta: NonNegativeFiniteDuration,
    maxOrdinalDelta: NonNegativeInt,
) extends PrettyPrinting {
  private[sequencing] def toProtoV30: v30.SubscriptionLivenessLimits =
    v30.SubscriptionLivenessLimits(
      maxTimestampDelta = Some(maxTimestampDelta.toProtoPrimitive),
      maxOrdinalDelta = maxOrdinalDelta.unwrap,
    )

  override protected def pretty: Pretty[SubscriptionLivenessLimits] = prettyOfClass(
    param("maxTimestampDelta", _.maxTimestampDelta),
    param("maxOrdinalDelta", _.maxOrdinalDelta),
  )
}

object SubscriptionLivenessLimits {
  val default: SubscriptionLivenessLimits = SubscriptionLivenessLimits(
    maxTimestampDelta = NonNegativeFiniteDuration.tryOfMinutes(2),
    maxOrdinalDelta = NonNegativeInt.tryCreate(50),
  )

  private[sequencing] def fromProtoV30(
      proto: v30.SubscriptionLivenessLimits
  ): ParsingResult[SubscriptionLivenessLimits] = {
    val v30.SubscriptionLivenessLimits(
      maxTimestampDeltaP,
      maxOrdinalDeltaP,
    ) = proto

    for {
      maxTimestampDelta <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("max_timestamp_delta"),
        "max_timestamp_delta",
        maxTimestampDeltaP,
      )
      maxOrdinalDelta <- ProtoConverter.parseNonNegativeInt(
        "max_ordinal_delta",
        maxOrdinalDeltaP,
      )
    } yield SubscriptionLivenessLimits(
      maxTimestampDelta = maxTimestampDelta,
      maxOrdinalDelta = maxOrdinalDelta,
    )
  }
}
