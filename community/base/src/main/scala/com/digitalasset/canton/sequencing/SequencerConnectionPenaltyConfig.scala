// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.digitalasset.canton.admin.sequencer.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeDouble, PositiveDouble, PositiveInt}
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.sequencing.client.pool.SequencerPenalties.{Penalty, PenaltyKind}
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
  * @param penaltyValues
  *   The penalty values applied to a sequencer for each kind of adverse event.
  */
final case class SequencerConnectionPenaltyConfig(
    enabled: Boolean,
    decayInterval: PositiveFiniteDuration,
    halfLife: PositiveFiniteDuration,
    zeroThreshold: PositiveDouble,
    penaltyValues: PenaltyValues,
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
      penaltyValues = Some(penaltyValues.toProtoV30),
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
    param("penaltyValues", _.penaltyValues),
  )

  val default: SequencerConnectionPenaltyConfig = SequencerConnectionPenaltyConfig(
    enabled = true,
    decayInterval = PositiveFiniteDuration.tryOfSeconds(10),
    halfLife = PositiveFiniteDuration.tryOfSeconds(60),
    zeroThreshold = PositiveDouble.tryCreate(0.01),
    penaltyValues = PenaltyValues.default,
  )

  private[sequencing] def fromProtoV30(
      proto: v30.SequencerConnectionPenaltyConfig
  ): ParsingResult[SequencerConnectionPenaltyConfig] = {
    val v30.SequencerConnectionPenaltyConfig(
      enabledP,
      decayIntervalP,
      halfLifeP,
      zeroThresholdP,
      penaltyValuesP,
    ) = proto

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
      penaltyValues <- ProtoConverter.parseRequired(
        PenaltyValues.fromProtoV30,
        "penalty_values",
        penaltyValuesP,
      )
    } yield SequencerConnectionPenaltyConfig(
      enabled = enabledP,
      decayInterval = decayInterval,
      halfLife = halfLife,
      zeroThreshold = zeroThreshold,
      penaltyValues = penaltyValues,
    )
  }
}

/** Penalty values applied to a sequencer for each kind of adverse event. A higher value makes the
  * sequencer less preferred for longer.
  *
  * @param sequencerOverloaded
  *   The sequencer refused a submission request because it is overloaded.
  * @param subscriptionError
  *   The subscription to the sequencer failed with an error.
  * @param subscriptionPermissionDenied
  *   The subscription to the sequencer failed because permission was denied.
  * @param subscriptionInactive
  *   The subscription to the sequencer became inactive.
  * @param subscriptionAggregationError
  *   The sequencer's events caused an error in the aggregation of the subscriptions.
  * @param subscriptionHandlerException
  *   The handling of the sequencer's events raised an exception.
  * @param subscriptionRetriableFailure
  *   The subscription to the sequencer failed with a retriable failure.
  * @param subscriptionNonRetriableFailure
  *   The subscription to the sequencer failed with a non-retriable failure.
  */
final case class PenaltyValues(
    sequencerOverloaded: PositiveInt,
    subscriptionError: PositiveInt,
    subscriptionPermissionDenied: PositiveInt,
    subscriptionInactive: PositiveInt,
    subscriptionAggregationError: PositiveInt,
    subscriptionHandlerException: PositiveInt,
    subscriptionRetriableFailure: PositiveInt,
    subscriptionNonRetriableFailure: PositiveInt,
) extends PrettyPrintingFromCompanion {

  // Penalties are computed once per instance
  /** The penalty to apply for the given kind of adverse event. */
  def penalty(kind: PenaltyKind): Penalty = kind match {
    case PenaltyKind.SequencerOverloaded => sequencerOverloadedPenalty
    case PenaltyKind.SubscriptionError => subscriptionErrorPenalty
    case PenaltyKind.SubscriptionPermissionDenied => subscriptionPermissionDeniedPenalty
    case PenaltyKind.SubscriptionInactive => subscriptionInactivePenalty
    case PenaltyKind.SubscriptionAggregationError => subscriptionAggregationErrorPenalty
    case PenaltyKind.SubscriptionHandlerException => subscriptionHandlerExceptionPenalty
    case PenaltyKind.SubscriptionRetriableFailure => subscriptionRetriableFailurePenalty
    case PenaltyKind.SubscriptionNonRetriableFailure => subscriptionNonRetriableFailurePenalty
    case PenaltyKind.Fixed(fixed) => fixed
  }

  private lazy val sequencerOverloadedPenalty: Penalty =
    Penalty.create(sequencerOverloaded)
  private lazy val subscriptionErrorPenalty: Penalty = Penalty.create(subscriptionError)
  private lazy val subscriptionPermissionDeniedPenalty: Penalty =
    Penalty.create(subscriptionPermissionDenied)
  private lazy val subscriptionInactivePenalty: Penalty =
    Penalty.create(subscriptionInactive)
  private lazy val subscriptionAggregationErrorPenalty: Penalty =
    Penalty.create(subscriptionAggregationError)
  private lazy val subscriptionHandlerExceptionPenalty: Penalty =
    Penalty.create(subscriptionHandlerException)
  private lazy val subscriptionRetriableFailurePenalty: Penalty =
    Penalty.create(subscriptionRetriableFailure)
  private lazy val subscriptionNonRetriableFailurePenalty: Penalty =
    Penalty.create(subscriptionNonRetriableFailure)

  private[sequencing] def toProtoV30: v30.SequencerConnectionPenaltyConfig.PenaltyValues =
    v30.SequencerConnectionPenaltyConfig.PenaltyValues(
      sequencerOverloaded = sequencerOverloaded.unwrap,
      subscriptionError = subscriptionError.unwrap,
      subscriptionPermissionDenied = subscriptionPermissionDenied.unwrap,
      subscriptionInactive = subscriptionInactive.unwrap,
      subscriptionAggregationError = subscriptionAggregationError.unwrap,
      subscriptionHandlerException = subscriptionHandlerException.unwrap,
      subscriptionRetriableFailure = subscriptionRetriableFailure.unwrap,
      subscriptionNonRetriableFailure = subscriptionNonRetriableFailure.unwrap,
    )

  override def prettyCompanion: PrettyPrintingCompanion[PenaltyValues] = PenaltyValues
}

object PenaltyValues extends PrettyPrintingCompanion[PenaltyValues] {
  override protected val pretty: Pretty[PenaltyValues] = prettyOfClass(
    param("sequencerOverloaded", _.sequencerOverloaded),
    param("subscriptionError", _.subscriptionError),
    param("subscriptionPermissionDenied", _.subscriptionPermissionDenied),
    param("subscriptionInactive", _.subscriptionInactive),
    param("subscriptionAggregationError", _.subscriptionAggregationError),
    param("subscriptionHandlerException", _.subscriptionHandlerException),
    param("subscriptionRetriableFailure", _.subscriptionRetriableFailure),
    param("subscriptionNonRetriableFailure", _.subscriptionNonRetriableFailure),
  )

  val default: PenaltyValues = PenaltyValues(
    sequencerOverloaded = PositiveInt.one,
    subscriptionError = PositiveInt.one,
    subscriptionPermissionDenied = PositiveInt.tryCreate(5),
    subscriptionInactive = PositiveInt.one,
    subscriptionAggregationError = PositiveInt.tryCreate(10),
    subscriptionHandlerException = PositiveInt.tryCreate(15),
    subscriptionRetriableFailure = PositiveInt.tryCreate(10),
    subscriptionNonRetriableFailure = PositiveInt.tryCreate(20),
  )

  private[sequencing] def fromProtoV30(
      proto: v30.SequencerConnectionPenaltyConfig.PenaltyValues
  ): ParsingResult[PenaltyValues] =
    for {
      sequencerOverloaded <- ProtoConverter.parsePositiveInt(
        "sequencer_overloaded",
        proto.sequencerOverloaded,
      )
      subscriptionError <- ProtoConverter.parsePositiveInt(
        "subscription_error",
        proto.subscriptionError,
      )
      subscriptionPermissionDenied <- ProtoConverter.parsePositiveInt(
        "subscription_permission_denied",
        proto.subscriptionPermissionDenied,
      )
      subscriptionInactive <- ProtoConverter.parsePositiveInt(
        "subscription_inactive",
        proto.subscriptionInactive,
      )
      subscriptionAggregationError <- ProtoConverter.parsePositiveInt(
        "subscription_aggregation_error",
        proto.subscriptionAggregationError,
      )
      subscriptionHandlerException <- ProtoConverter.parsePositiveInt(
        "subscription_handler_exception",
        proto.subscriptionHandlerException,
      )
      subscriptionRetriableFailure <- ProtoConverter.parsePositiveInt(
        "subscription_retriable_failure",
        proto.subscriptionRetriableFailure,
      )
      subscriptionNonRetriableFailure <- ProtoConverter.parsePositiveInt(
        "subscription_non_retriable_failure",
        proto.subscriptionNonRetriableFailure,
      )
    } yield PenaltyValues(
      sequencerOverloaded,
      subscriptionError,
      subscriptionPermissionDenied,
      subscriptionInactive,
      subscriptionAggregationError,
      subscriptionHandlerException,
      subscriptionRetriableFailure,
      subscriptionNonRetriableFailure,
    )
}
