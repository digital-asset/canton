// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.traverse.*
import com.digitalasset.canton.admin.sequencer.v30
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification.minimumConfirmationResponsePatience
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.LoggerUtil

/** Configures the submission request amplification. Amplification makes sequencer clients send
  * eligible submission requests to multiple sequencers to overcome message loss in faulty
  * sequencers.
  *
  * @param factor
  *   The maximum number of times the submission request shall be sent.
  * @param patience
  *   How long the sequencer client should wait after an acknowledged submission to a sequencer to
  *   observe the receipt or error before it attempts to send the submission request again (possibly
  *   to a different sequencer).
  * @param confirmationResponseFactorO
  *   If defined, overrides [[factor]] when sending confirmation responses.
  * @param confirmationResponsePatienceO
  *   If defined, overrides [[patience]] when sending confirmation responses.
  */
final case class SubmissionRequestAmplification(
    factor: PositiveInt,
    patience: NonNegativeFiniteDuration,
    confirmationResponseFactorO: Option[PositiveInt] = None,
    confirmationResponsePatienceO: Option[NonNegativeFiniteDuration] = None,
) extends PrettyPrinting {

  private[sequencing] def toProtoV30: v30.SubmissionRequestAmplification =
    v30.SubmissionRequestAmplification(
      factor = factor.unwrap,
      patience = Some(patience.toProtoPrimitive),
      confirmationResponseFactor = confirmationResponseFactorO.map(_.unwrap),
      confirmationResponsePatience = confirmationResponsePatienceO.map(_.toProtoPrimitive),
    )

  override protected def pretty: Pretty[SubmissionRequestAmplification] = prettyOfClass(
    param("factor", _.factor),
    param("patience", _.patience),
    paramIfDefined("confirmationResponseFactor", _.confirmationResponseFactorO),
    paramIfDefined("confirmationResponsePatience", _.confirmationResponsePatienceO),
  )

  def getActual(
      useConfirmationResponseParameters: Boolean
  ): (PositiveInt, NonNegativeFiniteDuration) =
    if (useConfirmationResponseParameters)
      (
        confirmationResponseFactorO.getOrElse(factor),
        confirmationResponsePatienceO.getOrElse(patience),
      )
    else (factor, patience)

  /** Entry point to perform validity checks. To be used by gRPC service implementations that need
    * to enforce them.
    */
  def validate: Either[String, Unit] = Either.cond(
    confirmationResponsePatienceO.forall(_ >= minimumConfirmationResponsePatience),
    (),
    s"Confirmation response patience $confirmationResponsePatienceO should be at least ${LoggerUtil
        .roundDurationForHumans(minimumConfirmationResponsePatience.duration)}",
  )
}

object SubmissionRequestAmplification {
  val NoAmplification: SubmissionRequestAmplification =
    SubmissionRequestAmplification(PositiveInt.one, NonNegativeFiniteDuration.Zero)

  private val minimumConfirmationResponsePatience = NonNegativeFiniteDuration.tryOfSeconds(1)

  private[sequencing] def fromProtoV30(
      proto: v30.SubmissionRequestAmplification
  ): ParsingResult[SubmissionRequestAmplification] = {
    val v30.SubmissionRequestAmplification(
      factorP,
      patienceP,
      confirmationResponseFactorOP,
      confirmationResponsePatienceOP,
    ) = proto
    for {
      factor <- ProtoConverter.parsePositiveInt("factor", factorP)
      patience <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("patience"),
        "patience",
        patienceP,
      )
      confirmationResponseFactorO <- confirmationResponseFactorOP.traverse(
        ProtoConverter.parsePositiveInt("confirmation_response_factor", _)
      )
      confirmationResonsePatienceO <- confirmationResponsePatienceOP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("confirmation_response_patience")
      )
    } yield SubmissionRequestAmplification(
      factor,
      patience,
      confirmationResponseFactorO,
      confirmationResonsePatienceO,
    )
  }
}
