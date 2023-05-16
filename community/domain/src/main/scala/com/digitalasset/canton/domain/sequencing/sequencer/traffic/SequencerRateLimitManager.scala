// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait SequencerRateLimitManager {

  def rateLimitsStatus: Seq[MemberTrafficStatus]

  def trafficStateForMember(member: Member): Option[MemberTrafficStatus]

  def register(member: Member, initialStatus: MemberTrafficStatus): Unit

  def topUp(
      member: Member,
      newExtraTrafficTotal: NonNegativeLong,
      timestamp: CantonTimestamp,
  ): Either[SequencerRateLimitError.UnknownMember, Unit]

  /** Consume the traffic costs of the submission request from the sender's traffic state.
    *
    * NOTE: This method must be called in order of the sequencing timestamps.
    */
  def consume(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, SequencerRateLimitError, Unit]
}

sealed trait SequencerRateLimitError
object SequencerRateLimitError {
  final case class UnknownMember(member: Member) extends SequencerRateLimitError
  final case class AboveTrafficLimit(
      trafficCost: Long,
      extraTrafficRemainder: Long,
      remainingBaseTraffic: Long,
  ) extends SequencerRateLimitError
}
