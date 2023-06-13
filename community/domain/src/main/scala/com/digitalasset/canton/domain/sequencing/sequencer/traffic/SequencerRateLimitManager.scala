// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Holds the traffic control state and control rate limiting logic of members of a sequencer
  */
trait SequencerRateLimitManager {

  /** Return the rate limit status for all members known to the manager
    */
  def rateLimitsStatus: Seq[MemberTrafficStatus]

  /** Return the traffic status for a single member
    */
  def trafficStateForMember(member: Member): Option[MemberTrafficStatus]

  /** Load the traffic control state of the member at the given timestamp.
    * @param member member to load
    * @param at timestamp to load data from
    */
  def loadMember(member: Member, at: CantonTimestamp)(implicit
      ec: ExecutionContext
  ): EitherT[Future, SequencerRateLimitError, Unit]

  /** Register a new member with its initial traffic status
    */
  def register(member: Member, initialStatus: MemberTrafficStatus)(implicit
      ec: ExecutionContext
  ): Future[Unit]

  /** Top up a member with its new extra traffic limit. Must be strictly increasing between subsequent calls.
    * @param member member to top up
    * @param newExtraTrafficTotal new limit
    * @param timestamp timestamp at which the top up will be effective
    */
  def topUp(
      member: Member,
      newExtraTrafficTotal: PositiveLong,
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
  ): EitherT[
    Future,
    SequencerRateLimitError,
    Unit,
  ]
}

sealed trait SequencerRateLimitError
object SequencerRateLimitError {
  final case class UnknownMember(member: Member) extends SequencerRateLimitError
  final case class AboveTrafficLimit(
      trafficCost: NonNegativeLong,
      extraTrafficRemainder: NonNegativeLong,
      remainingBaseTraffic: NonNegativeLong,
  ) extends SequencerRateLimitError
}
