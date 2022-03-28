// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.KillSwitch
import akka.stream.scaladsl.Source
import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.String256M
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  CreateSubscriptionError,
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SubmissionRequest}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

/** Errors from pruning */
sealed trait PruningError
object PruningError {

  /** The sequencer implementation does not support pruning */
  case object NotSupported extends PruningError

  /** The requested timestamp would cause data for enabled members to be removed potentially permanently breaking them. */
  case class UnsafePruningPoint(requestedTimestamp: CantonTimestamp, safeTimestamp: CantonTimestamp)
      extends PruningError
}

/** Interface for sequencer operations.
  * The default [[DatabaseSequencer]] implementation is backed by a database run by a single operator.
  * Other implementations support operating a Sequencer on top of third party ledgers or other infrastructure.
  */
trait Sequencer extends AutoCloseable {
  protected val loggerFactory: NamedLoggerFactory

  def isRegistered(member: Member)(implicit traceContext: TraceContext): Future[Boolean]
  def registerMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit]

  /** Always returns false for Sequencer drivers that don't support ledger identity authorization. Otherwise returns
    *  whether the given ledger identity is registered on the underlying ledger (and configured smart contract).
    */
  def isLedgerIdentityRegistered(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Currently this method is only implemented by the enterprise-only Ethereum driver. It immediately returns a Left
    * for ledgers where it is not implemented.
    *
    * This method authorizes a [[com.digitalasset.canton.domain.sequencing.sequencer.LedgerIdentity]] on the underlying ledger.
    * In the Ethereum-backed ledger, this enables the given Ethereum account to also write to the deployed
    * `Sequencer.sol` contract. Therefore, this method needs to be called before being able to use an Ethereum sequencer
    * with a given Ethereum account.
    *
    * NB: in Ethereum, this method needs to be called by an Ethereum sequencer whose associated Ethereum account is
    * already authorized. Else the authorization itself will fail.
    * To bootstrap the authorization, the Ethereum account that deploys the `Sequencer.sol` contract is the first account
    * to be authorized.
    */
  def authorizeLedgerIdentity(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]

  def sendAsync(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]
  def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource]

  /** Acknowledge that a member has successfully handled all events up to and including the timestamp provided.
    * Makes earlier events for this member available for pruning.
    * The timestamp is in sequencer time and will likely correspond to an event that the client has processed however
    * this is not validated.
    * It is assumed that members in consecutive calls will never acknowledge an earlier timestamp however this is also
    * not validated (and could be invalid if the member has many subscriptions from the same or many processes).
    * It is expected that members will periodically call this endpoint with their latest clean timestamp rather than
    * calling it for every event they process. The default interval is in the range of once a minute.
    */
  def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Return a structure containing the members registered with the sequencer and the latest positions of clients
    * reading events.
    */
  def pruningStatus(implicit traceContext: TraceContext): Future[SequencerPruningStatus]

  /** Return a structure indicating the health status of the sequencer implementation.
    * Should succeed even if the configured datastore is unavailable.
    */
  def health(implicit traceContext: TraceContext): Future[SequencerHealthStatus]

  /** Prune as much sequencer data as safely possible without breaking operation (except for members
    * that have been previously flagged as disabled).
    * Sequencers are permitted to prune to an earlier timestamp if required to for their own consistency.
    * For example, the Database Sequencer will adjust this time to a potentially earlier point in time where
    * counter checkpoints are available for all members (who aren't being ignored).
    */
  def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningError, String]

  /** Return a snapshot state that other newly onboarded sequencers can use as an initial state
    * from which to support serving events. This state depends on the provided timestamp
    * and will contain registered members, counters per member, latest timestamp (which will be greater than
    * or equal to the provided timestamp) as well as a sequencer implementation specific piece of information
    * such that all together form the point after which the new sequencer can safely operate.
    * The provided timestamp is typically the timestamp of the requesting sequencer's private key,
    * which is the point in time where it can effectively sign events.
    */
  def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SequencerSnapshot]

  /** First check is the member is registered and if not call `registerMember` */
  def ensureRegistered(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit] =
    for {
      isRegistered <- EitherT.right[SequencerWriteError[RegisterMemberError]](isRegistered(member))
      _ <- EitherTUtil.ifThenET(!isRegistered)(registerMember(member))
    } yield ()

  /** Disable the provided member. Should prevent them from reading or writing in the future (although they can still be addressed).
    * Their unread data can also be pruned.
    * Effectively disables all instances of this member.
    */
  def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit]
}

object Sequencer {
  type EventSource = Source[OrdinarySerializedEvent, KillSwitch]

  def validateSigningTimestamp(
      signingTolerance: NonNegativeFiniteDuration
  )(now: CantonTimestamp, requestedSigningTimestamp: CantonTimestamp): Either[String256M, Unit] = {
    val lowerBoundExcl = now.minus(signingTolerance.unwrap)
    // TODO(i4638): The upper bound is too high. The highest acceptable value is the timestamp of the recent identity snapshot.
    //  We should distinguish between a point in time that's truly in the future (then the sender is misbehaving) and
    //  the sequencer not yet having caught up (we can then think about rejecting the SubmissionRequest
    //  with an appropriate reason so that the sender can resend the request later).
    val upperBoundIncl = now

    for {
      _ <- Either.cond(
        requestedSigningTimestamp > lowerBoundExcl,
        (),
        String256M.tryCreate(
          s"Invalid signing timestamp $requestedSigningTimestamp. The signing timestamp must be strictly after $lowerBoundExcl."
        ),
      )
      _ <- Either.cond(
        requestedSigningTimestamp <= upperBoundIncl,
        (),
        String256M.tryCreate(
          s"Invalid signing timestamp $requestedSigningTimestamp. The signing timestamp must be before or at $upperBoundIncl."
        ),
      )
    } yield ()
  }
}
