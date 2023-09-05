// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, Informee}
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{
  LocalReject,
  MediatorRequest,
  MediatorResponse,
  Verdict,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait ResponseAggregation extends PrettyPrinting {

  /** Type used to uniquely identify a view.
    */
  type VKEY <: PrettyPrinting

  /** The request id of the [[com.digitalasset.canton.protocol.messages.InformeeMessage]]
    */
  def requestId: RequestId

  def request: MediatorRequest

  /** We retain the original trace context from the initial confirmation request
    * for raising timeouts to help with debugging. this ideally would be the same trace
    * context throughout all responses could not be in a distributed setup so this is not
    * validated anywhere. Intentionally supplied in a separate parameter list to avoid being
    * included in equality checks.
    */
  def requestTraceContext: TraceContext

  /** The sequencer timestamp of the most recent message that affected this [[ResponseAggregation]]
    */
  def version: CantonTimestamp

  /** If the [[com.digitalasset.canton.protocol.messages.InformeeMessage]] has been finalized,
    * this will be a `Left`
    * otherwise  a `Right` which shows which transaction view hashes are not confirmed yet.
    */
  def state: Either[Verdict, Map[VKEY, ViewState]]

  def verdict: Option[Verdict] = state.swap.toOption

  def isFinalized: Boolean = verdict.isDefined

  /** Record the additional confirmation response received. */
  def progress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregation]]

  def timeout(version: CantonTimestamp): ResponseAggregation

  def withVersion(version: CantonTimestamp): ResponseAggregation

  def withRequestId(requestId: RequestId): ResponseAggregation

}

object ResponseAggregation {

  final case class ConsortiumVotingState(
      threshold: PositiveInt = PositiveInt.one,
      approvals: Set[ParticipantId] = Set.empty,
      rejections: Set[ParticipantId] = Set.empty,
  ) extends PrettyPrinting {
    def approveBy(participant: ParticipantId): ConsortiumVotingState = {
      this.copy(approvals = this.approvals + participant)
    }

    def rejectBy(participant: ParticipantId): ConsortiumVotingState = {
      this.copy(rejections = this.rejections + participant)
    }

    def isApproved: Boolean = approvals.size >= threshold.value

    def isRejected: Boolean = rejections.size >= threshold.value

    override def pretty: Pretty[ConsortiumVotingState] = {
      prettyOfClass(
        paramIfTrue("consortium party", _.threshold.value > 1),
        paramIfTrue("non-consortium party", _.threshold.value == 1),
        param("threshold", _.threshold, _.threshold.value > 1),
        param("approved by participants", _.approvals),
        param("rejected by participants", _.rejections),
      )
    }
  }

  final case class ViewState(
      pendingConfirmingParties: Set[ConfirmingParty],
      consortiumVoting: Map[
        LfPartyId,
        ConsortiumVotingState,
      ], // pendingConfirmingParties is always a subset of consortiumVoting.keys()
      distanceToThreshold: Int,
      rejections: List[(Set[LfPartyId], LocalReject)],
  ) extends PrettyPrinting {

    lazy val totalAvailableWeight: Int = pendingConfirmingParties.map(_.weight.unwrap).sum

    override def pretty: Pretty[ViewState] = {
      prettyOfClass(
        param("distanceToThreshold", _.distanceToThreshold),
        param("pendingConfirmingParties", _.pendingConfirmingParties),
        param("consortiumVoting", _.consortiumVoting),
        param("rejections", _.rejections),
      )
    }
  }

  /** Creates a non-finalized response aggregation from a request.
    */
  def fromRequest(
      requestId: RequestId,
      request: MediatorRequest,
      protocolVersion: ProtocolVersion,
      topologySnapshot: TopologySnapshot,
  )(loggerFactory: NamedLoggerFactory)(implicit
      requestTraceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[ResponseAggregation] =
    if (protocolVersion >= ProtocolVersion.v5) {
      for {
        initialState <- mkInitialState(
          request.informeesAndThresholdByViewPosition,
          topologySnapshot,
        )
      } yield {
        ResponseAggregationV5(
          requestId,
          request,
          requestId.unwrap,
          Right(initialState),
        )(
          protocolVersion = protocolVersion,
          requestTraceContext = requestTraceContext,
        )(loggerFactory)
      }
    } else {
      for {
        initialState <- mkInitialState(request.informeesAndThresholdByViewHash, topologySnapshot)
      } yield {
        ResponseAggregationV0(
          requestId,
          request,
          requestId.unwrap,
          Right(initialState),
        )(
          protocolVersion = protocolVersion,
          requestTraceContext = requestTraceContext,
        )(loggerFactory)
      }
    }

  private def mkInitialState[K](
      informeesAndThresholdByView: Map[K, (Set[Informee], NonNegativeInt)],
      topologySnapshot: TopologySnapshot,
  )(implicit ec: ExecutionContext): Future[Map[K, ViewState]] = {
    informeesAndThresholdByView.toSeq
      .parTraverse { case (viewKey, (informees, threshold)) =>
        val confirmingParties = informees.collect { case cp: ConfirmingParty => cp }
        for {
          votingThresholds <- topologySnapshot.consortiumThresholds(confirmingParties.map(_.party))
        } yield {
          val consortiumVotingState = votingThresholds.map { case (party, threshold) =>
            (party -> ConsortiumVotingState(threshold))
          }
          viewKey -> ViewState(
            confirmingParties,
            consortiumVotingState,
            threshold.unwrap,
            rejections = Nil,
          )
        }
      }
      .map(_.toMap)
  }

  /** Creates a finalized response aggregation from a verdict.
    */
  def fromVerdict(
      requestId: RequestId,
      request: MediatorRequest,
      version: CantonTimestamp,
      verdict: Verdict,
  )(protocolVersion: ProtocolVersion, loggerFactory: NamedLoggerFactory)(implicit
      requestTraceContext: TraceContext
  ): ResponseAggregation = {
    if (protocolVersion < ProtocolVersion.v5)
      ResponseAggregationV0(requestId, request, version, Left(verdict))(
        protocolVersion,
        requestTraceContext,
      )(loggerFactory)
    else
      ResponseAggregationV5(requestId, request, version, Left(verdict))(
        protocolVersion,
        requestTraceContext,
      )(loggerFactory)
  }
}
