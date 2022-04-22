// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty}
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.LocalReject.Malformed
import com.digitalasset.canton.protocol.messages.Verdict.{Approve, MediatorReject, RejectReasons}
import com.digitalasset.canton.protocol.messages.{MediatorRequest, _}
import com.digitalasset.canton.protocol.{RequestId, ViewHash}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.util.ShowUtil._

import scala.concurrent.{ExecutionContext, Future}

object ResponseAggregation {
  final case class ViewState(
      pendingConfirmingParties: Set[ConfirmingParty],
      distanceToThreshold: Int,
      rejections: List[(Set[LfPartyId], LocalReject)],
  ) extends PrettyPrinting {
    lazy val totalAvailableWeight: Int = pendingConfirmingParties.foldLeft(0)(_ + _.weight)

    override def pretty: Pretty[ViewState] = {
      prettyOfClass(
        param("distanceToThreshold", _.distanceToThreshold),
        param("pendingConfirmingParties", _.pendingConfirmingParties),
        param("rejections", _.rejections),
      )
    }
  }

  private def initialState(request: MediatorRequest): Either[Verdict, Map[ViewHash, ViewState]] = {
    val minimumThreshold = request.confirmationPolicy.minimumThreshold
    request.informeesAndThresholdByView.toList
      .traverse { case (viewHash, (informees, threshold)) =>
        val confirmingParties = informees.collect { case c: ConfirmingParty => c }
        Either.cond(
          threshold >= minimumThreshold,
          viewHash -> ViewState(confirmingParties, threshold.unwrap, Nil),
          MediatorReject.MaliciousSubmitter.ViewThresholdBelowMinimumThreshold
            .Reject(show"viewHash=$viewHash, threshold=$threshold"),
        )
      }
      .map(_.toMap)
  }

  def apply(requestId: RequestId, request: MediatorRequest)(
      loggerFactory: NamedLoggerFactory
  )(implicit traceContext: TraceContext): ResponseAggregation = {
    val version = requestId.unwrap
    val initial = for {
      pending <- initialState(request)
      _ <- Either.cond(pending.values.exists(_.distanceToThreshold > 0), (), Approve)
      existsQuorum = pending.values.forall(state =>
        state.totalAvailableWeight >= state.distanceToThreshold
      )
      _ <- Either.cond(
        existsQuorum,
        (), {
          val failed = pending.filterNot { case (_, v) =>
            v.totalAvailableWeight >= v.distanceToThreshold
          }.keys
          MediatorReject.MaliciousSubmitter.NotEnoughConfirmingParties.Reject(failed.toString)
        },
      )
    } yield pending
    new ResponseAggregation(requestId, request, version, initial)(
      requestTraceContext = traceContext
    )(loggerFactory)
  }

  def apply(
      requestId: RequestId,
      request: MediatorRequest,
      version: CantonTimestamp,
      verdict: Verdict,
      requestTraceContext: TraceContext,
  )(loggerFactory: NamedLoggerFactory): ResponseAggregation =
    new ResponseAggregation(requestId, request, version, Left(verdict))(requestTraceContext)(
      loggerFactory
    )

  private[this] def apply(
      requestId: RequestId,
      request: MediatorRequest,
      version: CantonTimestamp,
      state: Either[Verdict, Map[ViewHash, ViewState]],
  )(loggerFactory: NamedLoggerFactory): ResponseAggregation =
    throw new UnsupportedOperationException("Use the other apply methods")
}

/** @param requestId The request Id of the [[com.digitalasset.canton.protocol.messages.InformeeMessage]]
  * @param version   The sequencer timestamp of the most recent message that affected this [[ResponseAggregation]]
  * @param state     If the [[com.digitalasset.canton.protocol.messages.InformeeMessage]] has been finalized,
  *                  this will be a `Left`
  *                  otherwise  a `Right` which shows which transaction view hashes are not confirmed yet.
  * @param requestTraceContext we retain the original trace context from the initial confirmation request
  *                            for raising timeouts to help with debugging. this ideally would be the same trace
  *                            context throughout all responses could not be in a distributed setup so this is not
  *                            validated anywhere. Intentionally supplied in a separate parameter list to avoid being
  *                            included in equality checks.
  */
case class ResponseAggregation private (
    requestId: RequestId,
    request: MediatorRequest,
    version: CantonTimestamp,
    state: Either[Verdict, Map[ViewHash, ViewState]],
)(val requestTraceContext: TraceContext)(protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging
    with PrettyPrinting {

  def isFinalized: Boolean = state.isLeft

  /** Record the additional confirmation response received. */
  def progress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, ResponseAggregationError, ResponseAggregation] = {
    val MediatorResponse(
      _requestId,
      sender,
      viewHashO,
      localVerdict,
      rootHashO,
      confirmingParties,
      _domainId,
    ) =
      response
    val requiredTrustLevel = request.confirmationPolicy.requiredTrustLevel

    def authorizedPartiesOfSender(
        viewHash: ViewHash,
        declaredConfirmingParties: Set[LfPartyId],
    ): EitherT[Future, ResponseAggregationError, Set[LfPartyId]] =
      localVerdict match {
        case malformed: Malformed =>
          reportMalformed(sender, requestId, malformed)
          val hostedConfirmingPartiesF =
            declaredConfirmingParties.toList
              .filterA(p => topologySnapshot.canConfirm(sender, p, requiredTrustLevel))
              .map(_.toSet)
          val res = hostedConfirmingPartiesF.map { hostedConfirmingParties =>
            logger.debug(
              show"Malformed response $responseTimestamp for $viewHash considered as a rejection on behalf of $hostedConfirmingParties"
            )
            Right(hostedConfirmingParties): Either[ResponseAggregationError, Set[LfPartyId]]
          }
          EitherT(res)

        case LocalApprove | _: LocalReject =>
          val unexpectedConfirmingParties = confirmingParties -- declaredConfirmingParties
          for {
            _ <- EitherT.cond[Future](
              unexpectedConfirmingParties.isEmpty,
              (),
              UnexpectedMediatorResponse(
                requestId,
                viewHash,
                unexpectedConfirmingParties,
              ): ResponseAggregationError,
            )
            unauthorizedConfirmingParties <- EitherT.right(
              confirmingParties.toList
                .filterA(p =>
                  topologySnapshot.canConfirm(sender, p, requiredTrustLevel).map(x => !x)
                )
                .map(_.toSet)
            )
            _ <- EitherT.cond[Future](
              unauthorizedConfirmingParties.isEmpty,
              (),
              UnauthorizedMediatorResponse(
                requestId,
                viewHash,
                sender,
                unauthorizedConfirmingParties,
              ): ResponseAggregationError,
            )
          } yield confirmingParties
      }

    def progressView(
        statesOfViews: Map[ViewHash, ViewState],
        viewHashAndParties: (ViewHash, Set[LfPartyId]),
    ): Either[Verdict, Map[ViewHash, ViewState]] = {
      val (viewHash, authorizedParties) = viewHashAndParties
      val stateOfView = statesOfViews.getOrElse(
        viewHash,
        throw new IllegalArgumentException(
          s"The transaction view hash $viewHashO is not covered by the request"
        ),
      )
      val ViewState(pendingConfirmingParties, distanceToThreshold, rejections) = stateOfView
      val (newlyResponded, stillPending) =
        pendingConfirmingParties.partition(cp => authorizedParties.contains(cp.party))

      logger.debug(
        show"$requestId(view hash $viewHash): Received verdict $localVerdict for pending parties $newlyResponded by participant $sender. " +
          show"Still waiting for $stillPending."
      )
      val alreadyResponded = authorizedParties -- newlyResponded.map(_.party)
      // Because some of the responders might have had some other participant already confirmed on their behalf
      // we ignore those responders
      if (alreadyResponded.nonEmpty) logger.debug(s"Ignored responses from $alreadyResponded")

      if (newlyResponded.isEmpty) {
        logger.debug(
          s"Nothing to do upon response from $sender for $requestId(view hash $viewHash) because no new responders"
        )
        Either.right[Verdict, Map[ViewHash, ViewState]](statesOfViews)
      } else {
        localVerdict match {
          case LocalApprove =>
            val contribution = newlyResponded.foldLeft(0)(_ + _.weight)
            val nextViewState =
              ViewState(stillPending, distanceToThreshold - contribution, rejections)
            val nextStatesOfViews = statesOfViews + (viewHash -> nextViewState)
            Either.cond(
              nextStatesOfViews.values.exists(_.distanceToThreshold > 0),
              nextStatesOfViews,
              Approve,
            )

          case rejection: LocalReject =>
            val nextViewState = ViewState(
              stillPending,
              distanceToThreshold,
              (authorizedParties -> rejection) :: rejections,
            )
            Either.cond(
              nextViewState.distanceToThreshold <= nextViewState.totalAvailableWeight,
              statesOfViews + (viewHash -> nextViewState),
              // TODO(#5337): Don't discard the rejection reasons of the other views.
              RejectReasons(nextViewState.rejections),
            )
        }
      }
    }

    for {
      statesOfViews <- EitherT.fromEither[Future](state.leftMap { s =>
        logger.debug(
          s"Request ${requestId.unwrap} has already been finalized with verdict $s before response $responseTimestamp from $sender with $localVerdict for view $viewHashO arrives"
        )
        MediatorRequestAlreadyFinalized(requestId, s)
      })
      _ <- EitherT.fromEither[Future](rootHashO.traverse_ { rootHash =>
        Either.cond(
          request.rootHash.forall(_ == rootHash),
          (),
          MediatorRequestNotFound(requestId, viewHashO, Some(rootHash)),
        )
      })

      viewHashesAndParties <- {
        val tmp: EitherT[Future, ResponseAggregationError, List[(ViewHash, Set[LfPartyId])]] =
          viewHashO match {
            case None =>
              // If no view hash is given, the local verdict is Malformed and confirming parties is empty by the invariants of MediatorResponse.
              // We treat this as a rejection for all parties hosted by the participant.
              localVerdict match {
                case malformed: Malformed => reportMalformed(sender, requestId, malformed)
                case other =>
                  ErrorUtil.requireState(
                    condition = false,
                    s"Verdict should be of type malformed, but got $other",
                  )
              }

              val informeesByView = request.informeesAndThresholdByView
              val ret = informeesByView.toList
                .traverseFilter { case (viewHash, (informees, _threshold)) =>
                  val hostedConfirmingPartiesF = informees.toList.traverseFilter {
                    case ConfirmingParty(party, _) =>
                      topologySnapshot
                        .canConfirm(sender, party, requiredTrustLevel)
                        .map(x => if (x) Some(party) else None)
                    case _ => Future.successful(None)
                  }
                  hostedConfirmingPartiesF.map { hostedConfirmingParties =>
                    if (hostedConfirmingParties.nonEmpty)
                      Some(viewHash -> hostedConfirmingParties.toSet)
                    else None
                  }
                }
                .map { viewsWithConfirmingPartiesForSender =>
                  logger.debug(
                    s"Malformed response $responseTimestamp from $sender considered as a rejection for ${viewsWithConfirmingPartiesForSender}"
                  )
                  viewsWithConfirmingPartiesForSender
                }
              EitherT.right(ret)
            case Some(viewHash) =>
              for {
                informeesAndThreshold <- EitherT.fromEither[Future](
                  request.informeesAndThresholdByView
                    .get(viewHash)
                    .toRight(MediatorRequestNotFound(requestId, viewHashO, rootHashO))
                )
                (informees, _) = informeesAndThreshold
                declaredConfirmingParties = informees.collect { case ConfirmingParty(party, _) =>
                  party
                }
                authorizedConfirmingParties <- authorizedPartiesOfSender(
                  viewHash,
                  declaredConfirmingParties,
                )
              } yield List(viewHash -> authorizedConfirmingParties)
          }
        tmp
      }
    } yield {
      val updatedState = MonadUtil.foldLeftM(statesOfViews, viewHashesAndParties)(progressView)
      copy(version = responseTimestamp, state = updatedState)(requestTraceContext)(loggerFactory)
    }
  }

  private def reportMalformed(sender: ParticipantId, requestId: RequestId, reason: Malformed)(
      implicit traceContext: TraceContext
  ): Unit = {
    // TODO(M40): Make this an alarm
    //  Requires a refactoring, because currently the method can either return the next ResponseAggregation or an alarm.
    //  But in this case, it needs to return both.
    reason.logWithContext(Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender"))
  }

  def timeout(version: CantonTimestamp) =
    new ResponseAggregation(
      this.requestId,
      this.request,
      version,
      Left(MediatorReject.Timeout.Reject()),
    )(requestTraceContext)(loggerFactory)

  override def pretty: Pretty[ResponseAggregation] = prettyOfClass(
    param("id", _.requestId),
    param("request", _.request),
    param("version", _.version),
    param("state", _.state),
  )
}
