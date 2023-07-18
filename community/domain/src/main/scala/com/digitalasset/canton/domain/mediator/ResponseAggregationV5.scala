// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.OptionT
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, ViewPosition}
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.Verdict.{Approve, ParticipantReject}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[mediator] final case class ResponseAggregationV5(
    requestId: RequestId,
    request: MediatorRequest,
    version: CantonTimestamp,
    state: Either[Verdict, Map[ViewPosition, ViewState]],
)(protocolVersion: ProtocolVersion, val requestTraceContext: TraceContext)(
    protected val loggerFactory: NamedLoggerFactory
) extends ResponseAggregation
    with NamedLogging {

  override type VKEY = ViewPosition

  /** Record the additional confirmation response received. */
  override def progress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregationV5]] = {
    val MediatorResponse(
      _requestId,
      sender,
      _viewHashO,
      viewPositionO,
      localVerdict,
      rootHashO,
      confirmingParties,
      _domainId,
    ) =
      response

    def authorizedPartiesOfSender(
        viewPosition: ViewPosition,
        declaredConfirmingParties: Set[ConfirmingParty],
    ): OptionT[Future, Set[LfPartyId]] =
      localVerdict match {
        case malformed: Malformed =>
          malformed.logWithContext(
            Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender")
          )
          val hostedConfirmingPartiesF =
            declaredConfirmingParties.toList
              .parFilterA(p => topologySnapshot.canConfirm(sender, p.party, p.requiredTrustLevel))
              .map(_.toSet)
          val res = hostedConfirmingPartiesF.map { hostedConfirmingParties =>
            logger.debug(
              show"Malformed response $responseTimestamp for $viewPosition considered as a rejection on behalf of $hostedConfirmingParties"
            )
            Some(hostedConfirmingParties.map(_.party)): Option[Set[LfPartyId]]
          }
          OptionT(res)

        case _: LocalApprove | _: LocalReject =>
          val unexpectedConfirmingParties =
            confirmingParties -- declaredConfirmingParties.map(_.party)
          for {
            _ <-
              if (unexpectedConfirmingParties.isEmpty) OptionT.some[Future](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received a mediator response at $responseTimestamp by $sender for request $requestId with unexpected confirming parties $unexpectedConfirmingParties. Discarding response...",
                    protocolVersion,
                  )
                  .report()
                OptionT.none[Future, Unit]
              }

            expectedConfirmingParties =
              declaredConfirmingParties.filter(p => confirmingParties.contains(p.party))
            unauthorizedConfirmingParties <- OptionT.liftF(
              expectedConfirmingParties.toList
                .parFilterA { p =>
                  topologySnapshot.canConfirm(sender, p.party, p.requiredTrustLevel).map(x => !x)
                }
                .map(_.map(_.party))
                .map(_.toSet)
            )
            _ <-
              if (unauthorizedConfirmingParties.isEmpty) OptionT.some[Future](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received an unauthorized mediator response at $responseTimestamp by $sender for request $requestId on behalf of $unauthorizedConfirmingParties. Discarding response...",
                    protocolVersion,
                  )
                  .report()
                OptionT.none[Future, Unit]
              }
          } yield confirmingParties
      }

    def progressView(
        statesOfViews: Map[ViewPosition, ViewState],
        viewPositionAndParties: (ViewPosition, Set[LfPartyId]),
    ): Either[Verdict, Map[ViewPosition, ViewState]] = {
      val (viewPosition, authorizedParties) = viewPositionAndParties
      val stateOfView = statesOfViews.getOrElse(
        viewPosition,
        throw new IllegalArgumentException(
          s"The transaction view position $viewPositionO is not covered by the request"
        ),
      )
      val ViewState(pendingConfirmingParties, consortiumVoting, distanceToThreshold, rejections) =
        stateOfView
      val (newlyResponded, _) =
        pendingConfirmingParties.partition(cp => authorizedParties.contains(cp.party))

      logger.debug(
        show"$requestId(view position $viewPosition): Received verdict $localVerdict for pending parties $newlyResponded by participant $sender. "
      )
      val alreadyResponded = authorizedParties -- newlyResponded.map(_.party)
      // Because some of the responders might have had some other participant already confirmed on their behalf
      // we ignore those responders
      if (alreadyResponded.nonEmpty) logger.debug(s"Ignored responses from $alreadyResponded")

      if (newlyResponded.isEmpty) {
        logger.debug(
          s"Nothing to do upon response from $sender for $requestId(view position $viewPosition) because no new responders"
        )
        Either.right[Verdict, Map[ViewPosition, ViewState]](statesOfViews)
      } else {
        localVerdict match {
          case LocalApprove() =>
            val consortiumVotingUpdated =
              newlyResponded.foldLeft(consortiumVoting)((votes, confirmingParty) => {
                votes + (confirmingParty.party -> votes(confirmingParty.party).approveBy(sender))
              })
            val newlyRespondedFullVotes = newlyResponded.filter {
              case ConfirmingParty(party, _, _) =>
                consortiumVotingUpdated(party).isApproved
            }
            logger.debug(
              show"$requestId(view position $viewPosition): Received an approval (or reached consortium thresholds) for parties: $newlyRespondedFullVotes"
            )
            val contribution = newlyRespondedFullVotes.foldLeft(0)(_ + _.weight)
            val stillPending = pendingConfirmingParties -- newlyRespondedFullVotes
            if (newlyRespondedFullVotes.isEmpty) {
              logger.debug(
                show"$requestId(view position $viewPosition): Awaiting approvals or additional votes for consortiums for $stillPending"
              )
            }
            val nextViewState =
              ViewState(
                stillPending,
                consortiumVotingUpdated,
                distanceToThreshold - contribution,
                rejections,
              )
            val nextStatesOfViews = statesOfViews + (viewPosition -> nextViewState)
            Either.cond(
              nextStatesOfViews.values.exists(_.distanceToThreshold > 0),
              nextStatesOfViews,
              Approve(protocolVersion),
            )

          case rejection: LocalReject =>
            val consortiumVotingUpdated =
              authorizedParties.foldLeft(consortiumVoting)((votes, party) => {
                votes + (party -> votes(party).rejectBy(sender))
              })
            val newRejectionsFullVotes = authorizedParties.filter(party => {
              consortiumVotingUpdated(party).isRejected
            })
            if (newRejectionsFullVotes.nonEmpty) {
              logger.debug(
                show"$requestId(view position $viewPosition): Received a rejection (or reached consortium thresholds) for parties: $newRejectionsFullVotes"
              )
              val nextRejections =
                NonEmpty(List, (newRejectionsFullVotes -> rejection), rejections *)
              val stillPending =
                pendingConfirmingParties.filterNot(cp => newRejectionsFullVotes.contains(cp.party))
              val nextViewState = ViewState(
                stillPending,
                consortiumVotingUpdated,
                distanceToThreshold,
                nextRejections,
              )
              Either.cond(
                nextViewState.distanceToThreshold <= nextViewState.totalAvailableWeight,
                statesOfViews + (viewPosition -> nextViewState),
                // TODO(#5337): Don't discard the rejection reasons of the other views.
                ParticipantReject(nextRejections, protocolVersion),
              )
            } else {
              // no full votes, need more confirmations (only in consortium case)
              logger.debug(
                show"$requestId(view position $viewPosition): Received a rejection, but awaiting more consortium votes for: $pendingConfirmingParties"
              )
              val nextViewState = ViewState(
                pendingConfirmingParties,
                consortiumVotingUpdated,
                distanceToThreshold,
                rejections,
              )
              Right(statesOfViews + (viewPosition -> nextViewState))
            }
        }
      }
    }

    (for {
      _ <- OptionT.fromOption[Future](rootHashO.traverse_ { rootHash =>
        if (request.rootHash.forall(_ == rootHash)) Some(())
        else {
          val cause =
            show"Received a mediator response at $responseTimestamp by $sender for request $requestId with an invalid root hash $rootHash instead of ${request.rootHash.showValueOrNone}. Discarding response..."
          val alarm = MediatorError.MalformedMessage.Reject(cause, protocolVersion)
          alarm.report()

          None
        }
      })

      viewPositionsAndParties <- {
        viewPositionO match {
          case None =>
            // If no view position is given, the local verdict is Malformed and confirming parties is empty by the invariants of MediatorResponse.
            // We treat this as a rejection for all parties hosted by the participant.
            localVerdict match {
              case malformed: Malformed =>
                malformed.logWithContext(
                  Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender")
                )
              case other =>
                ErrorUtil.requireState(
                  condition = false,
                  s"Verdict should be of type malformed, but got $other",
                )
            }

            val informeesByView = request.informeesAndThresholdByViewPosition
            val ret = informeesByView.toList
              .parTraverseFilter { case (viewPosition, (informees, _threshold)) =>
                val hostedConfirmingPartiesF = informees.toList.parTraverseFilter {
                  case ConfirmingParty(party, _, requiredTrustLevel) =>
                    topologySnapshot
                      .canConfirm(sender, party, requiredTrustLevel)
                      .map(x => if (x) Some(party) else None)
                  case _ => Future.successful(None)
                }
                hostedConfirmingPartiesF.map { hostedConfirmingParties =>
                  if (hostedConfirmingParties.nonEmpty)
                    Some(viewPosition -> hostedConfirmingParties.toSet)
                  else None
                }
              }
              .map { viewsWithConfirmingPartiesForSender =>
                logger.debug(
                  s"Malformed response $responseTimestamp from $sender considered as a rejection for ${viewsWithConfirmingPartiesForSender}"
                )
                viewsWithConfirmingPartiesForSender
              }
            OptionT.liftF(ret)
          case Some(viewPosition) =>
            for {
              informeesAndThreshold <- OptionT.fromOption[Future](
                request.informeesAndThresholdByViewPosition
                  .get(viewPosition)
                  .orElse {
                    val cause =
                      s"Received a mediator response at $responseTimestamp by $sender for request $requestId with an unknown view position $viewPosition. Discarding response..."
                    val alarm = MediatorError.MalformedMessage.Reject(cause, protocolVersion)
                    alarm.report()

                    None
                  }
              )
              (informees, _) = informeesAndThreshold
              declaredConfirmingParties = informees.collect { case p: ConfirmingParty => p }
              authorizedConfirmingParties <- authorizedPartiesOfSender(
                viewPosition,
                declaredConfirmingParties,
              )
            } yield List(viewPosition -> authorizedConfirmingParties)
        }
      }

      // This comes last so that the validation also runs for responses to finalized requests. Benefits:
      // - more exhaustive security alerts
      // - avoid race conditions in security tests
      statesOfViews <- OptionT.fromOption[Future](state.leftMap { s => // move down
        logger.debug(
          s"Request ${requestId.unwrap} has already been finalized with verdict $s before response $responseTimestamp from $sender with $localVerdict for view $viewPositionO arrives"
        )
      }.toOption)
    } yield {
      val updatedState = MonadUtil.foldLeftM(statesOfViews, viewPositionsAndParties)(progressView)
      copy(version = responseTimestamp, state = updatedState)
    }).value
  }

  def copy(
      requestId: RequestId = requestId,
      request: MediatorRequest = request,
      version: CantonTimestamp = version,
      state: Either[Verdict, Map[ViewPosition, ViewState]] = state,
  ): ResponseAggregationV5 = ResponseAggregationV5(requestId, request, version, state)(
    protocolVersion,
    requestTraceContext,
  )(loggerFactory)

  override def withVersion(version: CantonTimestamp): ResponseAggregationV5 =
    copy(version = version)

  override def withRequestId(requestId: RequestId): ResponseAggregationV5 =
    copy(requestId = requestId)

  def timeout(version: CantonTimestamp): ResponseAggregationV5 =
    ResponseAggregationV5(
      this.requestId,
      this.request,
      version,
      Left(MediatorError.Timeout.Reject.create(protocolVersion)),
    )(this.protocolVersion, requestTraceContext)(loggerFactory)

  override def pretty: Pretty[ResponseAggregationV5] = prettyOfClass(
    param("id", _.requestId),
    param("request", _.request),
    param("version", _.version),
    param("state", _.state),
  )
}
