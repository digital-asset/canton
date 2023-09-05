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
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty}
import com.digitalasset.canton.domain.mediator.ResponseAggregation.{
  ConsortiumVotingState,
  ViewState,
}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.Verdict.{Approve, ParticipantReject}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, ViewHash}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[mediator] object ResponseAggregationV0 {

  /** Creates a non-finalized response aggregation from a request.
    */
  def fromRequest(
      requestId: RequestId,
      request: MediatorRequest,
      protocolVersion: ProtocolVersion,
      topologySnapshot: TopologySnapshot,
      sendVerdict: Boolean,
  )(loggerFactory: NamedLoggerFactory)(implicit
      requestTraceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[ResponseAggregationV0] = {
    val initialStateF = request.informeesAndThresholdByViewHash.toSeq
      .parTraverse { case (viewHash, (informees, threshold)) =>
        val confirmingParties = informees.collect { case cp: ConfirmingParty => cp }
        for {
          votingThresholds <- topologySnapshot.consortiumThresholds(confirmingParties.map(_.party))
        } yield {
          val consortiumVotingState = votingThresholds.map { case (party, threshold) =>
            (party -> ConsortiumVotingState(threshold))
          }
          viewHash -> ViewState(
            confirmingParties,
            consortiumVotingState,
            threshold.unwrap,
            rejections = Nil,
          )
        }
      }
      .map(_.toMap)

    for {
      initialState <- initialStateF
    } yield {
      ResponseAggregationV0(requestId, request, requestId.unwrap, Right(initialState))(
        protocolVersion = protocolVersion,
        requestTraceContext = requestTraceContext,
      )(loggerFactory)
    }
  }

  /** Creates a finalized response aggregation from a verdict.
    */
  def fromVerdict(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      protocolVersion: ProtocolVersion,
  )(
      loggerFactory: NamedLoggerFactory
  )(implicit requestTraceContext: TraceContext): ResponseAggregationV0 =
    ResponseAggregationV0(requestId, request, requestId.unwrap, Left(verdict))(
      protocolVersion,
      requestTraceContext,
    )(loggerFactory)
}

private[mediator] final case class ResponseAggregationV0(
    requestId: RequestId,
    request: MediatorRequest,
    version: CantonTimestamp,
    state: Either[Verdict, Map[ViewHash, ViewState]],
)(protocolVersion: ProtocolVersion, val requestTraceContext: TraceContext)(
    protected val loggerFactory: NamedLoggerFactory
) extends ResponseAggregation
    with NamedLogging {

  override type VKEY = ViewHash

  /** Record the additional confirmation response received. */
  def progress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregationV0]] = {
    val MediatorResponse(
      _requestId,
      sender,
      viewHashO,
      _viewPositionO,
      localVerdict,
      rootHashO,
      confirmingParties,
      _domainId,
    ) =
      response

    def authorizedPartiesOfSender(
        viewHash: ViewHash,
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
              show"Malformed response $responseTimestamp for $viewHash considered as a rejection on behalf of $hostedConfirmingParties"
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
      val ViewState(pendingConfirmingParties, consortiumVoting, distanceToThreshold, rejections) =
        stateOfView
      val (newlyResponded, _) =
        pendingConfirmingParties.partition(cp => authorizedParties.contains(cp.party))

      logger.debug(
        show"$requestId(view hash $viewHash): Received verdict $localVerdict for pending parties $newlyResponded by participant $sender. "
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
              show"$requestId(view hash $viewHash): Received an approval (or reached consortium thresholds) for parties: $newlyRespondedFullVotes"
            )
            val contribution = newlyRespondedFullVotes.foldLeft(0)(_ + _.weight.unwrap)
            val stillPending = pendingConfirmingParties -- newlyRespondedFullVotes
            if (newlyRespondedFullVotes.isEmpty) {
              logger.debug(
                show"$requestId(view hash $viewHash): Awaiting approvals or additional votes for consortiums for $stillPending"
              )
            }
            val nextViewState =
              ViewState(
                stillPending,
                consortiumVotingUpdated,
                distanceToThreshold - contribution,
                rejections,
              )
            val nextStatesOfViews = statesOfViews + (viewHash -> nextViewState)
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
                show"$requestId(view hash $viewHash): Received a rejection (or reached consortium thresholds) for parties: $newRejectionsFullVotes"
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
                statesOfViews + (viewHash -> nextViewState),
                // TODO(#5337): Don't discard the rejection reasons of the other views.
                ParticipantReject(nextRejections, protocolVersion),
              )
            } else {
              // no full votes, need more confirmations (only in consortium case)
              logger.debug(
                show"$requestId(view hash $viewHash): Received a rejection, but awaiting more consortium votes for: $pendingConfirmingParties"
              )
              val nextViewState = ViewState(
                pendingConfirmingParties,
                consortiumVotingUpdated,
                distanceToThreshold,
                rejections,
              )
              Right(statesOfViews + (viewHash -> nextViewState))
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

      viewHashesAndParties <- {
        viewHashO match {
          case None =>
            // If no view hash is given, the local verdict is Malformed and confirming parties is empty by the invariants of MediatorResponse.
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

            val informeesByView = request.informeesAndThresholdByViewHash
            val ret = informeesByView.toList
              .parTraverseFilter { case (viewHash, (informees, _threshold)) =>
                val hostedConfirmingPartiesF = informees.toList.parTraverseFilter {
                  case ConfirmingParty(party, _, requiredTrustLevel) =>
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
            OptionT.liftF(ret)
          case Some(viewHash) =>
            for {
              informeesAndThreshold <- OptionT.fromOption[Future](
                request.informeesAndThresholdByViewHash
                  .get(viewHash)
                  .orElse {
                    val cause =
                      s"Received a mediator response at $responseTimestamp by $sender for request $requestId with an unknown view hash $viewHash. Discarding response..."
                    val alarm = MediatorError.MalformedMessage.Reject(cause, protocolVersion)
                    alarm.report()

                    None
                  }
              )
              (informees, _) = informeesAndThreshold
              declaredConfirmingParties = informees.collect { case p: ConfirmingParty => p }
              authorizedConfirmingParties <- authorizedPartiesOfSender(
                viewHash,
                declaredConfirmingParties,
              )
            } yield List(viewHash -> authorizedConfirmingParties)
        }
      }

      // This comes last so that the validation also runs for responses to finalized requests. Benefits:
      // - more exhaustive security alerts
      // - avoid race conditions in security tests
      statesOfViews <- OptionT.fromOption[Future](state.leftMap { s => // move down
        logger.debug(
          s"Request ${requestId.unwrap} has already been finalized with verdict $s before response $responseTimestamp from $sender with $localVerdict for view $viewHashO arrives"
        )
      }.toOption)
    } yield {
      val updatedState = MonadUtil.foldLeftM(statesOfViews, viewHashesAndParties)(progressView)
      copy(version = responseTimestamp, state = updatedState)
    }).value
  }

  def copy(
      requestId: RequestId = requestId,
      request: MediatorRequest = request,
      version: CantonTimestamp = version,
      state: Either[Verdict, Map[ViewHash, ViewState]] = state,
  ): ResponseAggregationV0 = ResponseAggregationV0(requestId, request, version, state)(
    protocolVersion,
    requestTraceContext,
  )(loggerFactory)

  override def withVersion(version: CantonTimestamp): ResponseAggregationV0 =
    copy(version = version)

  override def withRequestId(requestId: RequestId): ResponseAggregationV0 =
    copy(requestId = requestId)

  def timeout(version: CantonTimestamp) =
    ResponseAggregationV0(
      this.requestId,
      this.request,
      version,
      Left(MediatorError.Timeout.Reject.create(protocolVersion)),
    )(this.protocolVersion, requestTraceContext)(loggerFactory)

  override def pretty: Pretty[ResponseAggregationV0] = prettyOfClass(
    param("id", _.requestId),
    param("request", _.request),
    param("version", _.version),
    param("state", _.state),
  )
}
