// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType.TopologyTransaction
import com.digitalasset.canton.sequencing.protocol.{
  MemberRecipient,
  MemberRecipientOrBroadcast,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}

import scala.concurrent.ExecutionContext

import BlockUpdateGeneratorImpl.{PrevalidationOutcome, SequencedPreValidatedSubmissionResult, State}
import SequencedSubmissionsValidator.SequencedSubmissionsValidationResult
import SubmissionRequestValidator.{SubmissionRequestValidationResult, TrafficConsumption}

/** Sequential part of the block chunk processing
  *
  * Consumes the results of the parallel validation performed by [[SubmissionRequestValidator]].
  *
  * Effectively, further validates and processes a list of
  * [[SequencedPreValidatedSubmissionResult]]s corresponding to a chunk and performs the sequential
  * parts such as aggregation and traffic accounting.
  *
  * NOTE: RENAME ME INTO SEQUENTIAL SUBMISSION VALIDATOR AND PROCESSOR
  */
private[update] final class SequencedSubmissionsValidator(
    sequencerId: SequencerId,
    inFlightAggregationHandler: InFlightAggregationHandler,
    trafficControlValidator: TrafficControlValidator,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def sequentialApplySubmissionsAndEmitOutcomes(
      state: State,
      height: Long,
      sequencedValidatedSubmissions: Seq[SequencedPreValidatedSubmissionResult],
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] =
    MonadUtil.foldLeftM(
      initialState = SequencedSubmissionsValidationResult(
        inFlightAggregations = state.inFlightAggregations,
        latestPendingTopologyTransactionTimestamp = state.latestPendingTopologyTransactionTimestamp,
      ),
      sequencedValidatedSubmissions,
    )(applySubmissionAndAddOutcome(state.latestSequencerEventTimestamp, height))

  /** @param latestSequencerEventTimestamp
    *   Since each chunk contains at most one event addressed to the sequencer, (and if so it's the
    *   last event), we can treat this timestamp static for the whole chunk and need not update it
    *   in the accumulator.
    */
  private def applySubmissionAndAddOutcome(
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      height: Long,
  )(
      partialResult: SequencedSubmissionsValidationResult,
      sequencedValidatedSubmissionRequest: SequencedPreValidatedSubmissionResult,
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] = {
    // the result of the previous sequential validation iteration on this block
    val SequencedSubmissionsValidationResult(
      inFlightAggregations,
      latestPendingTopologyTransactionTimestamp,
      inFlightAggregationUpdates,
      sequencerEventTimestampSoFar,
      reversedOutcomes,
    ) = partialResult
    // the previous result of the independent and parallel pre-validation of this particular submission
    val SequencedPreValidatedSubmissionResult(
      sequencingTimestamp,
      signedSubmissionRequest,
      orderingSequencerId,
      trafficConsumption,
      errorOrPrevalidationOutcome,
      sequencingTopologySnapshot,
    ) = sequencedValidatedSubmissionRequest

    implicit val traceContext: TraceContext = sequencedValidatedSubmissionRequest.traceContext

    ErrorUtil.requireState(
      sequencerEventTimestampSoFar.isEmpty,
      "Only the last event in a chunk could be addressed to the sequencer",
    )

    for {
      newStateAndOutcome <-
        applyAggregationAndTrafficControlAndGenerateOutcomes(
          inFlightAggregations,
          sequencingTimestamp,
          signedSubmissionRequest,
          orderingSequencerId,
          trafficConsumption,
          errorOrPrevalidationOutcome,
          latestSequencerEventTimestamp,
          sequencingTopologySnapshot,
        )
      SubmissionRequestValidationResult(inFlightAggregations, outcome, sequencerEventTimestamp) =
        newStateAndOutcome
    } yield {
      logger
        .debug(
          s"At block $height, the submission request ${signedSubmissionRequest.content.messageId} " +
            s"at $sequencingTimestamp validated to: ${SubmissionOutcome.prettyString(outcome)}"
        )
      // Only consider delivered requests when updating latest pending topology transaction ts
      val updatedLatestPendingTopologyTransactionTimestamp =
        (outcome, signedSubmissionRequest.content.requestType) match {
          case (_: SubmissionOutcome.Deliver, TopologyTransaction) =>
            Some(sequencingTimestamp)
          case _ => latestPendingTopologyTransactionTimestamp
        }
      updateSequencedSubmissionsWithNewResult(
        inFlightAggregations = inFlightAggregations,
        outcome = outcome,
        // if we drop the event, we will just return the previous outcome,
        // but we still want to update the latest pending topology transaction timestamp
        resultIfNoDeliverEvents = partialResult.copy(latestPendingTopologyTransactionTimestamp =
          updatedLatestPendingTopologyTransactionTimestamp
        ),
        latestPendingTopologyTransactionTimestamp =
          updatedLatestPendingTopologyTransactionTimestamp,
        previouslyAccumulatedInFlightAggregationUpdates = inFlightAggregationUpdates,
        sequencerEventTimestamp = sequencerEventTimestamp,
        remainingReversedOutcomes = reversedOutcomes,
      )
    }
  }

  private def applyAggregationAndTrafficControlAndGenerateOutcomes(
      inFlightAggregations: InFlightAggregations,
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedSubmissionRequest,
      orderingSequencerId: SequencerId,
      trafficConsumption: TrafficConsumption,
      errorOrPrevalidationOutcome: Either[SubmissionOutcome, PrevalidationOutcome],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      sequencingTopologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[SubmissionRequestValidationResult] = {
    val processingResult =
      errorOrPrevalidationOutcome match {
        case Right(prevalidationOutcome) =>
          applyToAggregationState(
            prevalidationOutcome,
            inFlightAggregations,
            sequencingTimestamp,
            signedSubmissionRequest.content,
            sequencingTopologySnapshot,
          ).leftMap { errorSubmissionOutcome =>
            SubmissionRequestValidationResult(inFlightAggregations, errorSubmissionOutcome, None)
          }.merge
        case Left(errorSubmissionOutcome) =>
          FutureUnlessShutdown.pure(
            SubmissionRequestValidationResult(
              inFlightAggregations,
              errorSubmissionOutcome,
              None,
            )
          )
      }

    trafficControlValidator.applyTrafficControl(
      trafficConsumption,
      processingResult,
      signedSubmissionRequest,
      orderingSequencerId,
      sequencingTimestamp,
      latestSequencerEventTimestamp,
      sequencingTopologySnapshot,
    )
  }

  // Performs additional checks and runs the aggregation logic
  // If this succeeds, it will produce a SubmissionOutcome containing
  // the complete validation result for a single submission
  private def applyToAggregationState(
      prevalidationOutcome: PrevalidationOutcome,
      inFlightAggregations: InFlightAggregations,
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      sequencingTopologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    SubmissionOutcome,
    SubmissionRequestValidationResult,
  ] = inFlightAggregationHandler
    .computeAggregationUpdateAndSubmissionOutcome(
      submissionRequest,
      sequencingTimestamp,
      sequencingTopologySnapshot,
      aggregationInfo = prevalidationOutcome.aggregationInfo,
      inFlightAggregations = inFlightAggregations,
    )
    .map { case (requestOrAggregatedBatch, aggregationUpdateO) =>
      val submissionRecipients =
        (submissionRequest.batch.allMembers + submissionRequest.sender).map(MemberRecipient.apply)

      val allRecipients = prevalidationOutcome.recipients ++ submissionRecipients

      // We need to know whether the group of sequencers was addressed in order to update `latestSequencerEventTimestamp`.
      // Simply checking whether this sequencer is within the resulting event recipients opens up
      // the door for a malicious participant to target a single sequencer, which would result in the
      // various sequencers reaching a different value.
      //
      // Currently, the only use cases of addressing a sequencer are:
      //   * via AllMembersOfSynchronizer for topology transactions
      //   * via SequencersOfSynchronizer for traffic control top-up messages
      //
      // Therefore, we check whether this sequencer was addressed via a group address to avoid the above
      // case.
      //
      // NOTE: Pruning concerns
      // ----------------------
      // `latestSequencerEventTimestamp` is relevant for pruning.
      // For the traffic top-ups, we can use the block's last timestamp to signal "safe-to-prune", because
      // the logic to compute the balance based on `latestSequencerEventTimestamp` sits inside the manager
      // and we can make it work together with pruning.
      // For topology, pruning is not yet implemented. However, the logic to compute snapshot timestamps
      // sits outside of the topology processor and so from the topology processor's point of view,
      // `latestSequencerEventTimestamp` should be part of a "safe-to-prune" timestamp calculation.
      //
      // See https://github.com/DACH-NY/canton/pull/17676#discussion_r1515926774
      val sequencerEventTimestamp =
        Option.when(isThisSequencerAddressed(prevalidationOutcome.recipients, submissionRequest))(
          sequencingTimestamp
        )

      SubmissionRequestValidationResult(
        inFlightAggregations,
        SubmissionOutcome.Deliver(
          submissionRequest,
          sequencingTimestamp,
          allRecipients,
          requestOrAggregatedBatch,
          traceContext,
          trafficReceiptO = None, // traffic receipt is updated at the end of the processing
          inFlightAggregation = aggregationUpdateO,
        ),
        sequencerEventTimestamp,
      )
    }

  // Off-boarded sequencers may still receive blocks (e.g., BFT sequencers still contribute to ordering for a while
  //  after being deactivated in the Canton topology, specifically until the underlying consensus algorithm
  //  allows them to be also removed from the BFT ordering topology), but they should not be considered addressed,
  //  since they are not active in the Canton topology anymore (i.e., group recipients don't include them).
  private def isThisSequencerAddressed(
      recipients: Set[MemberRecipientOrBroadcast],
      submissionRequest: SubmissionRequest,
  ): Boolean =
    recipients.contains(MemberRecipient(sequencerId)) || submissionRequest.batch.isBroadcast

  private def updateSequencedSubmissionsWithNewResult(
      inFlightAggregations: InFlightAggregations,
      outcome: SubmissionOutcome,
      resultIfNoDeliverEvents: SequencedSubmissionsValidationResult,
      latestPendingTopologyTransactionTimestamp: Option[CantonTimestamp],
      previouslyAccumulatedInFlightAggregationUpdates: InFlightAggregationUpdates,
      sequencerEventTimestamp: Option[CantonTimestamp],
      remainingReversedOutcomes: Seq[SubmissionOutcome],
  )(implicit
      traceContext: TraceContext
  ): SequencedSubmissionsValidationResult =
    outcome match {
      case deliverable: DeliverableSubmissionOutcome =>
        // we potentially computed an update to the inflight aggregation related to this
        // submission. we now need to apply it to the ephemeral state which we'll then
        // use for the next outcome
        val (newInFlightAggregations, newInFlightAggregationUpdates) =
          deliverable.inFlightAggregation.fold(
            inFlightAggregations -> previouslyAccumulatedInFlightAggregationUpdates
          ) { case (aggregationId, inFlightAggregation, inFlightAggregationUpdate) =>
            val updatedInFlightAggregationState =
              inFlightAggregations.updated(aggregationId, inFlightAggregation)
            val updatedAccumulatedInflightAggregationUpdates = MapsUtil.extendedMapWith(
              previouslyAccumulatedInFlightAggregationUpdates,
              Iterable(aggregationId -> inFlightAggregationUpdate),
            )(_ tryMerge _)
            updatedInFlightAggregationState -> updatedAccumulatedInflightAggregationUpdates
          }

        SequencedSubmissionsValidationResult(
          newInFlightAggregations,
          latestPendingTopologyTransactionTimestamp,
          newInFlightAggregationUpdates,
          sequencerEventTimestamp,
          outcome +: remainingReversedOutcomes,
        )

      case _ => // Discarded submission
        resultIfNoDeliverEvents
    }
}

private[update] object SequencedSubmissionsValidator {

  /** Accumulates the submissions within a chunk
    *
    * We perform a foldLeft on the sequenced submissions and accumulate the state changes in here.
    *
    * @param inFlightAggregations
    *   the current state of inflight aggregations
    * @param latestPendingTopologyTransactionTimestamp
    *   the timestamp of the last topology event being sent to the sequencer
    * @param inFlightAggregationUpdates
    *   the state changes in this chunk, stored separately as we need to persist them
    * @param lastSequencerEventTimestamp
    *   the timestamp of the last sequencer event processed while validating the submissions
    * @param reversedOutcomes
    *   the processed submission requests
    */
  final case class SequencedSubmissionsValidationResult(
      inFlightAggregations: InFlightAggregations,
      latestPendingTopologyTransactionTimestamp: Option[CantonTimestamp],
      inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
      lastSequencerEventTimestamp: Option[CantonTimestamp] = None,
      reversedOutcomes: Seq[SubmissionOutcome] = Seq.empty,
  )
}
