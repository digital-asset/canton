// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.data.{Chain, EitherT}
import cats.implicits.catsStdInstancesForFuture
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationId,
  AggregationRule,
  Batch,
  ClosedUncompressedEnvelope,
  SequencerErrors,
  SubmissionRequest,
  SubmissionRequestValidations,
  *,
}
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.{
  AggregatedSender,
  FreshInFlightAggregation,
  InFlightAggregation,
  InFlightAggregationUpdate,
  InFlightAggregations,
  SubmissionOutcome,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.ExecutionContext

/** Helper class containing all the methods around aggregations */
class InFlightAggregationHandler(
    pureCrypto: CryptoPureApi,
    memberValidator: SequencerMemberValidator,
    val loggerFactory: NamedLoggerFactory,
    protocolVersion: ProtocolVersion,
) extends NamedLogging {

  /** Parallel aggregation prevalidation */
  def computeAggregationIdAndValidateAggregationRule(
      sequencingTimestamp: CantonTimestamp,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      submissionRequest: SubmissionRequest,
      skipMemberCheck: AggregationId => Boolean,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Option[(AggregationId, AggregationRule)]] = {

    val aggregationIdETO = EitherT.fromEither[FutureUnlessShutdown](
      submissionRequest
        .aggregationId(pureCrypto)
        .leftMap { err =>
          logger.error(
            s"Internal error occurred when processing request $sequencingTimestamp: computation of aggregation id: ${err.message}"
          )
          SubmissionOutcome.Discard
        }
    )

    for {
      _ <-
        // in pv34, we perform this test before checking envelope signatures and computing recipients
        // we cannot change the order in pv34 as otherwise, we would generate different outcomes for a
        // malformed transaction that fails both checks.
        // starting with pv35, we perform this check here (and can subsequently change
        // the method to be private and only be called here when we drop the pv34 code path)
        if (protocolVersion > ProtocolVersion.v34)
          validateMaxSequencingTimeForAggregationRule(
            submissionRequest,
            topologyOrSequencingSnapshot,
            sequencingTimestamp,
          )
        else EitherTUtil.unitUS
      aggregationIdAndRuleO <- aggregationIdETO
      _ <- aggregationIdAndRuleO.traverse { case (_, rule) =>
        wellFormedAggregationRule(submissionRequest, rule)
      }
      _ <- aggregationIdAndRuleO
        .map { case (aggregationId, aggregationRule) =>
          // check whether we can optimistically skip the member check (no harm if we do it twice in parallel in case we race)
          (aggregationRule, skipMemberCheck(aggregationId))
        }
        .traverse {
          case (rule, false) =>
            validateAllMembersInAggregationAreKnown(submissionRequest, sequencingTimestamp, rule)
          case (_, true) =>
            EitherTUtil.unitUS[SubmissionOutcome]
        }
    } yield aggregationIdAndRuleO

  }

  def validateMaxSequencingTimeForAggregationRule(
      submissionRequest: SubmissionRequest,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Unit] =
    submissionRequest.aggregationRule.traverse_ { _ =>
      for {
        synchronizerParameters <- EitherT(
          topologyOrSequencingSnapshot.ipsSnapshot.findDynamicSynchronizerParameters()
        )
          .leftMap(error =>
            SubmissionOutcome.Reject.logAndCreate(
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"Could not fetch dynamic synchronizer parameters: $error"
              ),
            )
          )
        maxSequencingTimeUpperBound = sequencingTimestamp.toInstant.plus(
          synchronizerParameters.parameters.sequencerAggregateSubmissionTimeout.duration
        )
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          submissionRequest.maxSequencingTime.toInstant.isBefore(maxSequencingTimeUpperBound),
          SubmissionOutcome.Reject.logAndCreate(
            submissionRequest,
            sequencingTimestamp,
            SequencerErrors.MaxSequencingTimeTooFar(
              submissionRequest.messageId,
              submissionRequest.maxSequencingTime,
              maxSequencingTimeUpperBound,
            ),
          ): SubmissionOutcome,
        )
      } yield ()
    }

  private def validateAllMembersInAggregationAreKnown(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Unit] =
    for {
      unregisteredEligibleMembers <-
        EitherT.right(
          memberValidator
            .areMembersRegisteredAt(rule.eligibleSenders.forgetNE, sequencingTimestamp)
            .map(_.flatMap {
              case (member, true) => None
              case (member, false) => Some(member)
            })
        )

      _ <- EitherTUtil
        .condUnitET(
          unregisteredEligibleMembers.isEmpty,
          // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
          SubmissionOutcome.Reject.logAndCreate(
            submissionRequest,
            sequencingTimestamp,
            SequencerErrors.SubmissionRequestRefused(
              s"Aggregation rule contains unregistered eligible members: $unregisteredEligibleMembers"
            ),
          ): SubmissionOutcome,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  private def wellFormedAggregationRule(
      submissionRequest: SubmissionRequest,
      rule: AggregationRule,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Unit] =
    EitherT.fromEither(
      SubmissionRequestValidations
        .wellformedAggregationRule(submissionRequest.sender, rule)
        .leftMap { message =>
          val alarm = SequencerErrors.SubmissionRequestMalformed.Error(submissionRequest, message)
          alarm.report()

          SubmissionOutcome.Discard
        }
    )

  /** Sequential part of aggregation producing what we are ultimately going to deliver (unless
    * traffic rejects it)
    */
  def computeAggregationUpdateAndSubmissionOutcome(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologySnapshot: TopologySnapshot,
      aggregationInfo: Option[(AggregationId, AggregationRule)],
      inFlightAggregations: InFlightAggregations,
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): EitherT[
    FutureUnlessShutdown,
    SubmissionOutcome,
    (
        // the output batch
        Batch[ClosedEnvelope],
        // the pending aggregation update (new aggregation and the update are kept separate as we need to store the updates subsequently)
        Option[
          (AggregationId, InFlightAggregation, InFlightAggregationUpdate)
        ],
    ),
  ] =
    aggregationInfo
      .traverse { case (aggregationId, aggregationRule) =>
        val inFlightAggregation = inFlightAggregations.get(aggregationId)
        EitherT
          .fromEither[FutureUnlessShutdown](
            submissionRequest.batch.toClosedUncompressedBatchResult
              .leftMap[SubmissionOutcome](_ => SubmissionOutcome.Discard)
          )
          .flatMap { uncompressedBatch =>
            updateInFlightAggregation(
              submissionRequest,
              uncompressedBatch,
              sequencingTimestamp,
              aggregationId,
              aggregationRule,
              inFlightAggregation,
              topologySnapshot,
            ).map { case (updatedInFlightAggregation, inFlightAggregationUpdate) =>
              (
                aggregationId,
                updatedInFlightAggregation,
                inFlightAggregationUpdate,
                uncompressedBatch,
              )
            }
          }
      }
      .map {
        case None => (submissionRequest.batch, None)
        case Some(
              (
                aggregationId,
                updatedInFlightAggregation,
                inFlightAggregationUpdate,
                uncompressedBatch,
              )
            ) =>
          val aggregatedBatch = uncompressedBatch
            .focus(_.envelopes)
            .modify(_.lazyZip(updatedInFlightAggregation.aggregatedSignatures).map {
              (envelope, signatures) =>
                envelope.updateSignatures(signatures = signatures)
            })
          (
            aggregatedBatch,
            Some((aggregationId, updatedInFlightAggregation, inFlightAggregationUpdate)),
          )
      }

  /** Computes the updated aggregation based on the assumption that the aggregation rule and id are
    * valid
    */
  private def updateInFlightAggregation(
      submissionRequest: SubmissionRequest,
      uncompressedBatch: Batch[ClosedUncompressedEnvelope],
      sequencingTimestamp: CantonTimestamp,
      aggregationId: AggregationId,
      rule: AggregationRule,
      inFlightAggregationO: Option[InFlightAggregation],
      topologySnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    SubmissionOutcome,
    (InFlightAggregation, InFlightAggregationUpdate),
  ] = {
    ErrorUtil.requireState(
      submissionRequest.aggregationRule.contains(rule),
      s"Mismatch in aggregation rule $rule vs ${submissionRequest.aggregationRule} at ${topologySnapshot.timestamp}",
    )
    val (inFlightAggregation, inFlightAggregationUpdate) = inFlightAggregationO match {
      case None =>
        // New aggregation
        val fresh = FreshInFlightAggregation(
          submissionRequest.maxSequencingTime,
          rule,
        )
        (
          InFlightAggregation.initial(fresh),
          InFlightAggregationUpdate(
            Some(fresh),
            Chain.empty,
          ),
        )
      case Some(inFlightAggregation) =>
        // Existing aggregation
        (inFlightAggregation, InFlightAggregationUpdate.empty)
    }

    val aggregatedSender = AggregatedSender(
      submissionRequest.sender,
      AggregationBySender(
        sequencingTimestamp,
        uncompressedBatch.envelopes.map(_.signatures),
      ),
    )
    for {
      updatedAggregation <-
        EitherT.fromEither[FutureUnlessShutdown](
          inFlightAggregation
            .tryAggregate(aggregatedSender)
            .leftMap {
              case InFlightAggregation.AlreadyDelivered(deliveredAt) =>
                val message =
                  s"The aggregatable request with aggregation ID $aggregationId was previously delivered at $deliveredAt"
                SubmissionOutcome.Reject.logAndCreate(
                  submissionRequest,
                  sequencingTimestamp,
                  SequencerErrors.AggregateSubmissionAlreadySent(message),
                )
              case InFlightAggregation.AggregationStuffing(_, at) =>
                val message =
                  s"The sender ${submissionRequest.sender} previously contributed to the aggregatable submission with ID $aggregationId at $at"
                SubmissionOutcome.Reject.logAndCreate(
                  submissionRequest,
                  sequencingTimestamp,
                  SequencerErrors.AggregateSubmissionStuffing(message),
                )
            }
        )

      fullInFlightAggregationUpdate = inFlightAggregationUpdate.tryMerge(
        InFlightAggregationUpdate(None, Chain.one(aggregatedSender))
      )
      // If we're not delivering the request to all recipients right now, just send a receipt back to the sender
      _ <- EitherT
        .cond(
          updatedAggregation.deliveredAt.nonEmpty,
          logger.debug(
            s"Aggregation ID $aggregationId has reached its threshold ${updatedAggregation.rule.threshold} and will be delivered at $sequencingTimestamp."
          ), {
            logger.debug(
              s"Aggregation ID $aggregationId has now ${updatedAggregation.aggregatedSenders.size} senders aggregated. Threshold is ${updatedAggregation.rule.threshold.value}."
            )
            // we only return a receipt, as we are still collecting aggregation results
            SubmissionOutcome.DeliverReceipt(
              submissionRequest,
              sequencingTimestamp,
              traceContext,
              trafficReceiptO = None, // traffic receipt is updated at the end of the processing
              inFlightAggregation =
                Some((aggregationId, updatedAggregation, fullInFlightAggregationUpdate)),
            ): SubmissionOutcome
          },
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield (updatedAggregation, fullInFlightAggregationUpdate)
  }

}
