// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.data.{EitherT, WriterT}
import cats.kernel.Monoid
import cats.syntax.either.*
import com.digitalasset.base.error.BaseAlarm
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.crypto.SignatureCheckError.GeneralError
import com.digitalasset.canton.crypto.{HashPurpose, SignatureCheckError, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl.PrevalidationOutcome
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

import SubmissionRequestValidator.*

/** Validates a single [[SubmissionRequest]] within a chunk.
  *
  * Can run in parallel to other submission requests.
  *
  * NOTE RENAME ME INTO PARALLESUBMISSIONREQUESTVALIDATOR
  */
private[update] final class SubmissionRequestValidator(
    inFlightAggregationHandler: InFlightAggregationHandler,
    batchingConfig: BatchingConfig,
    override val loggerFactory: NamedLoggerFactory,
    memberValidator: SequencerMemberValidator,
    protocolVersion: ProtocolVersion,
) extends NamedLogging {

  /** Performs validations that don't affect any state and resolves groups to members. Can and
    * should be run in parallel on many submissions (e.g. in a chunk).
    *
    * Upon a Left, we stop processing the submission request and produce the sequenced output event.
    * We use [[SequencedEventValidation]] as a return result in order to capture whether the event,
    * despite returning a left, should be charged to the submitting sender.
    *
    * We can do this after we validated the submission signature.
    *
    * @param skipFreshInFlightValidationCheck
    *   small optimization to skip the member check if the aggregation id is already known
    */
  def performIndependentValidations(
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: Traced[SignedContent[SubmissionRequest]],
      snapshotToValidateSubmissionRequest: SyncCryptoApi,
      topologySnapshotFromRequestO: Option[SyncCryptoApi],
      topologyTimestampError: Option[SequencerDeliverError],
      skipFreshInFlightValidationCheck: AggregationId => Boolean,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): SequencedEventValidation[PrevalidationOutcome] = {
    val submissionRequest = signedSubmissionRequest.value.content
    for {
      isSenderRegistered <-
        EitherT
          .right(
            memberValidator.isMemberRegisteredAt(submissionRequest.sender, sequencingTimestamp)
          )
          .mapK(validationFUSK)
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          isSenderRegistered,
          // we expect callers to validate the sender exists before queuing requests on their behalf
          // if we hit this case here it likely means the caller didn't check, or the member has subsequently
          // been deleted.
          {
            logger.warn(
              s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] " +
                "is not registered so cannot send or receive events. Dropping send request."
            )
            SubmissionOutcome.Discard
          },
        )
        .mapK(validationFUSK)
      // Warn if we use an approximate snapshot but only after we've read at least one
      _ <- checkSignatureOnSubmissionRequest(
        signedSubmissionRequest,
        snapshotToValidateSubmissionRequest,
      ).mapK(validationFUSK)
      // At this point we know the sender has indeed properly signed the submission request
      // so we'll want to run the traffic control logic
      _ <- EitherT.liftF[SequencedEventValidationF, SubmissionOutcome, Unit](
        WriterT.tell(TrafficConsumption(true))
      )
      _ <- EitherT.cond[SequencedEventValidationF](
        sequencingTimestamp <= submissionRequest.maxSequencingTime,
        (),
        // The sequencer is beyond the timestamp allowed for sequencing this request so it is silently dropped.
        // A correct sender should be monitoring their sequenced events and notice that the max-sequencing-time has been
        // exceeded and trigger a timeout.
        // We don't log this as a warning as it is expected behaviour. Within a distributed network, the source of
        // a delay can come from different nodes and we should only log this as a warning in a way where we can
        // attribute the delay to a specific node.
        {
          SequencerError.ExceededMaxSequencingTime
            .Error(
              sequencingTimestamp,
              submissionRequest.maxSequencingTime,
              submissionRequest.messageId.unwrap,
            )
            .discard
          SubmissionOutcome.Discard
        },
      )
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          SubmissionRequestValidations.checkToAtMostOneMediator(submissionRequest),
          (), {
            SequencerError.MultipleMediatorRecipients
              .Error(submissionRequest, sequencingTimestamp)
              .report()
            SubmissionOutcome.Discard: SubmissionOutcome
          },
        )
        .mapK(validationFUSK)
      _ <- checkRecipientsAreKnown(
        submissionRequest,
        sequencingTimestamp,
      ).mapK(validationFUSK)
      _ <- EitherT.fromEither[SequencedEventValidationF](
        validateTopologyTimestamp(
          sequencingTimestamp,
          submissionRequest,
          topologyTimestampError,
        )
      )
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      topologyOrSequencingSnapshot = topologySnapshotFromRequestO.getOrElse(
        snapshotToValidateSubmissionRequest
      )
      // Preserve the old behavior to avoid ledger forks for older protocols.
      _ <- (if (protocolVersion <= ProtocolVersion.v34)
              inFlightAggregationHandler.validateMaxSequencingTimeForAggregationRule(
                submissionRequest,
                topologyOrSequencingSnapshot,
                sequencingTimestamp,
              )
            else EitherTUtil.unitUS).mapK(validationFUSK)
      _ <- checkClosedEnvelopesSignatures(
        topologyOrSequencingSnapshot,
        signedSubmissionRequest.map(_.content),
        sequencingTimestamp,
      ).mapK(validationFUSK)
      recipients <-
        resolveSubmissionRequestRecipients(
          submissionRequest,
          sequencingTimestamp,
          topologyOrSequencingSnapshot,
        ).mapK(validationFUSK)
      // compute the aggregation id last (we keep this here as the old code computed
      // the aggregation id as first step in the sequential stage. we now do this in the
      // parallel stage but preserve the same order of operation)
      aggregationInfo <- inFlightAggregationHandler
        .computeAggregationIdAndValidateAggregationRule(
          sequencingTimestamp,
          topologyOrSequencingSnapshot,
          submissionRequest,
          skipFreshInFlightValidationCheck,
        )
        .mapK(validationFUSK)
    } yield PrevalidationOutcome(recipients, aggregationInfo)
  }

  // TODO(#18401): This method should be harmonized with the GroupAddressResolver
  private def resolveSubmissionRequestRecipients(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologyOrSequencingSnapshot: SyncCryptoApi,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Set[MemberRecipientOrBroadcast]] = {
    val groupRecipients = submissionRequest.batch.allRecipients.collect {
      // Note: we don't resolve AllMembersOfSynchronizer as it is encoded as -1 and handled internally by db sequencer
      case group: GroupRecipient if group != AllMembersOfSynchronizer =>
        group
    }

    val hasBroadcast = submissionRequest.batch.allRecipients.contains(AllMembersOfSynchronizer)

    if (groupRecipients.isEmpty)
      EitherT.rightT(Set.empty)
    else
      for {
        mediatorGroupsMembers <-
          expandMediatorGroupRecipients(
            submissionRequest,
            sequencingTimestamp,
            groupRecipients,
            topologyOrSequencingSnapshot,
          )
        sequencersOfSynchronizerMembers <-
          expandSequencersOfSynchronizerGroupRecipients(
            submissionRequest,
            sequencingTimestamp,
            topologyOrSequencingSnapshot,
            groupRecipients,
          )
      } yield (mediatorGroupsMembers ++ sequencersOfSynchronizerMembers).map(member =>
        MemberRecipient(member)
      ) ++ (if (hasBroadcast) Set(AllMembersOfSynchronizer) else Set.empty)
  }

  private def expandSequencersOfSynchronizerGroupRecipients(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      groupRecipients: Set[GroupRecipient],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Set[Member]] = {
    val useSequencersOfSynchronizer = groupRecipients.contains(SequencersOfSynchronizer)
    if (useSequencersOfSynchronizer) {
      for {
        sequencers <- EitherT(
          topologyOrSequencingSnapshot.ipsSnapshot
            .sequencerGroup()
            .map(
              _.fold[Either[SubmissionOutcome, Set[Member]]](
                // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                Left(
                  SubmissionOutcome.Reject.logAndCreate(
                    submissionRequest,
                    sequencingTimestamp,
                    SequencerErrors.SubmissionRequestRefused("No sequencer group found"),
                  )
                )
              )(group => Right((group.active.forgetNE ++ group.passive).toSet[Member]))
            )
        )
      } yield Map((SequencersOfSynchronizer: GroupRecipient) -> sequencers).values.toSet.flatten
    } else
      EitherT.rightT[FutureUnlessShutdown, SubmissionOutcome](
        Set.empty[Member]
      )
  }

  private def expandMediatorGroupRecipients(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: SyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Set[Member]] = {
    val mediatorGroups =
      groupRecipients.collect { case MediatorGroupRecipient(group) =>
        group
      }.toSeq
    if (mediatorGroups.isEmpty)
      EitherT.rightT[FutureUnlessShutdown, SubmissionOutcome](
        Set.empty
      )
    else
      for {
        groups <- topologyOrSequencingSnapshot.ipsSnapshot
          .mediatorGroupsOfAll(mediatorGroups)
          .leftMap(nonExistingGroups =>
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            SubmissionOutcome.Reject.logAndCreate(
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The following mediator groups do not exist $nonExistingGroups"
              ),
            ): SubmissionOutcome
          )
        _ <- MonadUtil.parTraverseWithLimit(batchingConfig.parallelism)(groups) { group =>
          val nonRegisteredF =
            memberValidator
              .areMembersRegisteredAt(group.active ++ group.passive, sequencingTimestamp)
              .map(_.flatMap {
                case (member, false) => Some(member)
                case (member, true) => None
              })

          EitherT(
            nonRegisteredF.map { nonRegistered =>
              Either.cond(
                nonRegistered.isEmpty,
                (),
                // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                SubmissionOutcome.Reject.logAndCreate(
                  submissionRequest,
                  sequencingTimestamp,
                  SequencerErrors.SubmissionRequestRefused(
                    s"The mediator group ${group.index} contains non registered mediators $nonRegistered"
                  ),
                ): SubmissionOutcome,
              )
            }
          )
        }
      } yield GroupAddressResolver.asGroupRecipientsToMembers(groups).values.toSet.flatten
  }

  private def checkClosedEnvelopesSignatures(
      topologyOrSequencingSnapshot: SyncCryptoApi,
      submissionRequest: Traced[SubmissionRequest],
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Unit] =
    MonadUtil
      .parTraverseWithLimit_(batchingConfig.parallelism)(submissionRequest.value.batch.envelopes) {
        closedEnvelope =>
          EitherT
            .fromEither[FutureUnlessShutdown](
              closedEnvelope.toClosedUncompressedEnvelopeResult
                .leftMap[SignatureCheckError] { deserializationError =>
                  GeneralError(new IllegalArgumentException(deserializationError.message))
                }
            )
            .flatMap { closedUncompressedEnvelope =>
              closedUncompressedEnvelope.verifySignatures(
                topologyOrSequencingSnapshot,
                submissionRequest.value.sender,
              )
            }
      }
      .leftMap { error =>
        SequencerError.InvalidEnvelopeSignature
          .Error(
            submissionRequest.value,
            error,
            sequencingTimestamp,
            topologyOrSequencingSnapshot.ipsSnapshot.timestamp,
          )
          .report()
        SubmissionOutcome.Discard
      }

  private def checkRecipientsAreKnown(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Unit] =
    // group addresses checks are covered separately later on
    for {
      unknownRecipients <-
        EitherT
          .right(
            memberValidator
              .areMembersRegisteredAt(submissionRequest.batch.allMembers.toSeq, sequencingTimestamp)
              .map(_.flatMap {
                case (_, true) => None
                case (member, false) => Some(member)
              }.toSeq)
          )

      res <- EitherT.cond[FutureUnlessShutdown](
        unknownRecipients.isEmpty,
        (),
        SubmissionOutcome.Reject.logAndCreate(
          submissionRequest,
          sequencingTimestamp,
          SequencerErrors.UnknownRecipients(unknownRecipients),
        ): SubmissionOutcome,
      )
    } yield res

  private def checkSignatureOnSubmissionRequest(
      signedSubmissionRequest: Traced[SignedContent[SubmissionRequest]],
      topologyOrSequencingSnapshot: SyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionOutcome, Unit] = {

    val alarm = for {
      _ <-
        signedSubmissionRequest.value
          .verifySignature(
            topologyOrSequencingSnapshot,
            signedSubmissionRequest.value.content.sender,
            HashPurpose.SubmissionRequestSignature,
          )
          .leftMap[BaseAlarm](error =>
            SequencerError.InvalidSubmissionRequestSignature.Error(
              signedSubmissionRequest.value,
              error,
              topologyOrSequencingSnapshot.ipsSnapshot.timestamp,
              signedSubmissionRequest.value.timestampOfSigningKey,
            )
          )
    } yield ()

    alarm.leftMap { a =>
      a.report()
      SubmissionOutcome.Discard
    }
  }

  private def validateTopologyTimestamp(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      topologyTimestampError: Option[SequencerDeliverError],
  )(implicit
      traceContext: TraceContext
  ): Either[SubmissionOutcome, Unit] =
    topologyTimestampError
      .map(
        SubmissionOutcome.Reject.logAndCreate(
          submissionRequest,
          sequencingTimestamp,
          _,
        )
      )
      .toLeft(())

}

private[update] object SubmissionRequestValidator {
  // Effect type used in validation flow - passes along the traffic consumption state that is utilized
  // at the end of the processing to decide on traffic consumption
  type SequencedEventValidationF[A] = WriterT[FutureUnlessShutdown, TrafficConsumption, A]
  // Type of validation methods, uses SequencedEventValidationF as the F of an EitherT
  // This gives us short circuiting semantics while having access to the traffic consumption state at the end
  type SequencedEventValidation[A] = EitherT[SequencedEventValidationF, SubmissionOutcome, A]
  def validationFUSK(implicit executionContext: ExecutionContext) =
    WriterT.liftK[FutureUnlessShutdown, TrafficConsumption]
  def validationK(implicit executionContext: ExecutionContext) =
    FutureUnlessShutdown.outcomeK.andThen(validationFUSK)

  object TrafficConsumption {
    implicit val accumulatedTrafficCostMonoid: Monoid[TrafficConsumption] =
      new Monoid[TrafficConsumption] {
        override def empty: TrafficConsumption = TrafficConsumption(false)
        override def combine(x: TrafficConsumption, y: TrafficConsumption): TrafficConsumption =
          TrafficConsumption(x.consume || y.consume)
      }
  }

  /** Encodes whether or not traffic should be consumed for the sender for a sequenced event.
    * Currently this is just a boolean but can be expended later to cover more granular cost
    * accumulation depending on delivery, validation etc...
    */
  final case class TrafficConsumption(consume: Boolean)

  final case class SubmissionRequestValidationResult(
      inFlightAggregations: InFlightAggregations,
      outcome: SubmissionOutcome,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  ) {

    // When we reach the end of the validation, we decide based on the outcome so far
    // if we should try to consume traffic for the event.
    def shouldTryToConsumeTraffic: Boolean = outcome match {
      // The happy case where the request will be delivered - traffic should be consumed
      case _: SubmissionOutcome.Deliver => true
      // This is a deliver receipt from an aggregated submission - traffic should be consumed
      case _: SubmissionOutcome.DeliverReceipt => true
      // If the submission is rejected, the sender will receive a receipt notifying it of the rejection
      // At this point we assume all rejections can be verified by the sender, and therefore
      // we consume the cost. We can be more granular if necessary by deciding differently based on the
      // actual reason for the rejection
      case _: SubmissionOutcome.Reject => true
      // If the submission is discarded, nothing is sent back to the sender
      // In that case we do not consume anything
      case SubmissionOutcome.Discard => false
    }

    // Wasted traffic is defined as events that have been sequenced but will not be delivered to their
    // recipients. This method return a Some with the reason if the traffic was wasted, None otherwise
    def wastedTrafficReason: Option[String] = outcome match {
      // Only events that are delivered are not wasted
      case _: SubmissionOutcome.Deliver => None
      case _: SubmissionOutcome.DeliverReceipt => None
      case reject: SubmissionOutcome.Reject =>
        CantonBaseError.statusErrorCodes(reject.error).headOption
      case SubmissionOutcome.Discard => Some("discarded")
    }

    def updateTrafficReceipt(
        trafficReceipt: Option[TrafficReceipt]
    ): SubmissionRequestValidationResult =
      copy(outcome = outcome.updateTrafficReceipt(trafficReceipt))
  }
}
