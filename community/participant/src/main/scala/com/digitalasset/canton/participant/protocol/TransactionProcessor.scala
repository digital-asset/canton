// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, NonEmptyChain}
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.participant.state.v2.{ChangeId, SubmitterInfo, TransactionMeta}
import com.digitalasset.canton._
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.SubmissionErrorGroup
import com.digitalasset.canton.error._
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.ProcessingSteps.WrapsProcessorError
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.ProcessorError
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionSubmitted
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.ConfirmationRequestCreationError
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.participant.protocol.submission.{
  ConfirmationRequestFactory,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.protocol.validation.{
  ConfirmationResponseFactory,
  ModelConformanceChecker,
}
import com.digitalasset.canton.participant.store.{DuplicateContract, SyncDomainEphemeralState}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.client.{SendAsyncClientError, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.DeliverErrorReason
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

class TransactionProcessor(
    participantId: ParticipantId,
    confirmationRequestFactory: ConfirmationRequestFactory,
    domainId: DomainId,
    damle: DAMLe,
    staticDomainParameters: StaticDomainParameters,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    ephemeral: SyncDomainEphemeralState,
    metrics: TransactionProcessingMetrics,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ProtocolProcessor[
      TransactionProcessingSteps.SubmissionParam,
      TransactionSubmitted,
      TransactionViewType,
      TransactionResultMessage,
      TransactionProcessor.TransactionSubmissionError,
    ](
      new TransactionProcessingSteps(
        domainId,
        participantId,
        confirmationRequestFactory,
        new ConfirmationResponseFactory(participantId, domainId, loggerFactory),
        ModelConformanceChecker(
          damle,
          confirmationRequestFactory.transactionTreeFactory,
          loggerFactory,
        ),
        staticDomainParameters,
        crypto,
        ephemeral.storedContractManager,
        metrics,
        loggerFactory,
      ),
      inFlightSubmissionTracker,
      ephemeral,
      crypto,
      sequencerClient,
      participantId,
      futureSupervisor,
      loggerFactory,
    ) {

  def submit(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: WellFormedTransaction[WithoutSuffixes],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessor.TransactionSubmissionError, Future[
    TransactionSubmitted
  ]] =
    this.submit(
      TransactionProcessingSteps.SubmissionParam(submitterInfo, transactionMeta, transaction)
    )
}

object TransactionProcessor {
  sealed trait TransactionProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrinting {
    override def underlyingProcessorError(): Option[ProcessorError] = None
  }

  trait TransactionSubmissionError extends TransactionProcessorError with TransactionError {
    override def pretty: Pretty[TransactionSubmissionError] = {
      this.prettyOfString(_ =>
        this.code.toMsg(cause, None) + "; " + CantonError.formatContextAsString(context)
      )
    }
  }

  object SubmissionErrors extends SubmissionErrorGroup {

    // TODO(i5990) split the text into sub-categories with codes
    @Explanation(
      """This error has not yet been properly categorised into sub-error codes."""
    )
    object MalformedRequest
        extends ErrorCode(
          id = "MALFORMED_REQUEST",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      // TODO(i5990) properly set `definiteAnswer` where appropriate when sub-categories are created
      case class Error(message: String, reason: ConfirmationRequestCreationError)
          extends TransactionErrorImpl(cause = "Malformed request")
    }

    @Explanation(
      """This error occurs if a transaction was submitted referring to a package that
        |a receiving participant has not vetted. Any transaction view can only refer to packages that have
        |explicitly been approved by the receiving participants."""
    )
    @Resolution(
      """Ensure that the receiving participant uploads and vets the respective package."""
    )
    object PackageNotVettedByRecipients
        extends ErrorCode(
          id = "PACKAGE_NO_VETTED_BY_RECIPIENTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(unknownTo: Seq[PackageUnknownTo])
          extends TransactionErrorImpl(
            cause =
              "Not all receiving participants have vetted a package that is referenced by the submitted transaction",
            // Reported asynchronously after in-flight submission checking, so covered by the rank guarantee
            definiteAnswer = true,
          )
    }

    // TODO(#7348) Add the submission rank of the in-flight submission
    case class SubmissionAlreadyInFlight(
        changeId: ChangeId,
        existingSubmissionId: Option[LedgerSubmissionId],
        existingSubmissionDomain: DomainId,
    ) extends TransactionErrorImpl(cause = "The submission is already in flight")(
          LedgerApiErrors.ConsistencyErrors.SubmissionAlreadyInFlight.code
        )
        with TransactionSubmissionError

    @Explanation(
      """This error occurs when the sequencer refuses to accept a command due to backpressure."""
    )
    @Resolution("Wait a bit and retry, preferably with some backoff factor.")
    object DomainBackpressure
        extends ErrorCode(id = "DOMAIN_BACKPRESSURE", ErrorCategory.ContentionOnSharedResources) {
      override def logLevel: Level = Level.WARN

      case class Rejection(reason: String)
          extends TransactionErrorImpl(
            cause = "The domain is overloaded.",
            // Only reported asynchronously, so covered by submission rank guarantee
            definiteAnswer = true,
          )
    }

    @Explanation(
      """This error occurs when a command is submitted while the system is performing a shutdown."""
    )
    @Resolution(
      "Assuming that the participant will restart or failover eventually, retry in a couple of seconds."
    )
    object SubmissionDuringShutdown
        extends ErrorCode(
          id = "SUBMISSION_DURING_SHUTDOWN",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      case class Rejection()
          extends TransactionErrorImpl(cause = "Command submitted during shutdown.")
          with TransactionSubmissionError
    }

    @Explanation("""This error occurs when the command cannot be sent to the domain.""")
    object SequencerRequest
        extends ErrorCode(
          id = "SEQUENCER_REQUEST_FAILED",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      // TODO(i5990) proper send async client errors
      //  SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded) is already mapped to DomainBackpressure
      case class Error(sendError: SendAsyncClientError)
          extends TransactionErrorImpl(
            cause = "Failed to send command",
            // Only reported asynchronously via timely rejections, so covered by submission rank guarantee
            definiteAnswer = true,
          )
    }

    @Explanation(
      """This error occurs when the domain refused to sequence the given message."""
    )
    object SequencerDeliver
        extends ErrorCode(
          id = "SEQUENCER_DELIVER_ERROR",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      // TODO(i5990) proper deliver errors
      case class Error(deliverErrorReason: DeliverErrorReason)
          extends TransactionErrorImpl(
            cause = "Failed to send command",
            // Only reported asynchronously via timely rejections, so covered by submission rank guarantee
            definiteAnswer = true,
          )
    }

    @Explanation(
      """This error occurs when the transaction was not sequenced within the pre-defined max-sequencing time
        |and has therefore timed out. The max-sequencing time is derived from the transaction's ledger time via
        |the ledger time model skews.
        |"""
    )
    @Resolution(
      """Resubmit if the delay is caused by high load.
        |If the command requires substantial processing on the participant, 
        |specify a higher minimum ledger time with the command submission so that a higher max sequencing time is derived.
        |"""
    )
    object TimeoutError
        extends ErrorCode(id = "NOT_SEQUENCED_TIMEOUT", ErrorCategory.ContentionOnSharedResources) {
      case class Error(timestamp: CantonTimestamp)
          extends TransactionErrorImpl(
            cause =
              "Transaction was not sequenced within the pre-defined max sequencing time and has therefore timed out"
          )
          with TransactionSubmissionError
    }

    @Explanation("The participant routed the transaction to a domain without an active mediator.")
    @Resolution("Add a mediator to the domain.")
    object DomainWithoutMediatorError
        extends ErrorCode(
          id = "DOMAIN_WITHOUT_MEDIATOR",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      case class Error(topology_snapshot_timestamp: CantonTimestamp, chosen_domain: DomainId)
          extends TransactionErrorImpl(
            cause = "There are no active mediators on the domain"
          )
          with TransactionSubmissionError
    }

    @Explanation(
      "The mediator chosen for the transaction got deactivated before the request was sequenced."
    )
    @Resolution("Resubmit.")
    // TODO(#5990) Generalize this for arbitrary requests, not just transactions
    object InactiveMediatorError
        extends ErrorCode(
          id = "CHOSEN_MEDIATOR_IS_INACTIVE",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      case class Error(chosen_mediator_id: MediatorId, timestamp: CantonTimestamp)
          extends TransactionErrorImpl(
            cause = "the chosen mediator is not active on the domain"
          )
    }
  }

  case class GenericStepsError(error: ProcessorError) extends TransactionProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)

    override def pretty: Pretty[GenericStepsError] = prettyOfParam(_.error)
  }

  case class ViewParticipantDataError(
      transactionId: TransactionId,
      viewHash: ViewHash,
      error: String,
  ) extends TransactionProcessorError {
    override def pretty: Pretty[ViewParticipantDataError] = prettyOfClass(
      param("transaction id", _.transactionId),
      param("view hash", _.viewHash),
      param("error", _.error.unquoted),
    )
  }

  case class FailedToStoreContract(error: NonEmptyChain[DuplicateContract])
      extends TransactionProcessorError {
    override def pretty: Pretty[FailedToStoreContract] = prettyOfClass(
      unnamedParam(_.error.toChain.toList)
    )
  }
  case class FieldConversionError(field: String, error: String) extends TransactionProcessorError {
    override def pretty: Pretty[FieldConversionError] = prettyOfClass(
      param("field", _.field.unquoted),
      param("error", _.error.unquoted),
    )
  }

  case object TransactionSubmitted
  type TransactionSubmitted = TransactionSubmitted.type
}
