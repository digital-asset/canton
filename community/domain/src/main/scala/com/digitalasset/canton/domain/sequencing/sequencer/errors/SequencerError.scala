// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.daml.error.{Explanation, Resolution}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.protocol.{SignedContent, SubmissionRequest}
import com.digitalasset.canton.topology.Member

trait SequencerError extends CantonError with Product with Serializable

object SequencerError extends SequencerErrorGroup {

  @Explanation("""
                 |This error indicates that the member has acknowledged a timestamp that is after the events 
                 |it has received. This violates the sequencing protocol. 
                 |""")
  object InvalidAcknowledgement extends AlarmErrorCode("INVALID_ACKNOWLEDGEMENT_TIMESTAMP") {

    case class Error(
        member: Member,
        acked_timestamp: CantonTimestamp,
        latest_valid_timestamp: CantonTimestamp,
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm(
          s"Member $member has acknowledged the timestamp $acked_timestamp when only events with timestamps at most $latest_valid_timestamp have been delivered."
        )
        with CantonError
  }

  @Explanation("""
                 |This error indicates that some sequencer node has distributed an invalid sequencer pruning request via the blockchain.
                 |Either the sequencer nodes got out of sync or one of the sequencer nodes is buggy.
                 |The sequencer node will stop processing to prevent the danger of severe data corruption.
                 |""")
  @Resolution(
    """Stop using the domain involving the sequencer nodes. Contact support."""
  )
  object InvalidPruningRequestOnChain
      extends AlarmErrorCode("INVALID_SEQUENCER_PRUNING_REQUEST_ON_CHAIN") {
    case class Error(
        blockHeight: Long,
        blockLatestTimestamp: CantonTimestamp,
        safePruningTimestamp: CantonTimestamp,
        invalidPruningRequests: Seq[CantonTimestamp],
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm(
          s"Pruning requests in block $blockHeight are unsafe: previous block's latest timestamp $blockLatestTimestamp, safe pruning timestamp $safePruningTimestamp, unsafe pruning timestamps: $invalidPruningRequests"
        )
        with CantonError
  }

  @Explanation(
    """This error means that the request size has exceeded the configured value maxInboundMessageSize."""
  )
  @Resolution(
    """Send smaller requests or increase the maxInboundMessageSize in the domain parameters"""
  )
  object MaxRequestSizeExceeded extends AlarmErrorCode("MAX_REQUEST_SIZE_EXCEEDED") {

    case class Error(message: String, maxRequestSize: NonNegativeInt) extends Alarm(message)
  }

  @Explanation("""
                 |This error indicates that the sequencer has detected an invalid submission request signature.
                 |This most likely indicates that the request is bogus and has been created by a malicious sequencer.
                 |So it will not get processed.
                 |""")
  object InvalidSubmissionRequestSignature
      extends AlarmErrorCode("INVALID_SUBMISSION_REQUEST_SIGNATURE") {
    case class Error(
        signedSubmissionRequest: SignedContent[SubmissionRequest],
        error: SignatureCheckError,
        sequencingTimestamp: CantonTimestamp,
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm({
          val submissionRequest = signedSubmissionRequest.content
          s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] provided signature that failed to be verified. " +
            s"Could not sequence at $sequencingTimestamp: $error"
        })
        with CantonError {
      override val logOnCreation: Boolean = false
    }
  }

  @Explanation("""
      |This error indicates that the sequencer has detected that the signed submission request being processed is missing a signature timestamp.
      |It indicates that the sequencer node that placed the request is not following the protocol as there should always be a defined timestamp.
      |This request will not get processed.
      |""")
  object MissingSubmissionRequestTimestampSignature
      extends AlarmErrorCode("MISSING_SUBMISSION_REQUEST_SIGNATURE_TIMESTAMP") {
    case class Error(
        signedSubmissionRequest: SignedContent[SubmissionRequest],
        sequencingTimestamp: CantonTimestamp,
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm({
          val submissionRequest = signedSubmissionRequest.content
          s"Send request [${submissionRequest.messageId}] by sender [${submissionRequest.sender}] is missing a signature timestamp. " +
            s"Could not sequence at $sequencingTimestamp"
        })
        with CantonError {
      override val logOnCreation: Boolean = false
    }
  }

  @Explanation("""
      |This error indicates that the sequencer has detected a signed submission request with a signing timestamp after the sequencing timestamp.
      |It will not get processed.
      |""")
  object InvalidSubmissionRequestSignatureTimestamp
      extends AlarmErrorCode("INVALID_SUBMISSION_REQUEST_SIGNATURE_TIMESTAMP") {
    case class Error(
        submissionRequest: SubmissionRequest,
        signingTimestamp: CantonTimestamp,
        sequencingTimestamp: CantonTimestamp,
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm(
          s"Send request [${submissionRequest.messageId}] by sender [${submissionRequest.sender}] has an " +
            s"invalid signing timestamp $signingTimestamp that is after current sequencing time $sequencingTimestamp. Could not sequence."
        )
        with CantonError {
      override val logOnCreation: Boolean = false
    }
  }
}
