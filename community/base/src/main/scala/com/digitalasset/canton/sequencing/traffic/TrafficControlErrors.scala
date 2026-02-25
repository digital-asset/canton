// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import com.digitalasset.base.error.{
  Alarm,
  AlarmErrorCode,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.TrafficControlErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

object TrafficControlErrors extends TrafficControlErrorGroup {
  sealed trait TrafficControlError extends Product with Serializable with ContextualizedCantonError

  @Explanation(
    """This error indicates that the participant does not have a traffic state."""
  )
  @Resolution(
    """Ensure that the the participant is connected to a synchronizer with traffic control enabled,
        and that it has received at least one event from the synchronizer since its connection."""
  )
  object TrafficStateNotFound
      extends ErrorCode(
        id = "TRAFFIC_CONTROL_STATE_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Traffic state not found"
        )
        with TrafficControlError
  }

  @Explanation(
    """This error indicates that the requested timestamp has not been reached by this sequencer yet."""
  )
  @Resolution(
    """Retry once the sequencer has reached the timestamp."""
  )
  object RequestedTimestampInTheFuture
      extends ErrorCode(
        id = "REQUESTED_TIMESTAMP_IN_THE_FUTURE",
        ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
      ) {
    final case class Error(timestamp: CantonTimestamp)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Timestamp in the future: $timestamp"
        )
        with TrafficControlError
  }

  @Explanation(
    """This error indicates that some of the requested timestamps have no corresponding events.
      |This may be because the events have already been pruned, because there was never
      |any event sequenced at these timestamps."""
  )
  @Resolution(
    """Request summaries at timestamps at which events have been sequenced."""
  )
  object NoEventAtTimestamps
      extends ErrorCode(
        id = "NO_EVENT_AT_TIMESTAMPS",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(timestamps: Set[CantonTimestamp])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"No events at timestamps: $timestamps"
        )
        with TrafficControlError
  }

  @Explanation(
    """This error indicates an error in generating the traffic summary for an envelope."""
  )
  @Resolution(
    """Inspect the details of the error for more details and resolution actions."""
  )
  object EnvelopeTrafficSummaryError
      extends ErrorCode(
        id = "ENVELOPE_TRAFFIC_SUMMARY_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(error: ProtoDeserializationError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Could not generate traffic summary for envelope: ${error.message}"
        )
        with TrafficControlError
  }

  @Explanation(
    """Traffic control is not active on the synchronizer."""
  )
  @Resolution(
    """Enable traffic control by setting the traffic control dynamic synchronizer parameter."""
  )
  object TrafficControlDisabled
      extends ErrorCode(
        id = "TRAFFIC_CONTROL_DISABLED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "TrafficControlDisabled"
        )
        with TrafficControlError
  }

  @Explanation(
    """Received an unexpected error when sending a top up submission request for sequencing."""
  )
  @Resolution(
    """Re-submit the top up request with an exponential backoff strategy."""
  )
  object TrafficPurchasedRequestAsyncSendFailed
      extends ErrorCode(
        id = "TRAFFIC_CONTROL_TOP_UP_SUBMISSION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(failureCause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = failureCause
        )
        with TrafficControlError
  }

  @Explanation(
    """The member received an invalid traffic control message. This may occur due to a bug at the sender of the message.
      |The message will be discarded. As a consequence, the underlying traffic purchased update will not take effect.
      |"""
  )
  @Resolution("Contact support")
  object InvalidTrafficPurchasedMessage
      extends AlarmErrorCode("INVALID_TRAFFIC_CONTROL_PURCHASED_MESSAGE") {
    final case class Error(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause)
        with TrafficControlError
        with PrettyPrinting {
      override protected def pretty: Pretty[Error] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
      )
    }
  }
  @Explanation(
    """The provided serial is lower or equal to the latest recorded balance update.
      |"""
  )
  @Resolution(
    """Re-submit the top up request with a serial above the latest recorded balance update."""
  )
  object TrafficControlSerialTooLow
      extends ErrorCode(
        "TRAFFIC_CONTROL_SERIAL_TOO_LOW",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(failureCause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = failureCause
        )
        with TrafficControlError
  }
}
