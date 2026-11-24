// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.channel.errors

import com.digitalasset.base.error.{
  Alarm,
  AlarmErrorCode,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

object SequencerChannelErrors extends SequencerErrorGroup {

  @Explanation(
    """The sequencer channel pool of this sequencer has no free capacity:
      |either the maximum number of sequencer channels or of concurrent channel connections
      |has been reached."""
  )
  @Resolution(
    """Retry with backoff. If the error persists, ask the sequencer operator to raise the channel
      |pool limits, or check for members keeping channels or pending connections open."""
  )
  object PoolCapacityExhausted
      extends ErrorCode(
        "SEQUENCER_CHANNEL_POOL_CAPACITY_EXHAUSTED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Sequencer channel pool has reached its maximum capacity. Try again later."
        ) {
      // Callers log the rejection themselves (the pool at INFO, ping at DEBUG),
      // so don't log again on creation.
      override def logOnCreation: Boolean = false
    }
  }

  @Explanation(
    """A sequencer channel member received a session key message that failed an integrity check
      |that neither a key rotation nor a version mismatch can explain. The member closes the channel
      |connection, and the connection is retried."""
  )
  @Resolution(
    """Investigate the sequencer the channel runs on and the other channel member."""
  )
  object SessionKeyRejected extends AlarmErrorCode("SEQUENCER_CHANNEL_SESSION_KEY_REJECTED") {
    final case class Warn(override val cause: String) extends Alarm(cause)
  }

  @Explanation(
    """A sequencer channel member did not accept the session key from the other channel member:
      |the other member or the sequencer relaying the channel runs an incompatible version. The
      |connection is retried."""
  )
  @Resolution(
    """Check that both channel members and the sequencers relaying the channel run compatible
      |versions."""
  )
  object SessionKeyNotAccepted
      extends ErrorCode(
        "SEQUENCER_CHANNEL_SESSION_KEY_NOT_ACCEPTED",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    final case class Warn(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
  }

}
