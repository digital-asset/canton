// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.daml.error.Explanation
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
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
}
