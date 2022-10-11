// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.daml.error.{Explanation, Resolution}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
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
}
