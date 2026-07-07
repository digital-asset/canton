// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.base.error.{Alarm, AlarmErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyManagerErrorGroup
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.concurrent.ExecutionContext

object SubmissionTopologyHelper {

  object SubmissionTopologyErrors extends TopologyManagerErrorGroup() {
    @Explanation(
      "The participant has detected a transaction with a submission timestamp in the future."
    )
    @Resolution(
      "The participant will replace the submission timestamp with the sequencer timestamp."
    )
    object TopologyAlarm extends AlarmErrorCode("FUTURE_TOPOLOGY_ALARM") {
      final case class Warn(override val cause: String) extends Alarm(cause)
    }
  }

  /** Retrieve the topology snapshot used during submission by the submitter of a confirmation
    * request. This can be used to determine the impact of a topology change between submission and
    * sequencing. An example usage is during validation of a request: if some validation fails due
    * to such a change, the severity of the logs can sometimes be lowered from warning to info.
    *
    * Return `None` if the timestamp of the topology snapshot used at submission is too far in the
    * past compared to sequencing time (as determined by
    * [[com.digitalasset.canton.config.ProcessingTimeout.topologyChangeWarnDelay]]).
    *
    * @param sequencingTimestamp
    *   the timestamp at which the request was sequenced
    * @param submissionTopologyTimestamp
    *   the timestamp of the topology used at submission
    */
  def getSubmissionTopologySnapshot(
      timeouts: ProcessingTimeout,
      sequencingTimestamp: CantonTimestamp,
      submissionTopologyTimestamp: CantonTimestamp,
      crypto: SynchronizerCryptoClient,
  )(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerSnapshotSyncCryptoApi]] = {
    val maxDelay = timeouts.topologyChangeWarnDelay.toInternal
    val minTs = sequencingTimestamp - maxDelay

    // sanitize timestamp to guarantee that it is in the present/past
    val safeSubmissionTopologyTimestamp = if (submissionTopologyTimestamp > sequencingTimestamp) {
      SubmissionTopologyErrors.TopologyAlarm
        .Warn(
          s"Received future-dated submission timestamp $submissionTopologyTimestamp. " +
            s"Falling back to request timestamp $sequencingTimestamp."
        )
        .report()
      sequencingTimestamp
    } else {
      submissionTopologyTimestamp
    }

    if (safeSubmissionTopologyTimestamp >= minTs)
      crypto
        .awaitSnapshotUSSupervised(s"await crypto snapshot $submissionTopologyTimestamp")(
          safeSubmissionTopologyTimestamp
        )
        .map(syncCryptoApi => Some(syncCryptoApi))
    else {
      loggingContext.info(
        s"""Declared submission topology timestamp $submissionTopologyTimestamp is too far in the past (minimum
           |accepted: $minTs). Ignoring.
           |Note: this delay can be adjusted with the `topology-change-warn-delay` configuration parameter.""".stripMargin
          .replaceAll("\n", " ")
      )
      FutureUnlessShutdown.pure(None)
    }
  }
}
