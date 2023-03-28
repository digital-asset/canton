// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.api.util.TimeProvider
import com.daml.lf.data.{Ref, Time}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.Update
import com.digitalasset.canton.ledger.sandbox.bridge.LedgerBridge.{
  configChangedSuccess,
  packageUploadSuccess,
  partyAllocationSuccess,
  toOffset,
  transactionAccepted,
}
import com.digitalasset.canton.ledger.sandbox.domain.Submission

private[bridge] class PassThroughLedgerBridge(
    participantId: Ref.ParticipantId,
    timeProvider: TimeProvider,
) extends LedgerBridge {
  override def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission].zipWithIndex
      .map { case (submission, index) =>
        val nextOffset = toOffset(index)
        val update =
          successMapper(submission, index, participantId, timeProvider.getCurrentTimestamp)
        nextOffset -> update
      }

  private def successMapper(
      submission: Submission,
      index: Long,
      participantId: Ref.ParticipantId,
      currentTimestamp: Time.Timestamp,
  ): Update =
    submission match {
      case s: Submission.AllocateParty => partyAllocationSuccess(s, participantId, currentTimestamp)
      case s: Submission.Config => configChangedSuccess(s, participantId, currentTimestamp)
      case s: Submission.UploadPackages => packageUploadSuccess(s, currentTimestamp)
      case s: Submission.Transaction =>
        transactionAccepted(
          transactionSubmission = s,
          index = index,
          currentTimestamp = currentTimestamp,
        )
    }
}
