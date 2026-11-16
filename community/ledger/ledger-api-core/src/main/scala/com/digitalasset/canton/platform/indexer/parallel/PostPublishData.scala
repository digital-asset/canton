// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.{
  SequencedCommandRejected,
  SequencedReassignmentAccepted,
  SequencedTransactionAccepted,
  UnSequencedCommandRejected,
}
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref

import java.util.UUID

final case class PostPublishData(
    submissionSynchronizerId: SynchronizerId,
    publishSource: PublishSource,
    userId: Ref.UserId,
    commandId: Ref.CommandId,
    actAs: Set[Ref.Party],
    offset: Offset,
    publicationTime: CantonTimestamp,
    submissionId: Option[Ref.SubmissionId],
    accepted: Boolean,
    isTransaction: Boolean,
    traceContext: TraceContext,
)

object PostPublishData {
  def from(
      update: Update,
      offset: Offset,
      publicationTime: CantonTimestamp,
  ): Option[PostPublishData] = {
    def from(
        synchronizerId: SynchronizerId,
        publishSource: PublishSource,
        completionInfo: CompletionInfo,
        accepted: Boolean,
        isTransaction: Boolean,
    ): PostPublishData =
      PostPublishData(
        submissionSynchronizerId = synchronizerId,
        publishSource = publishSource,
        userId = completionInfo.userId,
        commandId = completionInfo.commandId,
        actAs = completionInfo.actAs.toSet,
        offset = offset,
        publicationTime = publicationTime,
        submissionId = completionInfo.submissionId,
        accepted = accepted,
        isTransaction = isTransaction,
        traceContext = update.traceContext,
      )

    update match {
      // isTransaction keeps the reassignments out of command deduplication, see InFlightSubmissionTracker
      case u: SequencedTransactionAccepted =>
        u.completionInfoO.map(completionInfo =>
          from(
            synchronizerId = u.synchronizerId,
            publishSource = PublishSource.Sequencer(u.recordTime),
            completionInfo = completionInfo,
            accepted = true,
            isTransaction = true,
          )
        )

      case u: SequencedReassignmentAccepted =>
        u.optCompletionInfo.map(completionInfo =>
          from(
            synchronizerId = u.synchronizerId,
            publishSource = PublishSource.Sequencer(u.recordTime),
            completionInfo = completionInfo,
            accepted = true,
            isTransaction = false,
          )
        )

      case u: SequencedCommandRejected =>
        Some(
          from(
            synchronizerId = u.synchronizerId,
            publishSource = PublishSource.Sequencer(u.recordTime),
            completionInfo = u.completionInfo,
            accepted = false,
            isTransaction = u.isTransaction,
          )
        )

      case u: UnSequencedCommandRejected =>
        Some(
          from(
            synchronizerId = u.synchronizerId,
            publishSource = PublishSource.Local(
              messageUuid = u.messageUuid
            ),
            completionInfo = u.completionInfo,
            accepted = false,
            isTransaction = u.isTransaction,
          )
        )

      case _ => None
    }
  }
}

sealed trait PublishSource extends Product with Serializable
object PublishSource {
  final case class Local(messageUuid: UUID) extends PublishSource
  final case class Sequencer(sequencerTimestamp: CantonTimestamp) extends PublishSource
}
