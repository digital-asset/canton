// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedCommandCompletionsReader.{
  CompletionsByHash,
  CompletionsFilter,
  mergeCompletionsByHash,
  planCompletionsByHash,
}
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{Party, UserId}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

class BufferedCommandCompletionsReader(
    bufferReader: BufferedStreamsReader[CompletionsFilter, CompletionStreamResponse],
    inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    dbReader: LedgerDaoCommandCompletionsReader,
)(implicit ec: ExecutionContext) {

  def getCommandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      userId: Option[UserId],
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed] =
    bufferReader
      .stream(
        startInclusive = startInclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = userId -> parties,
        bufferFilter = filterCompletions(_, parties, userId),
        toApiResponse = (response: CompletionStreamResponse) => Future.successful(response),
        descendingOrder = false,
        skipPruningChecks = false,
      )

  private def filterCompletions(
      transactionLogUpdate: TransactionLogUpdate,
      parties: Set[Party],
      userId: Option[UserId],
  ): Option[CompletionStreamResponse] = (transactionLogUpdate match {
    case accepted: TransactionLogUpdate.TransactionAccepted => accepted.completionStreamResponseO
    case rejected: TransactionLogUpdate.TransactionRejected =>
      Some(rejected.completionStreamResponse)
    case u: TransactionLogUpdate.ReassignmentAccepted => u.completionStreamResponseO
    case _: TransactionLogUpdate.TopologyTransactionEffective => None
    case _: TransactionLogUpdate.ReceivedAcsCommitment => None
  }).flatMap(toApiCompletion(_, parties, userId))

  private def toApiCompletion(
      completionStreamResponse: CompletionStreamResponse,
      parties: Set[Party],
      userId: Option[UserId],
  ): Option[CompletionStreamResponse] = {
    val completion = {
      val originalCompletion = completionStreamResponse.completionResponse.completion
        .getOrElse(throw new RuntimeException("No completion in completion stream response"))
      if (parties.isEmpty) originalCompletion
      else originalCompletion.withActAs(originalCompletion.actAs.filter(parties.map(_.toString)))
    }

    val visibilityPredicate =
      userId.forall(_.toString == completion.userId) &&
        completion.actAs.nonEmpty

    Option.when(visibilityPredicate)(
      CompletionStreamResponse.defaultInstance.withCompletion(completion)
    )
  }

  def getCompletionByHash(
      hash: Array[Byte],
      maxRejectedCompletions: Int,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[CompletionsByHash] = {
    val hashBytes = ByteString.copyFrom(hash)
    val (bufferedAcceptedTx, bufferedRejections) =
      inMemoryFanoutBuffer.lookupCompletionsByHash(hashBytes)

    val plan = planCompletionsByHash(
      bufferedAcceptedTx,
      bufferedRejections,
      maxRejectedCompletions,
      parties,
    )

    // Single DB call (or none) combining both concerns
    plan.dbQuery match {
      case Some(q) =>
        dbReader
          .getCompletionByHash(
            hash,
            q.maxRejectedCompletions,
            parties,
            q.rejectedBeforeOffset,
            q.includeAccepted,
          )
          .map(dbResult => mergeCompletionsByHash(plan, Some(dbResult)))
      case None =>
        Future.successful(mergeCompletionsByHash(plan, None))
    }
  }
}

object BufferedCommandCompletionsReader {
  private[dao] type Parties = Set[Party]
  private[dao] type CompletionsFilter = (Option[UserId], Parties)

  final case class CompletionsByHash(
      accepted: Option[Completion],
      rejected: Vector[Completion],
  )

  /** Arguments for the delegate DB call, derived purely from the buffer lookup and the request. */
  final case class DbCompletionsQuery(
      maxRejectedCompletions: Int,
      rejectedBeforeOffset: Option[Offset],
      includeAccepted: Boolean,
  )

  /** Buffer-side contribution plus the optional DB query to satisfy the remainder. */
  final case class CompletionsByHashPlan(
      acceptedFromBuffer: Option[Completion],
      rejectionsFromBuffer: Vector[Completion],
      dbQuery: Option[DbCompletionsQuery],
  )

  /** Decide the buffer contribution and what (if anything) to fetch from the DB. */
  private[dao] def planCompletionsByHash(
      bufferedAccepted: Option[TransactionLogUpdate.TransactionAccepted],
      bufferedRejections: Vector[TransactionLogUpdate.TransactionRejected],
      maxRejectedCompletions: Int,
      parties: Set[Party],
  ): CompletionsByHashPlan = {
    // Decide DB-need from the raw buffer lookup, not the filtered result:
    // `applyFilter` may drop the buffer hit entirely, which the DB also would have dropped.
    val needAcceptedFromDb = bufferedAccepted.isEmpty

    val acceptedFromBuffer: Option[Completion] =
      bufferedAccepted
        .flatMap(_.completionStreamResponseO)
        .flatMap(_.completionResponse.completion)
        .flatMap(applyFilter(_, parties))

    // Drop rejections not visible to the caller before they count toward the limit, keeping the
    // originating update so the DB boundary offset is taken from the last visible buffer hit.
    val visibleRejections: Vector[(TransactionLogUpdate.TransactionRejected, Completion)] =
      bufferedRejections.flatMap { rejection =>
        rejection.completionStreamResponse.completionResponse.completion
          .flatMap(applyFilter(_, parties))
          .map(rejection -> _)
      }

    val takenRejections = visibleRejections.take(maxRejectedCompletions)
    val rejectionsFromBuffer: Vector[Completion] = takenRejections.map(_._2)

    val dbRejectionLimit = maxRejectedCompletions - rejectionsFromBuffer.size
    val dbRejectionsBeforeOffset = takenRejections.lastOption.map(_._1.offset)

    val dbQuery =
      Option.when(needAcceptedFromDb || dbRejectionLimit > 0)(
        DbCompletionsQuery(dbRejectionLimit, dbRejectionsBeforeOffset, needAcceptedFromDb)
      )

    CompletionsByHashPlan(
      acceptedFromBuffer,
      rejectionsFromBuffer,
      dbQuery,
    )
  }

  /** Pure: applies the party filter to a single completion, mirroring the SQL push-down so buffer
    * hits and DB rows are filtered identically.
    *
    *   - Empty `parties` (caller has `CanReadAsAnyParty`): passthrough.
    *   - Non-empty `parties`: redact `actAs` to the visible parties, drop when nothing remains.
    */
  private[dao] def applyFilter(
      completion: Completion,
      parties: Set[Party],
  ): Option[Completion] =
    if (parties.isEmpty) Some(completion)
    else {
      val partyStrings = parties.map(_.toString)
      val redacted = completion.withActAs(completion.actAs.filter(partyStrings))
      Option.when(redacted.actAs.nonEmpty)(redacted)
    }

  /** Pure: merge the buffer contribution with the DB result. */
  private[dao] def mergeCompletionsByHash(
      plan: CompletionsByHashPlan,
      dbResult: Option[CompletionsByHash],
  ): CompletionsByHash =
    dbResult match {
      case Some(db) =>
        CompletionsByHash(
          accepted = db.accepted.orElse(plan.acceptedFromBuffer),
          // We don't re-cap here, but trust the dbReader to honor the rejection limit it was given.
          rejected = plan.rejectionsFromBuffer ++ db.rejected,
        )
      case None =>
        CompletionsByHash(plan.acceptedFromBuffer, plan.rejectionsFromBuffer)
    }

  def apply(
      dbReader: LedgerDaoCommandCompletionsReader,
      inMemoryFanoutBuffer: InMemoryFanoutBuffer,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): BufferedCommandCompletionsReader = {
    val fetchCompletions = new FetchFromPersistence[CompletionsFilter, CompletionStreamResponse] {
      override def apply(
          startInclusive: Offset,
          endInclusive: Offset,
          descendingOrder: Boolean,
          filter: (Option[UserId], Parties),
          skipPruningChecks: Boolean,
      )(implicit
          loggingContext: LoggingContextWithTrace
      ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
        require(!descendingOrder, s"This flow cannot use descending order")
        require(!skipPruningChecks, s"This flow cannot use skipping pruning checks")
        val (userId, parties) = filter
        dbReader
          .getCommandCompletions(
            startInclusive,
            endInclusive,
            userId,
            parties,
          )
      }
    }

    new BufferedCommandCompletionsReader(
      bufferReader = new BufferedStreamsReader[CompletionsFilter, CompletionStreamResponse](
        inMemoryFanoutBuffer = inMemoryFanoutBuffer,
        fetchFromPersistence = fetchCompletions,
        // Processing for completions is a no-op so it is unnecessary to have configurable parallelism.
        bufferedStreamEventsProcessingParallelism = 1,
        metrics = metrics,
        streamName = "completions",
        loggerFactory,
      ),
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      dbReader = dbReader,
    )
  }
}
