// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.ScalaPbMessageWithPrecomputedSerializedSize
import com.digitalasset.canton.platform.store.backend.CompletionStorageBackend
import com.digitalasset.canton.platform.store.dao.BufferedCommandCompletionsReader.CompletionsByHash
import com.digitalasset.canton.platform.store.dao.events.{OffsetRange, QueryValidRange}
import com.digitalasset.canton.platform.{Party, UserId}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** @param pageSize
  *   a single DB fetch query is guaranteed to fetch no more than this many results.
  */
private[dao] final class CommandCompletionsReader(
    dispatcher: DbDispatcher,
    storageBackend: CompletionStorageBackend,
    queryValidRange: QueryValidRange,
    metrics: LedgerApiServerMetrics,
    pageSize: Int,
    override protected val loggerFactory: NamedLoggerFactory,
) extends LedgerDaoCommandCompletionsReader
    with NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def offsetFor(response: CompletionStreamResponse): Offset =
    // It would be nice to obtain the offset such that it's obvious that it always exists (rather then relaying on calling .get)
    Offset.tryFromLong(response.completionResponse.completion.get.offset)

  override def getCommandCompletions(
      offsetRange: OffsetRange,
      userId: Option[UserId],
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val pruneSafeQuery =
      (range: OffsetRange) =>
        queryValidRange.withRangeNotPruned[Vector[CompletionStreamResponse]](
          offsetRange = range,
          errorPruning = (prunedOffset: Offset) =>
            s"Command completions request for $offsetRange overlaps with pruned offset ${prunedOffset.unwrap}",
          errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
            s"Command completions request for $offsetRange is beyond ledger end offset ${ledgerEndOffset
                .fold(0L)(_.unwrap)}",
        ) {
          dispatcher.executeSql(metrics.index.db.getCompletions)(
            storageBackend.commandCompletions(
              offsetRange = range,
              userId = userId,
              parties = parties,
              limit = pageSize,
            )
          )
        }

    val source: Source[CompletionStreamResponse, NotUsed] = paginatingAsyncStream
      .streamFromSeekPagination[OffsetRange, CompletionStreamResponse](
        startFromOffset = offsetRange,
        getOffset = (previousCompletion: CompletionStreamResponse) => {
          val lastOffset = offsetFor(previousCompletion)
          Option.when(lastOffset < offsetRange.endInclusive)(
            offsetRange.copy(startInclusive = lastOffset.increment)
          )
        },
      ) { (subRange: OffsetRange) =>
        pruneSafeQuery(subRange)
      }
    source.map(response => offsetFor(response) -> response.withPrecomputedSerializedSize())
  }

  override def getCompletionByHash(
      hash: Array[Byte],
      maxRejectedCompletions: Int,
      parties: Set[Party],
      rejectedBeforeOffset: Option[Offset],
      includeAccepted: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[CompletionsByHash] =
    dispatcher.executeSql(metrics.index.db.getCompletionByHash) { connection =>
      CompletionsByHash(
        accepted =
          if (includeAccepted) storageBackend.acceptedCompletionByHash(hash, parties)(connection)
          else None,
        rejected = storageBackend.rejectedCompletionsByHash(
          hash,
          maxRejectedCompletions,
          rejectedBeforeOffset,
          parties,
        )(
          connection
        ),
      )
    }
}
