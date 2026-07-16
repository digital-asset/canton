// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, SimpleSql}
import cats.syntax.all.*
import cats.{FunctorFilter, MonoidK}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.backend.RowDef.column
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  Conversions,
  RowDef,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{Party, SubmissionId, UserId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.{ByteString, any}
import com.google.rpc.status.Status as StatusProto

import java.sql.Connection
import java.util.UUID

class CompletionStorageBackendTemplate(
    stringInterning: StringInterning,
    ledgerEndCache: LedgerEndCache,
    val loggerFactory: NamedLoggerFactory,
) extends CompletionStorageBackend
    with NamedLogging {

  object RowDefs {
    import CommonRowDefs.*

    def userId(stringInterning: StringInterning): RowDef[UserId] =
      column("user_id", int).map(stringInterning.userId.externalize)
    val messageUuid: RowDef[UUID] = column("message_uuid", str).map(UUID.fromString)
    val submissionId: RowDef[SubmissionId] =
      column("submission_id", str).map(SubmissionId.assertFromString)
    val completionOffset: RowDef[Offset] = column("completion_offset", Conversions.offset)

    // rejection status
    private val rejectionStatusCode: RowDef[Int] = column("rejection_status_code", int)
    private val rejectionStatusMessage: RowDef[String] = column("rejection_status_message", str)
    private val rejectionStatusDetails: RowDef[Option[Array[Byte]]] =
      column("rejection_status_details", byteArray).?
    private val rejectionStatus: RowDef[StatusProto] =
      (rejectionStatusCode, rejectionStatusMessage, rejectionStatusDetails).mapN(buildStatusProto)

    // deduplication offset
    val deduplicationOffset: RowDef[Option[Long]] = column("deduplication_offset", long).?
    val deduplicationDurationSeconds: RowDef[Option[Long]] =
      column("deduplication_duration_seconds", long).?
    val deduplicationDurationNanos: RowDef[Option[Int]] =
      column("deduplication_duration_nanos", int).?

    // post publish related
    private val isTransaction: RowDef[Boolean] = column("is_transaction", bool)

    private val publishSource: RowDef[PublishSource] =
      (messageUuid.?, recordTime).mapN(publishSourceFromColumns)

    private val transactionHash: RowDef[Option[ByteString]] =
      column("transaction_hash", byteArray).?.map(_.map(ByteString.copyFrom(_)))

    private def commandCompletionSharedColumns(
        stringInterning: StringInterning,
        filterSubmitters: Seq[Party] => Set[String],
    ): RowDef[CompletionFromTransaction.CommonCompletionProperties] = (
      submitters(stringInterning).map(filterSubmitters),
      recordTime,
      completionOffset,
      commandId,
      userId(stringInterning),
      submissionId.?,
      synchronizerId(stringInterning).map(_.toProtoPrimitive),
      traceContext.map(Conversions.protoTraceContextFrom(noTracingLogger)),
      trafficCost.?.map(_.getOrElse(0L)),
      deduplicationOffset,
      deduplicationDurationSeconds,
      deduplicationDurationNanos,
      transactionHash,
    ).mapN(
      CompletionFromTransaction.CommonCompletionProperties.createFromRecordTimeAndSynchronizerId
    )

    private def filteredCommandCompletionSharedColumns(
        stringInterning: StringInterning,
        parties: Set[Party],
    ): RowDef[CompletionFromTransaction.CommonCompletionProperties] =
      commandCompletionSharedColumns(stringInterning, _.view.filter(parties).toSet[String])

    private def unfilteredCommandCompletionSharedColumns(
        stringInterning: StringInterning
    ): RowDef[CompletionFromTransaction.CommonCompletionProperties] =
      commandCompletionSharedColumns(stringInterning, _.toSet[String])

    def commandCompletionParser(
        parties: Set[Party]
    ): RowDef[CompletionStreamResponse] =
      commandCompletionParser(filteredCommandCompletionSharedColumns(stringInterning, parties))

    def unfilteredCommandCompletionParser: RowDef[CompletionStreamResponse] =
      commandCompletionParser(unfilteredCommandCompletionSharedColumns(stringInterning))

    private def commandCompletionParser(
        sharedColumns: RowDef[CompletionFromTransaction.CommonCompletionProperties]
    ): RowDef[CompletionStreamResponse] = updateId.?.map(_.isDefined).branch(
      (true, acceptedCommand(sharedColumns)),
      (false, rejectedCommand(sharedColumns)),
    )

    private def acceptedCommand(
        sharedColumns: RowDef[CompletionFromTransaction.CommonCompletionProperties]
    ): RowDef[CompletionStreamResponse] = (
      sharedColumns,
      updateId,
    ).mapN(CompletionFromTransaction.acceptedCompletion)

    private def rejectedCommand(
        sharedColumns: RowDef[CompletionFromTransaction.CommonCompletionProperties]
    ): RowDef[CompletionStreamResponse] = (
      sharedColumns,
      rejectionStatus,
    ).mapN(CompletionFromTransaction.rejectedCompletion)

    def unfilteredAcceptedCompletionParser: RowDef[Completion] = (
      unfilteredCommandCompletionSharedColumns(stringInterning),
      updateId,
    ).mapN(CompletionFromTransaction.toApiAcceptedCompletion)

    def unfilteredRejectedCompletionParser: RowDef[Completion] = (
      unfilteredCommandCompletionSharedColumns(stringInterning),
      rejectionStatus,
    ).mapN(CompletionFromTransaction.toApiRejectedCompletion)

    def acceptedCompletionParser(parties: Set[Party]): RowDef[Completion] = (
      filteredCommandCompletionSharedColumns(stringInterning, parties),
      updateId,
    ).mapN(CompletionFromTransaction.toApiAcceptedCompletion)

    def rejectedCompletionParser(parties: Set[Party]): RowDef[Completion] = (
      filteredCommandCompletionSharedColumns(stringInterning, parties),
      rejectionStatus,
    ).mapN(CompletionFromTransaction.toApiRejectedCompletion)

    private def postPublishDataForTransactionParser: RowDef[PostPublishData] = (
      synchronizerId(stringInterning),
      publishSource,
      userId(stringInterning),
      commandId,
      submitters(stringInterning).map(_.toSet),
      completionOffset,
      publicationTime.map(CantonTimestamp.apply),
      submissionId.?,
      updateId.?.map(_.isDefined),
      traceContext.map(Conversions.traceContextFrom(noTracingLogger)),
    ).mapN(PostPublishData.apply)

    def postPublishDataParser: RowDef[Option[PostPublishData]] = isTransaction.branch(
      (true, postPublishDataForTransactionParser.map(Some(_))),
      (false, RowDef.static(None)),
    )

    private def publishSourceFromColumns(messageUuid: Option[UUID], recordTime: Timestamp) =
      messageUuid
        .map(PublishSource.Local(_): PublishSource)
        .getOrElse(
          PublishSource.Sequencer(
            CantonTimestamp(recordTime)
          )
        )
  }

  override def commandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      userId: Option[UserId],
      parties: Set[Party],
      limit: Int,
  )(connection: Connection): Vector[CompletionStreamResponse] = {
    import ComposableQuery.*

    // If userId is specified but not yet interned, that user never wrote completions
    val userFilter: Option[CompositeSql] = userId match {
      case Some(uid) =>
        stringInterning.userId.tryInternalize(uid) match {
          case Some(internedUserId) => Some(cSQL"AND user_id = $internedUserId")
          case None => None // short-circuit below
        }
      case None => Some(cSQL"") // no user filter needed
    }

    userFilter match {
      case None => Vector.empty
      case Some(userClause) =>
        val parser =
          if (parties.nonEmpty) RowDefs.commandCompletionParser(parties)
          else RowDefs.unfilteredCommandCompletionParser

        val query = (columns: CompositeSql) =>
          SQL"""
            SELECT
              $columns
            FROM
              lapi_command_completions
            WHERE
              ${QueryStrategy.offsetIsBetween(
              nonNullableColumn = "completion_offset",
              startInclusive = startInclusive,
              endInclusive = endInclusive,
            )}
              $userClause
            ORDER BY completion_offset ASC
            ${QueryStrategy.limitClause(Some(limit))}"""

        val results = parser.queryMultipleRows(query)(connection)

        if (parties.nonEmpty)
          results.filter(_.getCompletion.actAs.nonEmpty)
        else
          results
    }
  }

  override def acceptedCompletionByHash(
      hash: Array[Byte],
      parties: Set[Party],
  )(
      connection: Connection
  ): Option[Completion] = {
    import ComposableQuery.*
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

    runByHashLookup[Option](
      parties = parties,
      filteredParser = RowDefs.acceptedCompletionParser,
      unfilteredParser = RowDefs.unfilteredAcceptedCompletionParser,
    ) { (parser, ledgerEndOffset) =>
      val query = (columns: CompositeSql) => SQL"""
          SELECT
            $columns
          FROM
            lapi_command_completions c
          WHERE
            c.completion_offset <= $ledgerEndOffset AND
            c.completion_offset = (
              SELECT m.event_offset FROM lapi_update_meta m
              WHERE m.transaction_hash = $hash
                AND m.event_offset <= $ledgerEndOffset
              LIMIT 1
            )"""
      parser.querySingleOptRow(query)(connection)
    }
  }

  override def rejectedCompletionsByHash(
      hash: Array[Byte],
      limit: Int,
      beforeOffset: Option[Offset],
      parties: Set[Party],
  )(
      connection: Connection
  ): Vector[Completion] = {
    import ComposableQuery.*
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

    // We apply the LIMIT in SQL and redact rows to the requesting parties only afterwards, which is
    // fine even if it looks like a dropped row could leave us with fewer than `limit` results while
    // there's more in the DB. The transaction hash covers the whole transaction, including the actAs
    // parties, so all rejections with the same hash have the same submitters. So the party filter
    // either keeps all of them or drops all of them, and fetching more rows would just give us ones
    // that get dropped the same way. The limited set we read is already the right one.
    runByHashLookup[Vector](
      parties = parties,
      filteredParser = RowDefs.rejectedCompletionParser,
      unfilteredParser = RowDefs.unfilteredRejectedCompletionParser,
    ) { (parser, ledgerEndOffset) =>
      val beforeOffsetFilter = beforeOffset match {
        case Some(offset) => cSQL"AND completion_offset < $offset"
        case None => cSQL""
      }
      val query = (columns: CompositeSql) => SQL"""
          SELECT
            $columns
          FROM
            lapi_command_completions
          WHERE
            transaction_hash = $hash AND
            update_id IS NULL AND
            completion_offset <= $ledgerEndOffset
            $beforeOffsetFilter
          ORDER BY completion_offset DESC
          ${QueryStrategy.limitClause(Some(limit))}"""
      parser.queryMultipleRows(query)(connection)
    }
  }

  /** Runs a by-hash completion lookup, bounding [[run]] by the current ledger end and applying the
    * party filter consistently across the accepted and rejected variants.
    *
    * The ledger-end bound is required because the completion and update_meta tables are written
    * before the ledger end advances, so a matching row may exist that is not yet visible via the
    * Ledger API.
    *
    * Security invariant: a single `parties.nonEmpty` check selects the redacting parser and drops
    * fully-redacted (empty `actAs`) completions together, so the two cannot drift; an empty party
    * set means no filtering (caller has `CanReadAsAnyParty`).
    */
  private def runByHashLookup[F[_]: MonoidK: FunctorFilter](
      parties: Set[Party],
      filteredParser: Set[Party] => RowDef[Completion],
      unfilteredParser: RowDef[Completion],
  )(
      run: (RowDef[Completion], Offset) => F[Completion]
  ): F[Completion] = {
    val ledgerEndOffset = ledgerEndCache().map(_.lastOffset)
    ledgerEndOffset.fold(MonoidK[F].empty[Completion]) { endOffset =>
      if (parties.nonEmpty)
        run(filteredParser(parties), endOffset).filter(_.actAs.nonEmpty)
      else
        run(unfilteredParser, endOffset)
    }
  }

  private def buildStatusProto(
      rejectionStatusCode: Int,
      rejectionStatusMessage: String,
      rejectionStatusDetails: Option[Array[Byte]],
  ): StatusProto =
    StatusProto.of(
      rejectionStatusCode,
      rejectionStatusMessage,
      parseRejectionStatusDetails(rejectionStatusDetails),
    )

  private def parseRejectionStatusDetails(
      rejectionStatusDetails: Option[Array[Byte]]
  ): Seq[any.Any] =
    rejectionStatusDetails
      .map(StatusDetails.parseFrom)
      .map(_.details)
      .getOrElse(Seq.empty)

  override def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, traceContext: TraceContext): Unit =
    pruneWithLogging(queryDescription = "Command completions pruning") {
      import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
      SQL"delete from lapi_command_completions where completion_offset <= $pruneUpToInclusive"
    }(connection, traceContext)

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(
      traceContext
    )
  }

  override def commandCompletionsForRecovery(
      startInclusive: Offset,
      endInclusive: Offset,
  )(connection: Connection): Vector[PostPublishData] = {
    import ComposableQuery.*
    def query(columns: CompositeSql) = SQL"""
      SELECT
        $columns
      FROM
        lapi_command_completions
      WHERE
        ${QueryStrategy.offsetIsBetween(
        nonNullableColumn = "completion_offset",
        startInclusive = startInclusive,
        endInclusive = endInclusive,
      )}
      ORDER BY completion_offset ASC"""

    RowDefs.postPublishDataParser.queryMultipleRows(query)(connection).flatten
  }
}
