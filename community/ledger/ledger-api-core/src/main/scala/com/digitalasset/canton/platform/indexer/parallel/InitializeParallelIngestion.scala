// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.indexer.parallel.AchsMaintenancePipe.AchsWorkDistance
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsLastPointers,
  AchsState,
  LedgerEnd,
}
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  IngestionStorageBackend,
  ParameterStorageBackend,
  StringInterningStorageBackend,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interning.UpdatingStringInterningView
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

private[platform] final case class InitializeParallelIngestion(
    providedParticipantId: Ref.ParticipantId,
    ingestionStorageBackend: IngestionStorageBackend[?],
    parameterStorageBackend: ParameterStorageBackend,
    completionStorageBackend: CompletionStorageBackend,
    stringInterningStorageBackend: StringInterningStorageBackend,
    updatingStringInterningView: UpdatingStringInterningView,
    postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
    metrics: LedgerApiServerMetrics,
    loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def apply(
      dbDispatcher: DbDispatcher,
      initializeInMemoryState: (Option[LedgerEnd], AchsState) => Future[Unit],
      achsConfig: Option[AchsConfig],
  ): Future[(Option[LedgerEnd], AchsWorkDistance)] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(logger)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.empty
    logger.info(s"Attempting to initialize with participant ID $providedParticipantId")
    for {
      _ <- dbDispatcher.executeSql(metrics.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            participantId = ParticipantId(providedParticipantId)
          ),
          loggerFactory,
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.indexer.initialization)(
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
      (initialAchsState, initialAchsWork) <- achsConfig match {
        case Some(config) =>
          dbDispatcher.executeSql(metrics.indexer.initialization) { connection =>
            // TODO(#30241) handle already existing ACHS state in the database, for now we assume that the table is empty
            val achsState =
              ParameterStorageBackend.AchsState(
                validAt = 0,
                lastPointers = AchsLastPointers(lastRemoved = 0, lastPopulated = 0),
              )
            parameterStorageBackend.insertACHSState(achsState)(connection)
            val lastEventSeqId = ledgerEnd.map(_.lastEventSeqId).getOrElse(0L)
            achsState -> AchsMaintenancePipe.initialWork(achsState, lastEventSeqId, config)
          }
        case None =>
          Future.successful(
            // they are not used when ACHS is disabled
            AchsState(
              validAt = 0L,
              lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
            ) ->
              AchsWorkDistance(populate = 0L, remove = 0L)
          )
      }
      _ <- updatingStringInterningView.update(ledgerEnd.map(_.lastStringInterningId)) {
        (fromExclusive, toInclusive) =>
          implicit val loggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.empty
          dbDispatcher.executeSql(metrics.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
      }
      // post processing recovery should come after initializing string interning when the dependent storage backend operations are running
      postProcessingEndOffset <- dbDispatcher.executeSql(metrics.index.db.getPostProcessingEnd)(
        parameterStorageBackend.postProcessingEnd
      )
      potentiallyNonPostProcessedCompletions <- ledgerEnd.map(_.lastOffset) match {
        case Some(lastOffset) =>
          dbDispatcher.executeSql(
            metrics.index.db.getPostProcessingEnd
          )(
            completionStorageBackend.commandCompletionsForRecovery(
              startInclusive = postProcessingEndOffset.fold(Offset.firstOffset)(_.increment),
              endInclusive = lastOffset,
            )
          )
        case None => Future.successful(Vector.empty)
      }
      _ <- postProcessor(potentiallyNonPostProcessedCompletions, loggingContext.traceContext)
      _ <- dbDispatcher.executeSql(metrics.indexer.postProcessingEndIngestion)(
        parameterStorageBackend.updatePostProcessingEnd(ledgerEnd.map(_.lastOffset))
      )
      _ = logger.info(s"Indexer initialized at $ledgerEnd")
      _ <- initializeInMemoryState(ledgerEnd, initialAchsState)
    } yield (ledgerEnd, initialAchsWork)
  }
}
