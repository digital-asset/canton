// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.error.ContextualizedErrorLogger
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.{Metrics, Timed}
import com.digitalasset.canton.ledger.error.IndexErrors
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.ContractsReader.*
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*
import com.digitalasset.canton.platform.store.serialization.{Compression, ValueSerializer}
import com.digitalasset.canton.platform.{Contract, ContractId, Identifier, Key, Party, Value}

import java.io.ByteArrayInputStream
import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class ContractsReader(
    contractLoader: ContractLoader,
    storageBackend: ContractStorageBackend,
    dispatcher: DbDispatcher,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader
    with NamedLogging {

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key the contract key
    * @param validAt the event_sequential_id of the ledger at which to query for the key state
    * @return the key state.
    */
  override def lookupKeyState(key: Key, validAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState] =
    Timed.future(
      metrics.daml.index.db.lookupKey,
      dispatcher.executeSql(metrics.daml.index.db.lookupContractByKeyDbMetrics)(
        storageBackend.keyState(key, validAt)
      ),
    )

  override def lookupContractState(contractId: ContractId, before: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractState]] = {
    implicit val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext(logger, loggingContext)
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      contractLoader
        .load(contractId -> before)
        .map(_.map {
          case raw if raw.eventKind == 10 =>
            val contract = toContract(
              contractId = contractId,
              templateId =
                assertPresent(raw.templateId)("template_id must be present for a create event"),
              createArgument = assertPresent(raw.createArgument)(
                "create_argument must be present for a create event"
              ),
              createArgumentCompression =
                Compression.Algorithm.assertLookup(raw.createArgumentCompression),
              decompressionTimer =
                metrics.daml.index.db.lookupActiveContractsDbMetrics.compressionTimer,
              deserializationTimer =
                metrics.daml.index.db.lookupActiveContractsDbMetrics.translationTimer,
            )
            ActiveContract(
              contract,
              raw.flatEventWitnesses,
              assertPresent(raw.ledgerEffectiveTime)(
                "ledger_effective_time must be present for a create event"
              ),
            )
          case raw if raw.eventKind == 20 => ArchivedContract(raw.flatEventWitnesses)
          case raw =>
            throw throw IndexErrors.DatabaseErrors.ResultSetError
              .Reject(s"Unexpected event kind ${raw.eventKind}")
              .asGrpcError
        }),
    )
  }

  /** Lookup of a contract in the case the contract value is not already known */
  override def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupDivulgedActiveContractDbMetrics)(
          storageBackend.activeContractWithArgument(readers, contractId)
        )
        .map(_.map { raw =>
          toContract(
            contractId = contractId,
            templateId = raw.templateId,
            createArgument = raw.createArgument,
            createArgumentCompression =
              Compression.Algorithm.assertLookup(raw.createArgumentCompression),
            decompressionTimer =
              metrics.daml.index.db.lookupDivulgedActiveContractDbMetrics.compressionTimer,
            deserializationTimer =
              metrics.daml.index.db.lookupDivulgedActiveContractDbMetrics.translationTimer,
          )
        }),
    )
  }

  /** Lookup of a contract in the case the contract value is already known (loaded from a cache) */
  override def lookupActiveContractWithCachedArgument(
      readers: Set[Party],
      contractId: ContractId,
      createArgument: Value,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupDivulgedActiveContractDbMetrics)(
          storageBackend.activeContractWithoutArgument(readers, contractId)
        )
        .map(
          _.map(templateId =>
            toContract(
              templateId = templateId,
              createArgument = createArgument,
            )
          )
        ),
    )
  }
}

private[dao] object ContractsReader {

  private[dao] def apply(
      contractLoader: ContractLoader,
      dispatcher: DbDispatcher,
      metrics: Metrics,
      storageBackend: ContractStorageBackend,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ContractsReader = {
    new ContractsReader(
      contractLoader = contractLoader,
      storageBackend = storageBackend,
      dispatcher = dispatcher,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )
  }

  // The contracts table _does not_ store agreement texts as they are
  // unnecessary for interpretation and validation. The contracts returned
  // from this table will _always_ have an empty agreement text.
  private def toContract(
      contractId: ContractId,
      templateId: String,
      createArgument: Array[Byte],
      createArgumentCompression: Compression.Algorithm,
      decompressionTimer: Timer,
      deserializationTimer: Timer,
  ): Contract = {
    val decompressed =
      Timed.value(
        timer = decompressionTimer,
        value = createArgumentCompression.decompress(new ByteArrayInputStream(createArgument)),
      )
    val deserialized =
      Timed.value(
        timer = deserializationTimer,
        value = ValueSerializer.deserializeValue(
          stream = decompressed,
          errorContext = s"Failed to deserialize create argument for contract ${contractId.coid}",
        ),
      )
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = deserialized,
    )
  }

  private def toContract(
      templateId: String,
      createArgument: Value,
  ): Contract =
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = createArgument,
    )

  private def assertPresent[T](in: Option[T])(err: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): T =
    in.getOrElse(throw IndexErrors.DatabaseErrors.ResultSetError.Reject(err).asGrpcError)
}
