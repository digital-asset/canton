// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.KeysPageQuery
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class ContractsReader(
    contractLoader: ContractLoader,
    storageBackend: ContractStorageBackend,
    dispatcher: DbDispatcher,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader
    with NamedLogging {

  /** Batch lookup of contract keys directly from the database.
    *
    * Used to unit test the SQL queries for key lookups. Does not use the Pekko stream batch loader.
    */
  override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[Key, Long]] =
    Timed.future(
      metrics.index.db.lookupKey,
      dispatcher
        .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
          storageBackend.contractKeysPlain(
            keys.map(key =>
              KeysPageQuery(
                key = key,
                validAtEventSeqId = notEarlierThanEventSeqId,
                limit = 1,
                nextPageToken = None,
              )
            ),
            notEarlierThanEventSeqId,
          )
        )
        .map { results =>
          keys
            .zip(results)
            .flatMap { case (key, result) =>
              result.internalContractIds.headOption.map(key -> _)
            }
            .toMap
        },
    )

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key
    *   the contract key
    * @param notEarlierThanEventSeqId
    *   the lower bound offset of the ledger for which to query for the key state
    * @return
    *   the key state.
    */
  override def lookupKeyState(key: Key, notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState] =
    Timed.future(
      metrics.index.db.lookupKey,
      contractLoader.keys
        .load(
          KeysPageQuery(
            key = key,
            validAtEventSeqId = notEarlierThanEventSeqId,
            limit = 1,
            nextPageToken = None,
          )
        )
        .map {
          case Some((contractIds, _)) =>
            contractIds.headOption
              .map(KeyAssigned.apply)
              .getOrElse(KeyUnassigned)
          case None =>
            logger
              .error(
                s"Key $key resulted in an invalid empty load at offset $notEarlierThanEventSeqId"
              )(loggingContext.traceContext)
            KeyUnassigned
        },
    )

  override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ExistingContractStatus]] =
    Timed.future(
      metrics.index.db.lookupActiveContract,
      contractLoader.contracts.load(contractId -> notEarlierThanEventSeqId),
    )

  /** Looks up active contracts for a given key.
    *
    * Due to batching of several requests, we may return newer information than at the provided
    * offset, but never older information.
    *
    * @param key
    *   the contract key to query
    * @param notEarlierThanEventSeqId
    *   the offset threshold to resolve the key state (state can be newer, but not older)
    * @param nextPageToken
    *   pagination token for fetching subsequent pages
    * @param limit
    *   maximum number of contract IDs to return
    * @return
    *   a vector of active contract IDs and an optional next page token
    */
  override def lookupNonUniqueKey(
      key: Key,
      notEarlierThanEventSeqId: Long,
      nextPageToken: Option[Long],
      limit: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Vector[ContractId], Option[Long])] =
    Timed.future(
      metrics.index.db.lookupNonUniqueKey,
      contractLoader.keys
        .load(
          KeysPageQuery(
            key = key,
            limit = limit,
            nextPageToken = nextPageToken,
            validAtEventSeqId = notEarlierThanEventSeqId,
          )
        )
        .map {
          case Some(result) => result
          case None =>
            logger.error(
              s"Non-unique key lookup for $key resulted in an invalid empty load"
            )(loggingContext.traceContext)
            (Vector.empty, None)
        },
    )
}

private[dao] object ContractsReader {

  private[dao] def apply(
      contractLoader: ContractLoader,
      dispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      storageBackend: ContractStorageBackend,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ContractsReader =
    new ContractsReader(
      contractLoader = contractLoader,
      storageBackend = storageBackend,
      dispatcher = dispatcher,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )

}
