// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.ledger.participant.state.index
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.ledger.participant.state.index.{
  ContractKeyPage,
  ContractState,
  ContractStateStatus,
  ContractStore,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.cache.ContractKeyStateValue.*
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStore.resolveFromCache
import com.digitalasset.canton.platform.store.interfaces.{
  KeyLookupPageResult,
  LedgerDaoContractsReader,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.concurrent.{ExecutionContext, Future, Promise}

private[platform] class MutableCacheBackedContractStore(
    contractsReader: LedgerDaoContractsReader,
    val loggerFactory: NamedLoggerFactory,
    private[cache] val contractStateCaches: ContractStateCaches,
    contractStore: LedgerApiContractStore,
    ledgerEndCache: LedgerEndCache,
    maxLookupLimit: Int,
)(implicit executionContext: ExecutionContext)
    extends ContractStore
    with NamedLogging {

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[FatContract]] =
    lookupContractState(contractId)
      .map(contractStateToFatContract(readers))

  override def lookupContractState(
      contractId: ContractId
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractState] =
    contractStateCaches.contractState
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))
      .flatMap {
        case ContractStateStatus.Active =>
          contractStore
            .lookupPersisted(contractId)
            .map {
              case Some(persistedContract) => ContractState.Active(persistedContract.inst)
              case None =>
                logger.info(
                  s"Contract $contractId marked as active in index (db or cache) but not found in participant's contract store"
                )
                ContractState.NotFound
            }
        case ContractStateStatus.Archived => Future.successful(ContractState.Archived)
        case ContractStateStatus.NotFound => Future.successful(ContractState.NotFound)
      }

  override def lookupContractKey(
      readers: Set[Ref.Party],
      key: GlobalKey,
      pageToken: Option[Long],
      limit: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractKeyPage] = {
    val cappedLimit = if (limit > maxLookupLimit) {
      logger.info(
        s"Lookup limit $limit exceeds configured cap of $maxLookupLimit, using cap instead"
      )
      maxLookupLimit
    } else limit

    contractStateCaches.keyState
      .get(key)
      .flatMap(resolveFromCache(_, pageToken))
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key, pageToken, cappedLimit))
      .flatMap { case (contractIds, nextPageToken) =>
        toContractKeyPage(contractIds, readers, nextPageToken)
      }
  }

  private def toContractKeyPage(
      contractIds: Vector[ContractId],
      readers: Set[Ref.Party],
      nextPageToken: Option[Long],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractKeyPage] =
    Future
      .sequence(contractIds.map(contractStore.lookupPersisted))
      .map { contracts =>
        ContractKeyPage(
          contracts = contracts.view.flatten.map(_.inst).filter(visibleFor(readers)).toVector,
          nextPageToken = nextPageToken,
        )
      }

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractStateStatus] =
    contractStateCaches.contractState
      .putAsync(
        contractId,
        contractsReader.lookupContractState(contractId, _).map(toContractCacheValue),
      )

  private def contractStateToFatContract(readers: Set[Party])(
      value: index.ContractState
  ): Option[FatContract] =
    value.toContractOption.filter(visibleFor(readers))

  private def visibleFor(readers: Set[Party])(
      contract: FatContract
  ): Boolean = contract.stakeholders.view.exists(readers)

  private val toContractCacheValue: Option[ExistingContractStatus] => ContractStateStatus =
    _.getOrElse(ContractStateStatus.NotFound)

  private def readThroughKeyCache(
      key: GlobalKey,
      pageToken: Option[Long],
      limit: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Vector[ContractId], Option[Long])] =
    if (pageToken.isEmpty) {
      // We capture the full DB result in a Promise because putAsync only returns the cache value which may be a subset
      // of the response.
      val fullResultPromise = Promise[(Vector[ContractId], Option[Long])]()
      contractStateCaches.keyState
        .putAsync(
          key,
          validAtEventSeqId =>
            contractsReader
              .lookupNonUniqueKey(
                key = key,
                notEarlierThanEventSeqId = validAtEventSeqId,
                nextPageToken = pageToken,
                limit = limit,
              )
              .map { case KeyLookupPageResult(contractIdsWithSeqIds, nextPageToken) =>
                fullResultPromise.success((contractIdsWithSeqIds.map(_.contractId), nextPageToken))
                contractIdsWithSeqIds.headOption.fold[ContractKeyStateValue](Empty) { contractRef =>
                  Last(
                    contractId = contractRef.contractId,
                    eventSequentialId = contractRef.eventSequentialId,
                    thereMightBeMore = contractIdsWithSeqIds.sizeIs > 1 || nextPageToken.isDefined,
                  )
                }
              },
        )
        .flatMap(_ => fullResultPromise.future)
    } else {
      // No read-through: just query DB
      val validAtEventSeqId = ledgerEndCache().map(_.lastEventSeqId).getOrElse(0L)
      contractsReader
        .lookupNonUniqueKey(
          key = key,
          notEarlierThanEventSeqId = validAtEventSeqId,
          nextPageToken = pageToken,
          limit = limit,
        )
        .map { page =>
          (page.contracts.map(_.contractId), page.nextPageToken)
        }
    }
}

private[platform] object MutableCacheBackedContractStore {
  type EventSequentialId = Long

  def resolveFromCache(
      value: ContractKeyStateValue,
      pageToken: Option[Long],
  ): Option[(Vector[ContractId], Option[Long])] =
    value match {
      // The cache knows the key has no contracts: return an empty page and no
      // continuation token, regardless of any incoming `pageToken`.
      case Empty => Some((Vector.empty, None))
      case Last(cid, seqId, thereMightBeMore) =>
        pageToken match {
          case None =>
            // First page request: return the single cached contract.
            // If more contracts may exist return a meaningful token, so that the caller can request the next page from the DB,
            // otherwise return no token.
            val nextPageToken = if (thereMightBeMore) Some(seqId) else None
            Some((Vector(cid), nextPageToken))
          case Some(token) =>
            if (seqId < token) {
              // The cached contract is older than the page boundary, return the single cached contract.
              // Same nextPageToken handling as for the first page.
              val nextPageToken = if (thereMightBeMore) Some(seqId) else None
              Some((Vector(cid), nextPageToken))
            } else {
              // The cached contract is newer than or at the (exclusive) page boundary.
              if (thereMightBeMore) {
                // The cache cannot tell whether older contracts exist for this key; fall through
                // to the DB.
                None
              } else {
                // The cache is knows that nothing older than the cached contract exists,
                // so the page returned is empty and there is no continuation.
                Some((Vector.empty, None))
              }
            }
        }
    }
}
