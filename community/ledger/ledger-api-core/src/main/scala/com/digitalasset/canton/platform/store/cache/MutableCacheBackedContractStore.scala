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
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.concurrent.{ExecutionContext, Future}

private[platform] class MutableCacheBackedContractStore(
    contractsReader: LedgerDaoContractsReader,
    val loggerFactory: NamedLoggerFactory,
    private[cache] val contractStateCaches: ContractStateCaches,
    contractStore: LedgerApiContractStore,
    ledgerEndCache: LedgerEndCache,
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
                logger.error(
                  s"Contract $contractId marked as active in index (db or cache) but not found in participant's contract store"
                )
                ContractState.NotFound
            }
        case ContractStateStatus.Archived => Future.successful(ContractState.Archived)
        case ContractStateStatus.NotFound => Future.successful(ContractState.NotFound)
      }

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractId]] =
    contractStateCaches.keyState
      .get(key)
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key))
      .flatMap(keyStateToResponse(_, readers))

  override def lookupNonUniqueContractKey(
      readers: Set[Ref.Party],
      key: GlobalKey,
      pageToken: Option[Long],
      limit: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractKeyPage] =
    for {
      keyPageResult <- contractsReader.lookupNonUniqueKey(
        key = key,
        validAtEventSeqId = ledgerEndCache().map(_.lastEventSeqId).getOrElse(0L),
        nextPageToken = pageToken,
        limit = limit,
      )
      contractIdLookup <- contractStore.lookupBatchedContractIdsNonReadThrough(
        keyPageResult.internalContractIds
      )
      contractIds = keyPageResult.internalContractIds.flatMap(contractIdLookup.get)
      contracts <- Future.sequence(contractIds.map(contractStore.lookupPersisted))
      filteredContracts = contracts.view.flatten.map(_.inst).filter(visibleFor(readers)).toVector
    } yield ContractKeyPage(
      contracts = filteredContracts,
      nextPageToken = keyPageResult.nextPageToken,
    )

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractStateStatus] =
    contractStateCaches.contractState
      .putAsync(
        contractId,
        contractsReader.lookupContractState(contractId, _).map(toContractCacheValue),
      )

  private def keyStateToResponse(
      value: ContractKeyStateValue,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[ContractId]] = value match {
    case Assigned(contractId) =>
      lookupContractState(contractId).map(
        contractStateToFatContract(readers)(_).map(_.contractId)
      )

    case _: Assigned | Unassigned => Future.successful(None)
  }

  private def contractStateToFatContract(readers: Set[Party])(
      value: index.ContractState
  ): Option[FatContract] =
    value.toContractOption.filter(visibleFor(readers))

  private def visibleFor(readers: Set[Party])(
      contract: FatContract
  ): Boolean = contract.stakeholders.view.exists(readers)

  private val toContractCacheValue: Option[ExistingContractStatus] => ContractStateStatus =
    _.getOrElse(ContractStateStatus.NotFound)

  private val toKeyCacheValue: KeyState => ContractKeyStateValue = {
    case LedgerDaoContractsReader.KeyAssigned(contractId) =>
      Assigned(contractId)
    case LedgerDaoContractsReader.KeyUnassigned =>
      Unassigned
  }

  private def readThroughKeyCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractKeyStateValue] =
    // Even if we have a contract id, we do not automatically trigger the loading of the contract here.
    // For prefetching at the start of command interpretation, this is done explicitly there.
    // For contract key lookups during interpretation, there is no benefit in doing so,
    // because contract prefetching blocks in the current architecture.
    // So when Daml engine does not need the contract after all, we avoid loading it.
    // Conversely, when Daml engine does need the contract, then this will trigger the loading of the contract
    // with the same parallelization and batching opportunities.
    contractStateCaches.keyState
      .putAsync(
        key,
        contractsReader.lookupKeyState(key, _).map(toKeyCacheValue),
      )
}

private[platform] object MutableCacheBackedContractStore {
  type EventSequentialId = Long
}
