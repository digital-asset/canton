// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interfaces

import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.Future

private[platform] trait LedgerDaoContractsReader {

  /** Looks up the contract by id
    *
    * Due to batching of several requests, we may return newer information than at the provided
    * offset, but never older information.
    *
    * @param contractId
    *   the contract id to query
    * @param notEarlierThanEventSeqId
    *   the offset threshold to resolve the contract state (state can be newer, but not older)
    * @return
    *   the optional boolean flag indicating whether the contract is active (true) or archived
    *   (false). None if the contract is not found.
    */
  def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ExistingContractStatus]]

  /** Looks up the state of a contract key
    *
    * Due to batching of several requests, we may return newer information than at the provided
    * offset, but never older information.
    *
    * @param key
    *   the contract key to query
    * @param notEarlierThanEventSeqId
    *   the offset threshold to resolve the key state (state can be newer, but not older)
    * @return
    *   the [[KeyState]]
    */
  def lookupKeyState(key: GlobalKey, notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState]

  /** Batch lookup of contract keys
    *
    * Used to unit test the SQL queries for key lookups. Does not use batching.
    */
  @VisibleForTesting
  def lookupKeyStatesFromDb(keys: Seq[GlobalKey], notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[GlobalKey, Long]]

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
  def lookupNonUniqueKey(
      key: GlobalKey,
      notEarlierThanEventSeqId: Long,
      nextPageToken: Option[Long],
      limit: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[(Vector[ContractId], Option[Long])]

}

object LedgerDaoContractsReader {
  import com.digitalasset.daml.lf.value.Value as lfval
  private type ContractId = lfval.ContractId

  sealed trait KeyState extends Product with Serializable

  final case class KeyAssigned(contractId: ContractId) extends KeyState

  final case object KeyUnassigned extends KeyState
}
