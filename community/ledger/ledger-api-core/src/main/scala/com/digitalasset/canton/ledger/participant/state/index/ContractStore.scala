// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.protocol.LfFatContractInst
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.Future

/** Meant be used for optimistic contract lookups before command submission.
  */
trait ContractStore {

  /** Looking up an active contract.
    */
  def lookupActiveContract(
      readers: Set[Ref.Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[LfFatContractInst]]

  def lookupContractKey(readers: Set[Ref.Party], key: GlobalKey)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractId]]

  def lookupNonUniqueContractKey(
      readers: Set[Ref.Party],
      key: GlobalKey,
      pageToken: Option[Long],
      limit: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractKeyPage]

  /** Querying the state of the contracts.
    */
  def lookupContractState(
      contractId: ContractId
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractState]
}

final case class ContractKeyPage(
    contracts: Vector[LfFatContractInst],
    nextPageToken: Option[Long],
)

sealed trait ContractState {
  def toContractOption: Option[LfFatContractInst] = None
}

sealed trait ContractStateStatus extends Product with Serializable {
  def isActive: Boolean = this match {
    case ContractStateStatus.Active => true
    case _ => false
  }

  def isArchived: Option[Boolean] = this match {
    case ContractStateStatus.Archived => Some(true)
    case ContractStateStatus.Active => Some(false)
    case ContractStateStatus.NotFound => None
  }
}
object ContractStateStatus {
  case object NotFound extends ContractStateStatus
  sealed trait ExistingContractStatus extends ContractStateStatus
  case object Archived extends ExistingContractStatus
  case object Active extends ExistingContractStatus
}

object ContractState {
  case object NotFound extends ContractState
  case object Archived extends ContractState
  final case class Active(contractInstance: LfFatContractInst) extends ContractState {
    override def toContractOption: Option[LfFatContractInst] = Some(contractInstance)
  }
}
