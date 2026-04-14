// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId

/** Errors raised when building transactions with PartialTransaction:
 *   - [[TransactionError.DuplicateContractId]]
 */
sealed trait TransactionError extends Serializable with Product

sealed trait TransactionContractError extends TransactionError

/** Defines the errors raised by [[LegacyContractStateMachine]] and its clients:
  *  - [[TransactionError.DuplicateContractId]]
  *  - [[TransactionError.InconsistentContractKey]]
  */
object TransactionError {

  final case class AlreadyConsumed(
      cid: ContractId,
      tmplId: Ref.TypeConId,
      nid: NodeId
  ) extends TransactionContractError

  /** Signals that the transaction tried to create two contracts with the same
    * contract ID or tried to create a contract whose contract ID has been
    * previously successfully fetched.
    */
  final case class DuplicateContractId(
      contractId: ContractId
  ) extends TransactionContractError

  /** An exercise, fetch or lookupByKey failed because the mapping of key -> contract id
    * was inconsistent with earlier nodes (in execution order). This can happened in case
    * of a race condition between the contract and the contract keys queried to the ledger
    * during an interpretation.
    */
  final case class InconsistentContractKey(key: GlobalKey) extends TransactionContractError

  /** Signals that a rollback scope containing effectful nodes (e.g., creates or exercises)
    * was encountered in a context where rollback is not supported.
    */
  final case class EffectfulRollback(
      nodeIds: Set[NodeId],
  ) extends TransactionError
}
