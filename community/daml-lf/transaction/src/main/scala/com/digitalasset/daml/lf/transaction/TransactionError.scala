// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.value.Value.ContractId

/** Errors raised when building transactions with PartialTransaction:
 *   - [[TransactionError.DuplicateContractId]]
 *   - [[TransactionError.DuplicateContractKey]]
 */
sealed trait TransactionError extends Serializable with Product

sealed trait TransactionContractError extends TransactionError

/** Defines the errors raised by [[LegacyContractStateMachine]] and its clients:
  *  - [[TransactionError.DuplicateContractId]]
  *  - [[TransactionError.DuplicateContractKey]]
  *  - [[TransactionError.InconsistentContractKey]]
  */
object TransactionError {

  final case class AlreadyConsumed(
      cid: ContractId,
      nid: NodeId
  ) extends TransactionContractError

  /** Signals that the transaction tried to create two contracts with the same
    * contract ID or tried to create a contract whose contract ID has been
    * previously successfully fetched.
    */
  final case class DuplicateContractId(
      contractId: ContractId
  ) extends TransactionContractError


  /** Signals that within the transaction we got to a point where
    * two contracts with the same key were active.
    *
    * Note that speedy only detects duplicate key collisions
    * if both contracts are used in the transaction in by-key operations
    * meaning lookup, fetch or exercise-by-key or local creates.
    *
    * Two notable cases that will never produce duplicate key errors
    * is a standalone create or a create and a fetch (but not fetch-by-key)
    * with the same key.
    *
    * For ledger implementors this means that (for contract key uniqueness)
    * it is sufficient to only look at the inputs and the outputs of the
    * transaction while leaving all internal checks within the transaction
    * to the engine.
    */
  final case class DuplicateContractKey(
      key: GlobalKey
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
