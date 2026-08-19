// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction
package validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.annotation.unused

/*
 * Generic transaction validator
 */
abstract class TransactionValidator(
    @unused metadata: Transaction.Metadata,
    @unused inputContracts: Map[ContractId, FatContractInstance],
    @unused limits: Limits,
) {
  def validate(transaction: SubmittedTransaction): Option[Limit.Error]
}

/*
 * Validate that the transaction only contains valid nodes
 */
abstract class TransactionNodeValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionValidator(metadata, inputContracts, limits) {
  def validateNode(node: Node): Option[Limit.Error]

  override def validate(transaction: SubmittedTransaction): Option[Limit.Error] =
    transaction.nodes.values.collectFirst {
      case node if validateNode(node).isDefined =>
        validateNode(node).get
    }
}

/*
 * Validate that any transaction node, in a matching context, is valid
 */
abstract class TransactionContextValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionNodeValidator(metadata, inputContracts, limits) {
  def matchingContext(node: Node): Boolean

  def validateNode(node: Node): Option[Limit.Error]

  override def validate(transaction: SubmittedTransaction): Option[Limit.Error] =
    transaction.nodes.values.collectFirst {
      case node if matchingContext(node) && validateNode(node).isDefined =>
        validateNode(node).get
    }
}

object TransactionValidator {
  type TransactionValidatorF = (
      Transaction.Metadata,
      Map[ContractId, FatContractInstance],
      Limits,
  ) => TransactionValidator

  private val defaultEnabledValidators: List[TransactionValidatorF] = List(
    SignatoryValidator.apply,
    StakeholderValidator.apply,
    KeyMaintainersValidator.apply,
    ValueValidator.apply,
    ActingPartiesValidator.apply,
    ChoiceObserversValidator.apply,
    ChoiceAuthorizersValidator.apply,
    ExternalCallResultsValidator.apply,
    ExternalCallResultSizeValidator.apply,
    NodeChildrenValidator.apply,
    TotalInformeeValidator.apply,
    TransactionRootsValidator.apply,
    TransactionNodesValidator.apply,
    InputContractValidator.apply,
  )

  def validate(
      transaction: SubmittedTransaction,
      metadata: Transaction.Metadata,
      inputContracts: Map[ContractId, FatContractInstance],
      limits: Limits,
  )(implicit
      enabledValidators: List[TransactionValidatorF] = defaultEnabledValidators
  ): Set[Limit.Error] =
    enabledValidators.foldLeft(Set.empty[Limit.Error]) { case (results, validator) =>
      validator(metadata, inputContracts, limits).validate(transaction).toSet ++ results
    }
}
