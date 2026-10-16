// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.MetricsContext
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  Node,
  SubmittedTransaction,
  Transaction,
}
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.annotation.unused

/*
 * Generic transaction validator
 */
abstract class TransactionValidator(
    @unused metadata: Transaction.Metadata,
    @unused inputContracts: Map[ContractId, FatContractInstance],
    @unused limitConfig: LimitConfig,
) {
  def validate(transaction: SubmittedTransaction): Option[Limit.Error]

  final def updateGauge[T](value: T, limit: T, gauge: Gauge[T]): Unit =
    MetricsContext.withMetricLabels(
      "limit" -> s"$limit"
    ) { implicit mc =>
      gauge.updateValue(value)
    }
}

/*
 * Validate that the transaction only contains valid nodes
 */
abstract class TransactionNodeValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionValidator(metadata, inputContracts, limitConfig) {
  def validateNode(node: Node): Option[Limit.Error]

  override def validate(
      transaction: SubmittedTransaction
  ): Option[Limit.Error] =
    transaction.nodes.values.map(validateNode).collectFirst { case Some(error) =>
      error
    }
}

object TransactionValidator {
  type TransactionValidatorF = (
      Transaction.Metadata,
      Map[ContractId, FatContractInstance],
      LimitConfig,
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
      limitConfig: LimitConfig,
  ): Set[Limit.Error] =
    defaultEnabledValidators.foldLeft(Set.empty[Limit.Error]) { case (results, validator) =>
      validator(metadata, inputContracts, limitConfig).validate(transaction).toSet ++ results
    }
}
