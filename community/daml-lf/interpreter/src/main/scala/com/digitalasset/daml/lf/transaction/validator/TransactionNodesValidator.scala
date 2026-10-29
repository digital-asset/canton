// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class TransactionNodesValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionValidator(metadata, inputContracts, limitConfig) {
  override def validate(
      transaction: SubmittedTransaction
  ): Option[Limit.Error] = {
    val transactionNodeSize = transaction.nodes.values.size

    limitConfig.metrics.foreach { metrics =>
      updateGauge(
        transactionNodeSize,
        limitConfig.limits.transactionNodes,
        metrics.transactionNodesGauge,
      )
    }

    if (transactionNodeSize > limitConfig.limits.transactionNodes) {
      Some(Limit.TransactionNodes(limitConfig.limits.transactionNodes))
    } else {
      None
    }
  }
}
