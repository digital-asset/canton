// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  Node,
  SubmittedTransaction,
  Transaction,
}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class TotalInformeeValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionValidator(metadata, inputContracts, limitConfig) {
  override def validate(
      transaction: SubmittedTransaction
  ): Option[Limit.Error] = {
    val totalInformees = transaction.nodes.values.collect { case node: Node.Action =>
      node.informeesOfNode.size
    }.sum

    limitConfig.metrics.foreach { metrics =>
      updateGauge(
        totalInformees.toLong,
        limitConfig.limits.totalInformees,
        metrics.totalInformeesGauge,
      )
    }

    if (totalInformees > limitConfig.limits.totalInformees) {
      Some(Limit.TotalInformees(limitConfig.limits.totalInformees))
    } else {
      None
    }
  }
}
