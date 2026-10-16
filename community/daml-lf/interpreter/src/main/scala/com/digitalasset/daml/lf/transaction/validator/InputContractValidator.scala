// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class InputContractValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionValidator(metadata, inputContracts, limitConfig) {
  override def validate(
      transaction: SubmittedTransaction
  ): Option[Limit.Error] = {
    val inputContractsSize = inputContracts.size
    limitConfig.metrics.foreach { metrics =>
      updateGauge(
        inputContractsSize,
        limitConfig.limits.transactionInputContracts,
        metrics.inputContractsGauge,
      )
    }

    if (inputContractsSize > limitConfig.limits.transactionInputContracts) {
      Some(Limit.TransactionInputContracts(limitConfig.limits.transactionInputContracts))
    } else {
      None
    }
  }
}
