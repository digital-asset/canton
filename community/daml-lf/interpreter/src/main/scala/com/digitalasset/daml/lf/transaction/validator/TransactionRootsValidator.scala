// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class TransactionRootsValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionValidator(metadata, inputContracts, limitConfig) {
  override def validate(
      transaction: SubmittedTransaction
  ): Option[Limit.Error] = {
    val transactionRootsSize = transaction.roots.length

    limitConfig.metrics.foreach { metrics =>
      updateGauge(
        transactionRootsSize,
        limitConfig.limits.transactionRoots,
        metrics.transactionRootsGauge,
      )
    }

    if (transactionRootsSize > limitConfig.limits.transactionRoots) {
      Some(Limit.TransactionRoots(limitConfig.limits.transactionRoots))
    } else {
      None
    }
  }
}
