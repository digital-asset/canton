// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction
package validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.value.Value.ContractId

final case class InputContractValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionValidator(metadata, inputContracts, limits) {
  override def validate(transaction: SubmittedTransaction): Option[Limit.Error] =
    if (inputContracts.size > limits.transactionInputContracts) {
      Some(Limit.TransactionInputContracts(limits.transactionInputContracts))
    } else {
      None
    }
}
