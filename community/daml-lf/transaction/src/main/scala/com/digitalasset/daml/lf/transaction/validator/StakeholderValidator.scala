// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction
package validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.value.Value.ContractId

final case class StakeholderValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionNodeValidator(metadata, inputContracts, limits) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Create if (node.stakeholders.size > limits.contractStakeholders) =>
        Some(
          Limit.ContractStakeholders(
            node.coid,
            node.templateId,
            node.stakeholders,
            limits.contractStakeholders,
          )
        )
      case node: Node.Fetch if (node.stakeholders.size > limits.contractStakeholders) =>
        Some(
          Limit.ContractStakeholders(
            node.coid,
            node.templateId,
            node.stakeholders,
            limits.contractStakeholders,
          )
        )
      case node: Node.Exercise if (node.stakeholders.size > limits.contractStakeholders) =>
        Some(
          Limit.ContractStakeholders(
            node.targetCoid,
            node.templateId,
            node.stakeholders,
            limits.contractStakeholders,
          )
        )
      case _ =>
        None
    }
}
