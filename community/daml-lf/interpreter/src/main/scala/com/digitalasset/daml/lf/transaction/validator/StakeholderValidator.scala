// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class StakeholderValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Create =>
        val stakeholdersSize = node.stakeholders.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            stakeholdersSize,
            limitConfig.limits.contractStakeholders,
            metrics.stakeholdersGauge,
          )
        }
        if (stakeholdersSize > limitConfig.limits.contractStakeholders) {
          Some(
            Limit.ContractStakeholders(
              node.coid,
              node.templateId,
              node.stakeholders,
              limitConfig.limits.contractStakeholders,
            )
          )
        } else {
          None
        }

      case node: Node.Fetch =>
        val stakeholdersSize = node.stakeholders.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            stakeholdersSize,
            limitConfig.limits.contractStakeholders,
            metrics.stakeholdersGauge,
          )
        }
        if (stakeholdersSize > limitConfig.limits.contractStakeholders) {
          Some(
            Limit.ContractStakeholders(
              node.coid,
              node.templateId,
              node.stakeholders,
              limitConfig.limits.contractStakeholders,
            )
          )
        } else {
          None
        }

      case node: Node.Exercise =>
        val stakeholdersSize = node.stakeholders.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            stakeholdersSize,
            limitConfig.limits.contractStakeholders,
            metrics.stakeholdersGauge,
          )
        }
        if (stakeholdersSize > limitConfig.limits.contractStakeholders) {
          Some(
            Limit.ContractStakeholders(
              node.targetCoid,
              node.templateId,
              node.stakeholders,
              limitConfig.limits.contractStakeholders,
            )
          )
        } else {
          None
        }

      case _ =>
        None
    }
}
