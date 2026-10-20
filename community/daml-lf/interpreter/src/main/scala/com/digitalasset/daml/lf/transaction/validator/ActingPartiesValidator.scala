// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class ActingPartiesValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Fetch =>
        val actingPartiesSize = node.actingParties.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            actingPartiesSize,
            limitConfig.limits.actingParties,
            metrics.actingPartiesGauge,
          )
        }
        if (actingPartiesSize > limitConfig.limits.actingParties) {
          Some(
            Limit.ActingParties(
              node.coid,
              node.templateId,
              node.actingParties,
              limitConfig.limits.actingParties,
            )
          )
        } else {
          None
        }
      case node: Node.Exercise =>
        val actingPartiesSize = node.actingParties.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            node.actingParties.size,
            limitConfig.limits.actingParties,
            metrics.actingPartiesGauge,
          )
        }
        if (actingPartiesSize > limitConfig.limits.actingParties) {
          Some(
            Limit.ActingParties(
              node.targetCoid,
              node.templateId,
              node.actingParties,
              limitConfig.limits.actingParties,
            )
          )
        } else {
          None
        }
      case _ =>
        None
    }
}
