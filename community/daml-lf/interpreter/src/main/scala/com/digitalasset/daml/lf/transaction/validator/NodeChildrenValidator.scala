// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class NodeChildrenValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Exercise =>
        val childSize = node.children.length
        limitConfig.metrics.foreach { metrics =>
          updateGauge(childSize, limitConfig.limits.nodeChildren, metrics.nodeChildrenGauge)
        }
        if (childSize > limitConfig.limits.nodeChildren) {
          Some(
            Limit.NodeChildren(
              node.targetCoid,
              node.templateId,
              node.choiceId,
              node.chosenValue,
              limitConfig.limits.nodeChildren,
            )
          )
        } else {
          None
        }

      case _ =>
        None
    }
}
