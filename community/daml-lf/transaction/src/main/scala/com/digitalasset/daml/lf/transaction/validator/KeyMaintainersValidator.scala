// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction
package validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.value.Value.ContractId

final case class KeyMaintainersValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionNodeValidator(metadata, inputContracts, limits) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Create if (node.keyOpt.fold(0)(_.maintainers.size) > limits.keyMaintainers) =>
        node.keyOpt.map { key =>
          Limit.KeyMaintainers(
            node.coid,
            node.templateId,
            key.maintainers,
            limits.keyMaintainers,
          )
        }
      case node: Node.Fetch if (node.keyOpt.fold(0)(_.maintainers.size) > limits.keyMaintainers) =>
        node.keyOpt.map { key =>
          Limit.KeyMaintainers(
            node.coid,
            node.templateId,
            key.maintainers,
            limits.keyMaintainers,
          )
        }
      case node: Node.Exercise
          if (node.keyOpt.fold(0)(_.maintainers.size) > limits.keyMaintainers) =>
        node.keyOpt.map { key =>
          Limit.KeyMaintainers(
            node.targetCoid,
            node.templateId,
            key.maintainers,
            limits.keyMaintainers,
          )
        }
      case node: Node.QueryByKey if (node.key.maintainers.size > limits.keyMaintainers) =>
        Some(
          Limit.KeyMaintainers(
            node.packageName,
            node.templateId,
            node.key.maintainers,
            limits.keyMaintainers,
          )
        )
      case _ =>
        None
    }
}
