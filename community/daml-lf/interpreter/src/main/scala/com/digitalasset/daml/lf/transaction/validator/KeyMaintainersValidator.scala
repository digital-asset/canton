// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class KeyMaintainersValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Create =>
        val keyMaintainersSize = node.keyOpt.fold(0)(_.maintainers.size)
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            keyMaintainersSize,
            limitConfig.limits.keyMaintainers,
            metrics.keyMaintainersGauge,
          )
        }
        if (keyMaintainersSize > limitConfig.limits.keyMaintainers) {
          node.keyOpt.map { key =>
            Limit.KeyMaintainers(
              node.coid,
              node.templateId,
              key.maintainers,
              limitConfig.limits.keyMaintainers,
            )
          }
        } else {
          None
        }

      case node: Node.Fetch =>
        val keyMaintainersSize = node.keyOpt.fold(0)(_.maintainers.size)
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            keyMaintainersSize,
            limitConfig.limits.keyMaintainers,
            metrics.keyMaintainersGauge,
          )
        }
        if (keyMaintainersSize > limitConfig.limits.keyMaintainers) {
          node.keyOpt.map { key =>
            Limit.KeyMaintainers(
              node.coid,
              node.templateId,
              key.maintainers,
              limitConfig.limits.keyMaintainers,
            )
          }
        } else {
          None
        }

      case node: Node.Exercise =>
        val keyMaintainersSize = node.keyOpt.fold(0)(_.maintainers.size)
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            keyMaintainersSize,
            limitConfig.limits.keyMaintainers,
            metrics.keyMaintainersGauge,
          )
        }
        if (keyMaintainersSize > limitConfig.limits.keyMaintainers) {
          node.keyOpt.map { key =>
            Limit.KeyMaintainers(
              node.targetCoid,
              node.templateId,
              key.maintainers,
              limitConfig.limits.keyMaintainers,
            )
          }
        } else {
          None
        }

      case node: Node.QueryByKey =>
        val keyMaintainersSize = node.keyOpt.fold(0)(_.maintainers.size)
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            keyMaintainersSize,
            limitConfig.limits.keyMaintainers,
            metrics.keyMaintainersGauge,
          )
        }
        if (keyMaintainersSize > limitConfig.limits.keyMaintainers) {
          Some(
            Limit.KeyMaintainers(
              node.packageName,
              node.templateId,
              node.key.maintainers,
              limitConfig.limits.keyMaintainers,
            )
          )
        } else {
          None
        }

      case _ =>
        None
    }
}
