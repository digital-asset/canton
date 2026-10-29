// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class SignatoryValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Create =>
        val signatoriesSize = node.signatories.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            signatoriesSize,
            limitConfig.limits.contractSignatories,
            metrics.signatoriesGauge,
          )
        }
        if (signatoriesSize > limitConfig.limits.contractSignatories) {
          Some(
            Limit.ContractSignatories(
              node.coid,
              node.templateId,
              node.signatories,
              limitConfig.limits.contractSignatories,
            )
          )
        } else {
          None
        }

      case node: Node.Fetch =>
        val signatoriesSize = node.signatories.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            signatoriesSize,
            limitConfig.limits.contractSignatories,
            metrics.signatoriesGauge,
          )
        }
        if (signatoriesSize > limitConfig.limits.contractSignatories) {
          Some(
            Limit.ContractSignatories(
              node.coid,
              node.templateId,
              node.signatories,
              limitConfig.limits.contractSignatories,
            )
          )
        } else {
          None
        }

      case node: Node.Exercise =>
        val signatoriesSize = node.signatories.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            signatoriesSize,
            limitConfig.limits.contractSignatories,
            metrics.signatoriesGauge,
          )
        }
        if (signatoriesSize > limitConfig.limits.contractSignatories) {
          Some(
            Limit.ContractSignatories(
              node.targetCoid,
              node.templateId,
              node.signatories,
              limitConfig.limits.contractSignatories,
            )
          )
        } else {
          None
        }

      case _ =>
        None
    }
}
