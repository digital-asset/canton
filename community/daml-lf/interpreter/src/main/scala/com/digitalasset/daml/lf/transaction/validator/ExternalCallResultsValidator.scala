// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class ExternalCallResultsValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Exercise =>
        val externalCallResultsSize = node.externalCallResults.length
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            externalCallResultsSize,
            limitConfig.limits.externalCallResults,
            metrics.externalCallResultsGauge,
          )
        }
        if (externalCallResultsSize > limitConfig.limits.externalCallResults) {
          Some(
            Limit.ExternalCallResults(
              node.targetCoid,
              node.templateId,
              node.choiceId,
              node.chosenValue,
              node.externalCallResults,
              limitConfig.limits.externalCallResults,
            )
          )
        } else {
          None
        }

      case _ =>
        None
    }
}
