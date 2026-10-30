// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class ChoiceAuthorizersValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Exercise =>
        val requiredAuthorizersSize = node.requiredAuthorizers.size
        limitConfig.metrics.foreach { metrics =>
          updateGauge(
            requiredAuthorizersSize,
            limitConfig.limits.choiceAuthorizers,
            metrics.choiceAuthorizersGauge,
          )
        }
        if (requiredAuthorizersSize > limitConfig.limits.choiceAuthorizers) {
          Some(
            Limit.ChoiceAuthorizers(
              node.targetCoid,
              node.templateId,
              node.choiceId,
              node.chosenValue,
              node.requiredAuthorizers,
              limitConfig.limits.choiceAuthorizers,
            )
          )
        } else {
          None
        }

      case _ =>
        None
    }
}
