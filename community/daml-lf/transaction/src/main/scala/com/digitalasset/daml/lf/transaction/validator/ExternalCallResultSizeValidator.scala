// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction
package validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.transaction.TransactionOuterClass as proto
import com.digitalasset.daml.lf.value.Value.ContractId

final case class ExternalCallResultSizeValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionNodeValidator(metadata, inputContracts, limits) {
  import ExternalCallResultSizeValidator.*

  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Exercise =>
        node.externalCallResults
          .find {
            externalCallResultSize(_) > limits.externalCallResultSize
          }
          .map { externalCallResult =>
            Limit.ExternalCallResultSize(
              node.targetCoid,
              node.templateId,
              node.choiceId,
              node.chosenValue,
              externalCallResult,
              limits.externalCallResultSize,
            )
          }

      case _ =>
        None
    }
}

object ExternalCallResultSizeValidator {
  private[validator] def externalCallResultSize(externalCallResult: ExternalCallResult): Int =
    proto.ExternalCallResult
      .newBuilder()
      .setExtensionId(externalCallResult.extensionId)
      .setFunctionId(externalCallResult.functionId)
      .setConfig(externalCallResult.config.toByteString)
      .setInput(externalCallResult.input.toByteString)
      .setOutput(externalCallResult.output.toByteString)
      .build()
      .toByteString
      .size()
}
