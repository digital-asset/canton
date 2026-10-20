// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{
  ExternalCallResult,
  FatContractInstance,
  Node,
  Transaction,
  TransactionOuterClass as proto,
}
import com.digitalasset.daml.lf.value.Value.ContractId

final case class ExternalCallResultSizeValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionNodeValidator(metadata, inputContracts, limitConfig) {
  import ExternalCallResultSizeValidator.*

  override def validateNode(node: Node): Option[Limit.Error] =
    node match {
      case node: Node.Exercise =>
        node.externalCallResults
          .map(extCallRes => (extCallRes -> externalCallResultSize(extCallRes)))
          .toList
          .maxByOption(_._2)
          .flatMap { case (largestExternalCallResult, largestExternalCallResultSize) =>
            limitConfig.metrics.foreach { metrics =>
              updateGauge(
                largestExternalCallResultSize,
                limitConfig.limits.externalCallResultSize,
                metrics.externalCallResultSizeGauge,
              )
            }
            if (largestExternalCallResultSize > limitConfig.limits.externalCallResultSize) {
              Some(
                Limit.ExternalCallResultSize(
                  node.targetCoid,
                  node.templateId,
                  node.choiceId,
                  node.chosenValue,
                  largestExternalCallResult,
                  limitConfig.limits.externalCallResultSize,
                )
              )
            } else {
              None
            }
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
