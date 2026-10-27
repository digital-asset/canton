// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.LimitConfig
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  Node,
  SerializationVersion,
  SubmittedTransaction,
  Transaction,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{Value, ValueCoder}

final case class ValueValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limitConfig: LimitConfig,
) extends TransactionValidator(metadata, inputContracts, limitConfig) {
  import ValueValidator.*

  override def validate(
      transaction: SubmittedTransaction
  ): Option[Limit.Error] = {
    implicit val version: SerializationVersion = transaction.version

    def validateNode(node: Node): Option[Limit.Error] =
      node match {
        case node: Node.Create =>
          val argSize = valueSize(node.arg)
          val keySize = node.keyOpt.map(k => valueSize(k.globalKey.key)).getOrElse(0)

          limitConfig.metrics.foreach { metrics =>
            updateGauge(argSize.max(keySize), limitConfig.limits.valueSize, metrics.lfValueGauge)
          }

          if (argSize > limitConfig.limits.valueSize) {
            Some(
              Limit.ValueSize(
                node.coid,
                node.templateId,
                node.arg,
                limitConfig.limits.valueSize,
              )
            )
          } else if (keySize > limitConfig.limits.valueSize) {
            node.keyOpt.map { key =>
              Limit.ValueSize(
                node.coid,
                node.templateId,
                key.globalKey.key,
                limitConfig.limits.valueSize,
              )
            }
          } else {
            None
          }

        case node: Node.Fetch =>
          val keySize = node.keyOpt.map(k => valueSize(k.globalKey.key)).getOrElse(0)

          limitConfig.metrics.foreach { metrics =>
            updateGauge(keySize, limitConfig.limits.valueSize, metrics.lfValueGauge)
          }

          if (keySize > limitConfig.limits.valueSize) {
            node.keyOpt.map { key =>
              Limit.ValueSize(
                node.coid,
                node.templateId,
                key.globalKey.key,
                limitConfig.limits.valueSize,
              )
            }
          } else {
            None
          }

        case node: Node.Exercise =>
          val chosenValueSize = valueSize(node.chosenValue)
          val exerciseResultSize = node.exerciseResult.map(valueSize(_)).getOrElse(0)
          val keySize = node.keyOpt.map(k => valueSize(k.globalKey.key)).getOrElse(0)

          limitConfig.metrics.foreach { metrics =>
            updateGauge(
              Seq(chosenValueSize, exerciseResultSize, keySize).max,
              limitConfig.limits.valueSize,
              metrics.lfValueGauge,
            )
          }

          if (chosenValueSize > limitConfig.limits.valueSize) {
            Some(
              Limit.ValueSize(
                node.targetCoid,
                node.templateId,
                node.chosenValue,
                limitConfig.limits.valueSize,
              )
            )
          } else if (exerciseResultSize > limitConfig.limits.valueSize) {
            node.exerciseResult.map { result =>
              Limit.ValueSize(
                node.targetCoid,
                node.templateId,
                result,
                limitConfig.limits.valueSize,
              )
            }
          } else if (keySize > limitConfig.limits.valueSize) {
            node.keyOpt.map { key =>
              Limit.ValueSize(
                node.targetCoid,
                node.templateId,
                key.globalKey.key,
                limitConfig.limits.valueSize,
              )
            }
          } else {
            None
          }

        case node: Node.QueryByKey =>
          val keySize = valueSize(node.key.globalKey.key)

          limitConfig.metrics.foreach { metrics =>
            updateGauge(keySize, limitConfig.limits.valueSize, metrics.lfValueGauge)
          }

          if (keySize > limitConfig.limits.valueSize) {
            Some(
              Limit.ValueSize(
                node.packageName,
                node.templateId,
                node.key.globalKey.key,
                limitConfig.limits.valueSize,
              )
            )
          } else {
            None
          }

        case _ =>
          None
      }

    def validateInputContract(contract: FatContractInstance): Option[Limit.Error] = {
      val createArgSize = valueSize(contract.createArg)
      val keySize =
        contract.contractKeyWithMaintainers.map(k => valueSize(k.globalKey.key)).getOrElse(0)

      limitConfig.metrics.foreach { metrics =>
        updateGauge(createArgSize.max(keySize), limitConfig.limits.valueSize, metrics.lfValueGauge)
      }

      if (createArgSize > limitConfig.limits.valueSize) {
        Some(
          Limit.ValueSize(
            contract.contractId,
            contract.templateId,
            contract.createArg,
            limitConfig.limits.valueSize,
          )
        )
      } else if (keySize > limitConfig.limits.valueSize) {
        contract.contractKeyWithMaintainers.map { key =>
          Limit.ValueSize(
            contract.contractId,
            contract.templateId,
            key.globalKey.key,
            limitConfig.limits.valueSize,
          )
        }
      } else {
        None
      }
    }

    inputContracts.values
      .collectFirst {
        case inst if validateInputContract(inst).isDefined => validateInputContract(inst)
      }
      .orElse {
        transaction.nodes.values.collectFirst {
          case node if validateNode(node).isDefined => validateNode(node)
        }
      }
      .flatten
  }
}

object ValueValidator {
  // The following ignores contract ID suffixes for created non-global contracts
  // Values that can not be encoded into bytes have size 0
  def valueSize(value: Value)(implicit version: SerializationVersion): Int =
    ValueCoder.encodeValue(version, value).fold(_ => 0, _.size())
}
