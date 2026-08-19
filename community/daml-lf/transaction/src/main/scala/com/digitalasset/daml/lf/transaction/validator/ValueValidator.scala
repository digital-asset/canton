// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction
package validator

import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{Value, ValueCoder}

final case class ValueValidator(
    metadata: Transaction.Metadata,
    inputContracts: Map[ContractId, FatContractInstance],
    limits: Limits,
) extends TransactionValidator(metadata, inputContracts, limits) {
  import ValueValidator.*

  override def validate(transaction: SubmittedTransaction): Option[Limit.Error] = {
    def valueSizeExceeded(value: Value): Boolean =
      valueSize(value)(transaction.version) > limits.valueSize

    def validateNode(node: Node): Option[Limit.Error] =
      node match {
        case node: Node.Create if valueSizeExceeded(node.arg) =>
          Some(
            Limit.ValueSize(
              node.coid,
              node.templateId,
              node.arg,
              limits.valueSize,
            )
          )
        case node: Node.Create
            if node.keyOpt.map(k => valueSizeExceeded(k.globalKey.key)).getOrElse(false) =>
          node.keyOpt.map { key =>
            Limit.ValueSize(
              node.coid,
              node.templateId,
              key.globalKey.key,
              limits.valueSize,
            )
          }
        case node: Node.Fetch
            if node.keyOpt.map(k => valueSizeExceeded(k.globalKey.key)).getOrElse(false) =>
          node.keyOpt.map { key =>
            Limit.ValueSize(
              node.coid,
              node.templateId,
              key.globalKey.key,
              limits.valueSize,
            )
          }
        case node: Node.Exercise if valueSizeExceeded(node.chosenValue) =>
          Some(
            Limit.ValueSize(
              node.targetCoid,
              node.templateId,
              node.chosenValue,
              limits.valueSize,
            )
          )
        case node: Node.Exercise
            if node.exerciseResult.map(valueSizeExceeded(_)).getOrElse(false) =>
          node.exerciseResult.map { result =>
            Limit.ValueSize(
              node.targetCoid,
              node.templateId,
              result,
              limits.valueSize,
            )
          }
        case node: Node.Exercise
            if node.keyOpt.map(k => valueSizeExceeded(k.globalKey.key)).getOrElse(false) =>
          node.keyOpt.map { key =>
            Limit.ValueSize(
              node.targetCoid,
              node.templateId,
              key.globalKey.key,
              limits.valueSize,
            )
          }
        case node: Node.QueryByKey if valueSizeExceeded(node.key.globalKey.key) =>
          Some(
            Limit.ValueSize(
              node.packageName,
              node.templateId,
              node.key.globalKey.key,
              limits.valueSize,
            )
          )
        case _ =>
          None
      }

    inputContracts.values
      .collectFirst {
        case inst
            if valueSizeExceeded(inst.createArg) || inst.contractKeyWithMaintainers
              .map(k => valueSizeExceeded(k.globalKey.key))
              .getOrElse(false) =>
          inst.contractKeyWithMaintainers.map { key =>
            Limit.ValueSize(
              inst.packageName,
              inst.templateId,
              key.globalKey.key,
              limits.valueSize,
            )
          }
      }
      .flatten
      .orElse {
        transaction.nodes.values.collectFirst {
          case node if validateNode(node).isDefined =>
            validateNode(node).get
        }
      }
  }
}

object ValueValidator {
  // The following ignores contract ID suffixes for created non-global contracts
  // Values that can not be encoded into bytes have size 0
  def valueSize(value: Value)(implicit version: SerializationVersion): Int =
    ValueCoder.encodeValue(version, value).fold(_ => 0, _.size())
}
