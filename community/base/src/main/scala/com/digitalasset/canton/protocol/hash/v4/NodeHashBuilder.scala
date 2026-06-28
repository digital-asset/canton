// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v4

import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.protocol.{LfHash, hash}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.{ExternalCallResult, Node}

/** Node hash builder for HashingSchemeVersion.V4.
  *
  * V4 extends V3 by including development-version exercise external-call results in the prepared
  * transaction hash. This binds the result payloads shown in a prepared transaction to the external
  * party's signature.
  */
private[hash] class NodeHashBuilder(
    purpose: HashPurpose,
    hashTracer: HashTracer,
    enforceNodeSeedForCreateNodes: Boolean,
) extends hash.v3.NodeHashBuilder(
      purpose,
      hashTracer,
      enforceNodeSeedForCreateNodes,
    ) {

  override protected val hashingSchemeVersion: HashingSchemeVersion = HashingSchemeVersion.V4

  override private[hash] def newBuilder(hashTracer: HashTracer): NodeHashBuilder =
    new NodeHashBuilder(purpose, hashTracer, enforceNodeSeedForCreateNodes)

  private def addExternalCallResult(result: ExternalCallResult): this.type =
    addContext("External Call Result")
      .withContext("Extension Id")(_.addString(result.extensionId))
      .withContext("Function Id")(_.addString(result.functionId))
      .withContext("Config")(_.addByteString(result.config.toByteString))
      .withContext("Input")(_.addByteString(result.input.toByteString))
      .withContext("Output")(_.addByteString(result.output.toByteString))

  override protected def addExerciseNodeNoChildren(
      nodeSeed: LfHash
  ): Node.Exercise => this.type = node =>
    super
      .addExerciseNodeNoChildren(nodeSeed)(node)
      .withContext("External Call Results") { hashBuilder =>
        hashBuilder.addArray(node.externalCallResults) { (builder, result) =>
          builder.addExternalCallResult(result)
        }
      }
}
