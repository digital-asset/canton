// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.*

object VersionedTransactionHasher {

  /** Deterministically hash a versioned transaction in accordance with the provided hash version.
    */
  @throws[NodeHashingError]
  private[hash] def tryHashTransaction(
      hashVersion: HashingSchemeVersion,
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash = {
    val txNodes = versionedTransaction.nodes

    // A versioned transaction is a forest of trees. Hash the root count prefix, then hash each
    // root tree independently (stack-safe via RoseTree.foldLeft inside hashNode) and add the
    // resulting hash to the transaction builder.
    val builder = NodeHashBuilder(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = true,
      hashingSchemeVersion = hashVersion,
    )
      .addPurpose()
      .withContext("Serialization Version")(
        _.addString(SerializationVersion.toProtoValue(versionedTransaction.version))
      )
      .withContext("Root Nodes")(_.addInt(versionedTransaction.roots.length))

    versionedTransaction.roots.foreach { rootId =>
      val rootNode =
        txNodes.getOrElse(rootId, throw NodeHashingError.IncompleteTransactionTree(rootId))
      builder.addHash(
        builder
          .hashNode(rootNode, nodeSeeds.get(rootId), txNodes, nodeSeeds, hashTracer.subNodeTracer),
        "(Hashed Root Node)",
      )
    }

    builder.finish()
  }
}
