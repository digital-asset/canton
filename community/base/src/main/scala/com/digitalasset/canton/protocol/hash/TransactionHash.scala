// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.NodeHashBuilder.LFSerializationVersionMappingToMinimumHashingSchemeVersion
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.transaction.SerializationVersion.V2

import scala.util.control.NoStackTrace

object TransactionHash {
  sealed abstract class NodeHashingError(val msg: String) extends Exception(msg) with NoStackTrace
  object NodeHashingError {
    final case class UnsupportedFeature(message: String) extends NodeHashingError(message)
    final case class MissingNodeSeed(message: String) extends NodeHashingError(message)
    final case class IncompleteTransactionTree(nodeId: NodeId)
        extends NodeHashingError(s"The transaction does not contain a node with nodeId $nodeId")
    final case class UnsupportedSerializationVersion(
        nodeHashVersion: HashingSchemeVersion,
        version: SerializationVersion,
    ) extends NodeHashingError(
          s"Cannot hash node with LF serialization version $version using hashing scheme $nodeHashVersion." +
            // If LFS version is V2, it's likely because of contract keys. Add a hint for the user.
            (if (version == V2) " Does the transaction use contract keys?" else "") +
            s" Please using hashing scheme ${LFSerializationVersionMappingToMinimumHashingSchemeVersion.get(version).map(_.toString).getOrElse("N/A")} or higher."
        )
    final case class UnsupportedHashingVersion(version: HashingSchemeVersion)
        extends NodeHashingError(
          s"Cannot hash node with hashing version $version. Supported versions: ${NodeHashBuilder.HashingVersionToMaxSupportedLFSerializationVersionMapping.keySet
              .mkString(", ")}"
        )
  }

  /** Deterministically hash a versioned transaction and its metadata according to hashVersion.
    *
    * @param hashTracer
    *   tracer that can be used to debug encoding of the transaction.
    */
  @throws[NodeHashingError]
  def tryHashTransactionWithMetadata(
      hashVersion: HashingSchemeVersion,
      versionedTransaction: VersionedTransaction,
      nodeSeeds: Map[NodeId, LfHash],
      metadata: TransactionMetadataForHashing,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    TransactionHashBuilder(
      HashPurpose.PreparedSubmission,
      hashTracer,
      hashVersion,
    )
      .addHash(
        VersionedTransactionHasher.tryHashTransaction(
          hashVersion,
          versionedTransaction,
          nodeSeeds,
          hashTracer.subNodeTracer,
        ),
        "Transaction",
      )
      .addHash(
        TransactionMetadataHasher(hashVersion).tryHashMetadata(metadata, hashTracer.subNodeTracer),
        "Metadata",
      )
      .finish()

}
