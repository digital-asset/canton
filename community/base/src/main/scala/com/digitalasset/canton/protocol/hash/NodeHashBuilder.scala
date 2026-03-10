// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.{Node, NodeId, SerializationVersion}

import scala.collection.immutable.SortedMap

import TransactionHash.*

/** Hash builder with additional methods to encode and hash nodes. Specific hashing version schemes
  * must implement this class.
  */
private abstract class NodeHashBuilder(
    purpose: HashPurpose,
    hashTracer: HashTracer,
    protected val hashingSchemeVersion: HashingSchemeVersion,
) extends LfValueHashBuilder(purpose, hashTracer) {

  /** Adds a node to this builder, and returns it
    */
  protected def addNode(
      node: Node,
      nodeSeedO: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): this.type

  /** Creates a new builder, using the provided hash tracer
    */
  private[hash] def newBuilder(hashTracer: HashTracer): NodeHashBuilder

  private[hash] final def hashNode(
      node: Node,
      nodeSeed: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer,
  ): Hash = {
    // Ensure the LF serialization version of the node (optVersion) can be hashed
    // with the HashingSchemeVersion implemented by this class (hashingSchemeVersion)
    node.optVersion
      .foreach(
        NodeHashBuilder
          .assertHashingVersionSupportsLfSerializationVersion(_, hashingSchemeVersion)
      )

    newBuilder(hashTracer)
      .addNode(node, nodeSeed, nodes, nodeSeeds)
      .finish()
  }

  @throws[NodeHashingError]
  private def addNodeFromNodeId(
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): (this.type, NodeId) => this.type =
    (builder, nodeId) => {
      val node = nodes.getOrElse(nodeId, throw NodeHashingError.IncompleteTransactionTree(nodeId))
      addHash(
        builder.hashNode(node, nodeSeeds.get(nodeId), nodes, nodeSeeds, hashTracer.subNodeTracer),
        "(Hashed Inner Node)",
      )
    }

  def addNodesFromNodeIds(
      nodeIds: ImmArray[NodeId],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): this.type =
    addArray(nodeIds)(addNodeFromNodeId(nodes, nodeSeeds))
}

private object NodeHashBuilder {
  private[hash] val HashingVersionToMaxSupportedLFSerializationVersionMapping
      : SortedMap[HashingSchemeVersion, SerializationVersion] =
    SortedMap(
      HashingSchemeVersion.V2 -> SerializationVersion.V1,
      HashingSchemeVersion.V3 -> SerializationVersion.VDev,
    )
  private[hash] val LFSerializationVersionMappingToMinimumHashingSchemeVersion
      : Map[SerializationVersion, HashingSchemeVersion] =
    HashingVersionToMaxSupportedLFSerializationVersionMapping
      .groupMapReduce(_._2)(_._1)(Ordering[HashingSchemeVersion].min)

  private[hash] sealed abstract class NodeTag(val tag: Byte)

  private[hash] object NodeTag {
    case object CreateTag extends NodeTag(0)
    case object ExerciseTag extends NodeTag(1)
    case object FetchTag extends NodeTag(2)
    case object RollbackTag extends NodeTag(3)
    case object QueryByKey extends NodeTag(4)
  }

  @throws[NodeHashingError]
  private[hash] def assertHashingVersionSupportsLfSerializationVersion(
      nodeLfSerializationVersion: SerializationVersion,
      hashingSchemeVersion: HashingSchemeVersion,
  ): Unit = {
    val maximumLfSerializationVersionSupportedByHashingSchemeVersion: SerializationVersion =
      HashingVersionToMaxSupportedLFSerializationVersionMapping
        .getOrElse(
          hashingSchemeVersion,
          // This really shouldn't happen, unless someone removed an entry from the HashingVersionToMaxSupportedLFSerializationVersionMapping map
          throw NodeHashingError.UnsupportedHashingVersion(hashingSchemeVersion),
        )
    if (
      // If the node version is > the max supported version, throw
      Ordering[SerializationVersion].gt(
        nodeLfSerializationVersion,
        maximumLfSerializationVersionSupportedByHashingSchemeVersion,
      )
    )
      throw NodeHashingError.UnsupportedSerializationVersion(
        hashingSchemeVersion,
        nodeLfSerializationVersion,
      )
  }

  def apply(
      purpose: HashPurpose,
      hashTracer: HashTracer,
      enforceNodeSeedForCreateNodes: Boolean,
      hashingSchemeVersion: HashingSchemeVersion,
  ): NodeHashBuilder = hashingSchemeVersion match {
    case HashingSchemeVersion.V2 =>
      new v2.NodeHashBuilder(purpose, hashTracer, enforceNodeSeedForCreateNodes)
    case HashingSchemeVersion.V3 =>
      new v3.NodeHashBuilder(purpose, hashTracer, enforceNodeSeedForCreateNodes)
  }
}
