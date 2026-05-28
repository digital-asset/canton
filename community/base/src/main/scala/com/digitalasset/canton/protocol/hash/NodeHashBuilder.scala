// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.util.RoseTree
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.{Node, NodeId, SerializationVersion}

import scala.collection.immutable.SortedMap

/** Hash builder with additional methods to encode and hash nodes. Specific hashing version schemes
  * must implement this class.
  */
private[hash] abstract class NodeHashBuilder(
    purpose: HashPurpose,
    hashTracer: HashTracer,
    protected val hashingSchemeVersion: HashingSchemeVersion,
) extends LfValueHashBuilder(purpose, hashTracer) {

  /** Initializes this builder with a node's fields. For exercise and rollback nodes (which have
    * children), writes all fields including the children array length prefix, but NOT the children
    * hashes themselves. Use [[hashNode]] to hash a complete subtree iteratively.
    */
  protected def initNode(
      node: Node,
      nodeSeedO: Option[LfHash],
  ): this.type

  /** Creates a new builder, using the provided hash tracer
    */
  private[hash] def newBuilder(hashTracer: HashTracer): NodeHashBuilder

  /** Hashes a node and its entire subtree using an iterative (stack-safe) traversal powered by
    * [[RoseTree.foldLeft]]. Exercise and rollback nodes are handled by writing the children array
    * length prefix in [[initNode]], then adding each child hash via the fold update step, avoiding
    * recursive JVM stack frames. Takes a `Node` directly rather than a `NodeId` because the
    * metadata hasher calls this for disclosed Create nodes that have no NodeId. Using NodeId as the
    * item type would require Option[NodeId] or a fake id at the metadata callsite.
    */
  private[hash] final def hashNode(
      node: Node,
      nodeSeed: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
      hashTracer: HashTracer,
  ): Hash = {
    // Validate the root node's LF serialization version upfront.
    node.optVersion.foreach(
      NodeHashBuilder
        .assertHashingVersionSupportsLfSerializationVersion(_, hashingSchemeVersion)
    )

    // Each item carries the node, its seed, and the tracer to use when creating its builder.
    // The tracer is threaded through the children iterator so that nesting depth is preserved for
    // debug tracers (each level uses subNodeTracer of its parent).
    // Node is used directly rather than NodeId because we don't know the nodeId of root nodes here.
    // It also keeps the RoseTree.foldLeft call very simple because we keep the NodeId -> Node lookup
    // localized to the children call.
    type Item = (Node, Option[LfHash], HashTracer)

    val treeOps = new RoseTree.TreeOps[Item] {
      override def children(item: Item): Iterator[Item] = {
        val (n, _, itemTracer) = item
        val childTracer = itemTracer.subNodeTracer
        val childIds: Iterator[NodeId] = n match {
          case e: Node.Exercise => e.children.iterator
          case r: Node.Rollback => r.children.iterator
          case _ => Iterator.empty
        }
        childIds.map { id =>
          val child =
            nodes.getOrElse(id, throw NodeHashingError.IncompleteTransactionTree(id))
          // Validate each child's LF serialization version before descending.
          child.optVersion.foreach(
            NodeHashBuilder
              .assertHashingVersionSupportsLfSerializationVersion(_, hashingSchemeVersion)
          )
          (child, nodeSeeds.get(id), childTracer)
        }
      }
    }

    RoseTree.foldLeft(treeOps, (node, nodeSeed, hashTracer))(
      init = { case (n, seedOpt, tracer) => newBuilder(tracer).initNode(n, seedOpt) }
    )(
      finish = _.finish()
    )(
      update = { (builder, childHash) => builder.addHash(childHash, "(Hashed Inner Node)") }
    )
  }
}

private[hash] object NodeHashBuilder {
  private[hash] val HashingVersionToMaxSupportedLFSerializationVersionMapping
      : SortedMap[HashingSchemeVersion, SerializationVersion] =
    SortedMap(
      HashingSchemeVersion.V2 -> SerializationVersion.V1,
      HashingSchemeVersion.V3 -> SerializationVersion.V2,
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
