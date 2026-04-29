// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.SerializationVersion.V1
import com.digitalasset.daml.lf.transaction.{Node, NodeId, SerializationVersion}
import com.digitalasset.daml.lf.value.Value

/** Implementation of HashingSchemeVersion.V2 on Daml transaction nodes
  * @param enforceNodeSeedForCreateNodes
  *   when true, hashing will fail if a node seed is missing for a create node Set to false when
  *   hashing nodes coming from explicit disclosure for instance for which we don't have the seed.
  */
private class NodeHashBuilderV2(
    purpose: HashPurpose,
    hashTracer: HashTracer,
    enforceNodeSeedForCreateNodes: Boolean,
) extends NodeHashBuilder(purpose, hashTracer, HashingSchemeVersion.V2) {

  override protected def newBuilder(hashTracer: HashTracer): NodeHashBuilderV2 =
    new NodeHashBuilderV2(purpose, hashTracer, enforceNodeSeedForCreateNodes)

  protected def addCreateNode(nodeSeed: Option[LfHash]): Node.Create => this.type = {
    // Pattern match to make it more obvious which fields are part of the hashing and which are not
    case Node.Create(
          coid,
          packageName,
          templateId,
          arg,
          signatories,
          stakeholders,
          keyOpt,
          version,
        ) =>
      if (keyOpt.isDefined && version == V1) notSupported("keyOpt in Create node")
      addContext("Create Node")
        .withContext("Node Version")(_.addString(SerializationVersion.toProtoValue(version)))
        .addByte(NodeHashBuilder.NodeTag.CreateTag.tag, _ => "Create Node Tag")
        .withContext("Node Seed")(
          _.addOptional(nodeSeed, builder => seed => builder.addLfHash(seed, "node seed"))
        )
        .withContext("Contract Id")(_.addCid(coid))
        .withContext("Package Name")(_.addString(packageName))
        .withContext("Template Id")(_.addIdentifier(templateId))
        .withContext("Arg")(_.addTypedValue(arg))
        .withContext("Signatories")(_.addStringSet(signatories))
        .withContext("Stakeholders")(_.addStringSet(stakeholders))
  }

  protected def addFetchNode(node: Node.Fetch): this.type = node match {
    case Node.Fetch(
          coid,
          packageName,
          templateId,
          actingParties,
          signatories,
          stakeholders,
          keyOpt,
          byKey,
          interfaceId,
          version,
        ) =>
      if (keyOpt.nonEmpty && version == V1) notSupported("keyOpt in Fetch node")
      if (byKey && version == V1) notSupported("byKey in Fetch node")
      addContext("Fetch Node")
        .withContext("Node Version")(_.addString(SerializationVersion.toProtoValue(version)))
        .addByte(NodeHashBuilder.NodeTag.FetchTag.tag, _ => "Fetch Node Tag")
        .withContext("Contract Id")(_.addCid(coid))
        .withContext("Package Name")(_.addString(packageName))
        .withContext("Template Id")(_.addIdentifier(templateId))
        .withContext("Signatories")(_.addStringSet(signatories))
        .withContext("Stakeholders")(_.addStringSet(stakeholders))
        .withContext("Interface Id")(_.addOptional(interfaceId, _.addIdentifier))
        .withContext("Acting Parties")(_.addStringSet(actingParties))
  }

  protected def addExerciseNode(
      nodes: Map[NodeId, Node],
      nodeSeed: LfHash,
      nodeSeeds: Map[NodeId, LfHash],
  ): Node.Exercise => this.type = {
    case Node.Exercise(
          targetCoid,
          packageName,
          templateId,
          interfaceId,
          choiceId,
          consuming,
          actingParties,
          chosenValue,
          stakeholders,
          signatories,
          choiceObservers,
          choiceAuthorizers,
          children,
          exerciseResult,
          keyOpt,
          byKey,
          version,
        ) =>
      if (choiceAuthorizers.nonEmpty)
        notSupported("choiceAuthorizers in Exercise node") // 2.dev feature
      if (keyOpt.nonEmpty && version == V1) notSupported("keyOpt in Exercise node")
      if (byKey && version == V1) notSupported("byKey in Exercise node")
      addContext("Exercise Node")
        .withContext("Node Version")(_.addString(SerializationVersion.toProtoValue(version)))
        .addByte(NodeHashBuilder.NodeTag.ExerciseTag.tag, _ => "Exercise Node Tag")
        .withContext("Node Seed")(_.addLfHash(nodeSeed, "seed"))
        .withContext("Contract Id")(_.addCid(targetCoid))
        .withContext("Package Name")(_.addString(packageName))
        .withContext("Template Id")(_.addIdentifier(templateId))
        .withContext("Signatories")(_.addStringSet(signatories))
        .withContext("Stakeholders")(_.addStringSet(stakeholders))
        .withContext("Acting Parties")(_.addStringSet(actingParties))
        .withContext("Interface Id")(_.addOptional(interfaceId, _.addIdentifier))
        .withContext("Choice Id")(_.addString(choiceId))
        .withContext("Chosen Value")(_.addTypedValue(chosenValue))
        .withContext("Consuming")(_.addBool(consuming))
        .withContext("Exercise Result")(
          _.addOptional[Value](
            exerciseResult,
            builder => value => builder.addTypedValue(value),
          )
        )
        .withContext("Choice Observers")(_.addStringSet(choiceObservers))
        .withContext("Children")(_.addNodesFromNodeIds(children, nodes, nodeSeeds))
  }

  private def addRollbackNode(
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): Node.Rollback => this.type = { case Node.Rollback(children) =>
    addContext("Rollback Node")
      .addByte(NodeHashBuilder.NodeTag.RollbackTag.tag, _ => "Rollback Node Tag")
      .withContext("Children")(_.addNodesFromNodeIds(children, nodes, nodeSeeds))
  }

  protected def addNode(
      node: Node,
      nodeSeedO: Option[LfHash],
      nodes: Map[NodeId, Node],
      nodeSeeds: Map[NodeId, LfHash],
  ): this.type =
    (node, nodeSeedO) match {
      // Create nodes in a transaction should have a node seed, but we also need to encode create nodes for disclosed contracts
      // which do not have one.
      // We could differentiate between the 2 cases but to keep the encoding simpler we encode create nodes with an optional seed
      case (create: Node.Create, nodeSeed) =>
        // We can still enforce that create nodes within a transaction have a seed, even if we then encode it as an optional
        if (enforceNodeSeedForCreateNodes && nodeSeed.isEmpty) missingNodeSeed(node)
        addCreateNode(nodeSeed)(create)
      case (fetch: Node.Fetch, _) => addFetchNode(fetch)
      case (exercise: Node.Exercise, Some(nodeSeed)) =>
        addExerciseNode(nodes, nodeSeed, nodeSeeds)(exercise)
      case (_: Node.Exercise, None) => missingNodeSeed(node)
      case (rollback: Node.Rollback, _) => addRollbackNode(nodes, nodeSeeds)(rollback)
      case (_: Node.LookupByKey, _) =>
        notSupported(s"LookupByKey node")
    }

  private[this] def notSupported(str: String): Nothing =
    throw NodeHashingError.UnsupportedFeature(
      s"$str is not supported in version ${hashingSchemeVersion.index}"
    )

  private[this] def missingNodeSeed(node: Node): Nothing =
    throw NodeHashingError.MissingNodeSeed(
      s"Missing node seed for node $node"
    )
}
