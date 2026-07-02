// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateSerializationVersion
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import com.digitalasset.daml.lf.transaction.{NodeId, SerializationVersion}
import com.digitalasset.daml.lf.value.Value.ValueRecord
import org.scalatest.wordspec.AnyWordSpecLike

/** Tests that transaction hashing is stack-safe for deeply nested and wide transactions (both many
  * children, and many root nodes).
  */
class TransactionHashStackSafetyTest extends BaseTest with AnyWordSpecLike {

  /** Depth of the exercise chain. This should be large enough to overflow the stack with a naive
    * recursive implementation, but small enough to keep test duration reasonable.
    */
  private val nestingDepth = 10000

  /** Width of the transaction (number of sibling children under a single root exercise). This
    * should be large enough to overflow the stack with a naive recursive implementation on children
    * iteration, but small enough to keep test duration reasonable.
    */
  private val transactionWidth = 10000

  /** Builds a transaction consisting of a single chain of nested exercise nodes: exercise_0 ->
    * exercise_1 -> ... -> exercise_(depth-1) -> create
    *
    * Construction is iterative (top-down) to avoid blowing the stack during test setup.
    *
    * @return
    *   the versioned transaction and a corresponding node seed map
    */
  private def buildDeeplyNestedTransaction(
      depth: Int,
      serializationVersion: SerializationVersion,
  ) = {
    val builder = new NodeIdTransactionBuilder with TestNodeBuilder

    val createNode = builder.create(
      id = builder.newCid,
      templateId = Ref.TypeConId.assertFromString("pkg:M:T"),
      argument = ValueRecord(None, ImmArray.Empty),
      signatories = Set(Ref.Party.assertFromString("alice")),
      observers = Set.empty,
      version = CreateSerializationVersion.Version(serializationVersion),
    )

    def makeExercise() = builder.exercise(
      createNode,
      choice = Ref.ChoiceName.assertFromString("C"),
      consuming = false,
      actingParties = Set(Ref.Party.assertFromString("alice")),
      argument = ValueRecord(None, ImmArray.Empty),
      byKey = false,
    )

    // Build a chain of exercises top-down iteratively.
    // Add the outermost exercise as a root, then add each subsequent exercise as a child
    // of the previous one, and finally add the create as a child of the innermost exercise.
    val innermostExerciseId = (1 until depth).foldLeft(builder.add(makeExercise())) {
      (parentId, _) => builder.add(makeExercise(), parentId)
    }
    builder.add(createNode, innermostExerciseId)

    val tx = builder.build()

    // Build node seeds for all nodes (the builder assigns NodeIds starting from 0).
    // There are `depth` exercise nodes + 1 create node.
    val nodeSeeds: Map[NodeId, LfHash] = (0 to depth).map { idx =>
      NodeId(idx) -> LfHash.hashPrivateKey(s"seed-$idx")
    }.toMap

    (tx, nodeSeeds)
  }

  /** Builds a wide transaction consisting of a single root exercise with many sibling create
    * children: exercise -> (create_0, create_1, ..., create_(width-1)).
    *
    * @return
    *   the versioned transaction and a corresponding node seed map
    */
  private def buildWideTransaction(
      width: Int,
      serializationVersion: SerializationVersion,
  ) = {
    val builder = new NodeIdTransactionBuilder with TestNodeBuilder

    def makeCreate() = builder.create(
      id = builder.newCid,
      templateId = Ref.TypeConId.assertFromString("pkg:M:T"),
      argument = ValueRecord(None, ImmArray.Empty),
      signatories = Set(Ref.Party.assertFromString("alice")),
      observers = Set.empty,
      version = CreateSerializationVersion.Version(serializationVersion),
    )

    val rootExercise = builder.exercise(
      makeCreate(),
      choice = Ref.ChoiceName.assertFromString("C"),
      consuming = false,
      actingParties = Set(Ref.Party.assertFromString("alice")),
      argument = ValueRecord(None, ImmArray.Empty),
      byKey = false,
    )

    val rootId = builder.add(rootExercise)
    (0 until width).foreach { _ =>
      builder.add(makeCreate(), rootId)
    }

    val tx = builder.build()

    // 1 root exercise + `width` create children, NodeIds assigned starting from 0.
    val nodeSeeds: Map[NodeId, LfHash] = (0 to width).map { idx =>
      NodeId(idx) -> LfHash.hashPrivateKey(s"seed-$idx")
    }.toMap

    (tx, nodeSeeds)
  }

  /** Builds a transaction with a very large number of root create nodes (no nesting). This
    * exercises the top-level roots traversal, which is a different code path from per-node children
    * iteration.
    *
    * @return
    *   the versioned transaction and a corresponding node seed map
    */
  private def buildManyRootsTransaction(
      numRoots: Int,
      serializationVersion: SerializationVersion,
  ) = {
    val builder = new NodeIdTransactionBuilder with TestNodeBuilder

    def makeCreate() = builder.create(
      id = builder.newCid,
      templateId = Ref.TypeConId.assertFromString("pkg:M:T"),
      argument = ValueRecord(None, ImmArray.Empty),
      signatories = Set(Ref.Party.assertFromString("alice")),
      observers = Set.empty,
      version = CreateSerializationVersion.Version(serializationVersion),
    )

    (0 until numRoots).foreach(_ => builder.add(makeCreate()))

    val tx = builder.build()

    val nodeSeeds: Map[NodeId, LfHash] = (0 until numRoots).map { idx =>
      NodeId(idx) -> LfHash.hashPrivateKey(s"seed-$idx")
    }.toMap

    (tx, nodeSeeds)
  }

  /** Number of root nodes in the many-roots transaction. */
  private val numRoots = 10000

  private val versionToSerializationVersion: Map[HashingSchemeVersion, SerializationVersion] = Map(
    HashingSchemeVersion.V2 -> SerializationVersion.V1,
    HashingSchemeVersion.V3 -> SerializationVersion.V2,
  )

  "VersionedTransactionHasher" should {
    versionToSerializationVersion.foreach { case (hashingVersion, serializationVersion) =>
      s"be stack-safe for deeply nested transactions ($hashingVersion, depth=$nestingDepth)" in {
        val (tx, nodeSeeds) =
          buildDeeplyNestedTransaction(nestingDepth, serializationVersion)

        // This would throw a StackOverflowError with a naive recursive implementation
        val hash =
          VersionedTransactionHasher.tryHashTransaction(hashingVersion, tx, nodeSeeds)
        hash should not be null
      }

      s"be stack-safe for very wide transactions ($hashingVersion, width=$transactionWidth)" in {
        val (tx, nodeSeeds) =
          buildWideTransaction(transactionWidth, serializationVersion)

        // This would throw a StackOverflowError with a naive recursive implementation
        // on children iteration.
        val hash =
          VersionedTransactionHasher.tryHashTransaction(hashingVersion, tx, nodeSeeds)
        hash should not be null
      }

      s"be stack-safe for transactions with many root nodes ($hashingVersion, roots=$numRoots)" in {
        val (tx, nodeSeeds) =
          buildManyRootsTransaction(numRoots, serializationVersion)

        // This would throw a StackOverflowError with a naive recursive implementation
        // on the top-level roots traversal.
        val hash =
          VersionedTransactionHasher.tryHashTransaction(hashingVersion, tx, nodeSeeds)
        hash should not be null
      }
    }
  }
}
