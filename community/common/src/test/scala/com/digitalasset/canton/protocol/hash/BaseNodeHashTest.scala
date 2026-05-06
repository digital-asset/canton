// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError.IncompleteTransactionTree
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageName, Party}
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.transaction.Node.QueryByKey
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

/** Abstract test suite with common tests for existing hashing scheme versions (V2, V3). Specific
  * tests for specific version are added in corresponding subclasses
  */
abstract class BaseNodeHashTest
    extends BaseTest
    with AnyWordSpecLike
    with Matchers
    with HashUtilsTest {

  // Version specific values //
  protected def hashingSchemeVersion: HashingSchemeVersion
  protected val createNodeEncoding: String
  protected val createNodeHash: String
  protected val fetchNodeEncoding: String
  protected val fetchNodeHash: String
  protected val exerciseNodeEncoding: String
  protected val exerciseNodeHash: String
  protected val rollbackNodeEncoding: String
  protected val rollbackNodeHash: String
  protected val transactionEncoding: String
  protected val fullTransactionEncoding: String
  protected val fullTransactionHash: String
  protected val transactionHash: String
  protected val metadataHash: String
  protected def serializationVersion: SerializationVersion

  // Global Keys //
  protected val globalKey = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(
      defRef("module_key", "name"),
      PackageName.assertFromString("package_name_key"),
      VA.text.inj("hello"),
      crypto.Hash.hashPrivateKey("dummy-hello-key-hash"),
    ),
    Set[Party](Ref.Party.assertFromString("david")),
  )
  protected val globalKey2 = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(
      defRef("module_key", "name"),
      PackageName.assertFromString("package_name_key"),
      VA.text.inj("bye"),
      crypto.Hash.hashPrivateKey("dummy-bye-key-hash"),
    ),
    Set[Party](Ref.Party.assertFromString("david")),
  )

  // Contract Ids //
  protected val contractId1 = "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
  protected val contractId2 = "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"

  // Create nodes
  protected def createNode = Node.Create(
    coid = ContractId.V1.assertFromString(contractId1),
    packageName = packageName0,
    templateId = defRef("module", "name"),
    arg = VA.text.inj("hello"),
    signatories =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    stakeholders =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("charlie")),
    keyOpt = None,
    version = serializationVersion,
  )
  protected lazy val createNode2: Node.Create = createNode.copy(
    coid = ContractId.V1.assertFromString(contractId2)
  )

  // Fetch nodes
  protected def fetchNode = Node.Fetch(
    coid = ContractId.V1.assertFromString(contractId1),
    packageName = packageName0,
    templateId = defRef("module", "name"),
    actingParties =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    signatories = Set[Party](Ref.Party.assertFromString("alice")),
    stakeholders = Set[Party](Ref.Party.assertFromString("charlie")),
    keyOpt = None,
    byKey = false,
    interfaceId = None,
    version = serializationVersion,
  )
  protected lazy val fetchNode2: Node.Fetch = fetchNode.copy(
    coid = ContractId.V1.assertFromString(contractId2)
  )

  // Exercise node
  protected def exerciseNode = Node.Exercise(
    targetCoid = ContractId.V1.assertFromString(
      "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
    ),
    packageName = packageName0,
    templateId = defRef("module", "name"),
    interfaceId = Some(defRef("interface_module", "interface_name")),
    choiceId = ChoiceName.assertFromString("choice"),
    consuming = true,
    actingParties =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    chosenValue = VA.int64.inj(31380L),
    stakeholders = Set[Party](Ref.Party.assertFromString("charlie")),
    signatories = Set[Party](Ref.Party.assertFromString("alice")),
    choiceObservers = Set[Party](Ref.Party.assertFromString("david")),
    choiceAuthorizers = None,
    children = ImmArray(NodeId(0), NodeId(2)), // Create and Fetch
    exerciseResult = Some(VA.text.inj("result")),
    keyOpt = None,
    byKey = false,
    version = serializationVersion,
  )

  // Lookup / Query node
  protected lazy val queryNode: QueryByKey = Node.QueryByKey(
    packageName = packageName0,
    templateId = defRef("module", "name"),
    exhaustive = true,
    key = globalKey,
    result = Vector(
      ContractId.V1.assertFromString(contractId1)
    ),
    version = serializationVersion,
  )

  // Rollback node
  protected val rollbackNode = Node.Rollback(
    children = ImmArray(NodeId(2), NodeId(4)) // Fetch2 and Exercise
  )

  // Seeds
  private val nodeSeedCreate =
    LfHash.assertFromString("926bbb6f341bc0092ae65d06c6e284024907148cc29543ef6bff0930f5d52c19")
  private val nodeSeedFetch =
    LfHash.assertFromString("4d2a522e9ee44e31b9bef2e3c8a07d43475db87463c6a13c4ea92f898ac8a930")
  private val nodeSeedExercise =
    LfHash.assertFromString("a867edafa1277f46f879ab92c373a15c2d75c5d86fec741705cee1eb01ef8c9e")
  private val nodeSeedRollback =
    LfHash.assertFromString("5483d5df9b245e662c0e4368b8062e8a0fd24c17ce4ded1a0e452e4ee879dd81")
  private val nodeSeedCreate2 =
    LfHash.assertFromString("e0c69eae8afb38872fa425c2cdba794176f3b9d97e8eefb7b0e7c831f566458f")
  private val nodeSeedFetch2 =
    LfHash.assertFromString("b4f0534e651ac8d5e10d95ddfcafdb123550a5e3185e3fe61ec1746a7222a88e")
  private val defaultNodeSeedsMap: Map[NodeId, LfHash] = Map(
    NodeId(0) -> nodeSeedCreate,
    NodeId(1) -> nodeSeedCreate2,
    NodeId(2) -> nodeSeedFetch,
    NodeId(3) -> nodeSeedFetch2,
    NodeId(4) -> nodeSeedExercise,
    NodeId(5) -> nodeSeedRollback,
  )

  // Id -> Node mapping
  protected val subNodesMap: Map[NodeId, Node] = Map(
    NodeId(0) -> createNode,
    NodeId(1) -> createNode2,
    NodeId(2) -> fetchNode,
    NodeId(3) -> fetchNode2,
    NodeId(4) -> exerciseNode,
    NodeId(5) -> rollbackNode,
  )

  // Utility functions
  protected def tryHashNode(
      node: Node,
      nodeSeeds: Map[NodeId, LfHash] = Map.empty,
      nodeSeed: Option[LfHash] = None,
      subNodes: Map[NodeId, Node] = Map.empty,
      hashTracer: HashTracer = HashTracer.NoOp,
      enforceNodeSeedForCreateNodes: Boolean = true,
  ): Hash = tryHashNodeWithVersion(
    node,
    hashingSchemeVersion,
    nodeSeeds,
    nodeSeed,
    subNodes,
    hashTracer,
    enforceNodeSeedForCreateNodes,
  )

  protected def hashNodeWithDefaultNodeSeedMap(
      node: Node,
      nodeSeed: Option[LfHash],
      subNodes: Map[NodeId, Node] = subNodesMap,
      hashTracer: HashTracer,
  ): Hash = tryHashNode(
    node,
    defaultNodeSeedsMap,
    nodeSeed,
    subNodes,
    hashTracer,
  )

  private def shiftNodeIds(array: ImmArray[NodeId]): ImmArray[NodeId] = array.map {
    case NodeId(i) => NodeId(i + 1)
  }

  private def shiftNodeIdsSeeds(map: Map[NodeId, LfHash]): Map[NodeId, LfHash] = map.map {
    case (NodeId(i), value) => NodeId(i + 1) -> value
  }

  private def shiftNodeIds(map: Map[NodeId, Node]): Map[NodeId, Node] = map.map {
    case (NodeId(i), exercise: Node.Exercise) =>
      NodeId(i + 1) -> exercise.copy(children = shiftNodeIds(exercise.children))
    case (NodeId(i), rollback: Node.Rollback) =>
      NodeId(i + 1) -> rollback.copy(children = shiftNodeIds(rollback.children))
    case (NodeId(i), node) => NodeId(i + 1) -> node
  }

  protected def defaultCreateHash: Hash = Hash
    .fromHexStringRaw(createNodeHash)
    .getOrElse(fail("Invalid hash"))

  protected def hashCreateNode(node: Node.Create, hashTracer: HashTracer = HashTracer.NoOp): Hash =
    hashNodeWithDefaultNodeSeedMap(node, Some(nodeSeedCreate), hashTracer = hashTracer)

  "Encoding" should {
    "not encode create nodes without node seed" in {
      a[NodeHashingError.MissingNodeSeed] shouldBe thrownBy {
        tryHashNode(createNode, enforceNodeSeedForCreateNodes = true)
      }
    }

    "not encode exercise nodes without node seed" in {
      a[NodeHashingError.MissingNodeSeed] shouldBe thrownBy {
        tryHashNode(exerciseNode, enforceNodeSeedForCreateNodes = true)
      }
    }

    "encode create nodes without node seed if explicitly allowed" in {
      scala.util
        .Try(tryHashNode(createNode, enforceNodeSeedForCreateNodes = false))
        .isSuccess shouldBe true
    }
  }

  "CreateNodeBuilder" should {
    "be stable" in {
      hashCreateNode(createNode).toHexString shouldBe defaultCreateHash.toHexString
    }

    "not produce collision in contractId" in {
      hashCreateNode(
        createNode.copy(
          coid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in package name" in {
      hashCreateNode(
        createNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in template ID" in {
      hashCreateNode(
        createNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in arg" in {
      hashCreateNode(
        createNode.copy(
          arg = VA.bool.inj(true)
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in signatories" in {
      hashCreateNode(
        createNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in stakeholders" in {
      hashCreateNode(
        createNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultCreateHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashCreateNode(createNode, hashTracer = hashTracer)
      hash shouldBe defaultCreateHash
      hashTracer.result shouldBe createNodeEncoding
      assertStringTracer(hashTracer, hash)
    }
  }

  protected def defaultFetchHash = Hash
    .fromHexStringRaw(fetchNodeHash)
    .getOrElse(fail("Invalid hash"))

  protected def hashFetchNode(node: Node.Fetch, hashTracer: HashTracer = HashTracer.NoOp): Hash =
    hashNodeWithDefaultNodeSeedMap(node, nodeSeed = Some(nodeSeedFetch), hashTracer = hashTracer)

  "FetchNodeBuilder" should {
    "be stable" in {
      hashFetchNode(fetchNode).toHexString shouldBe defaultFetchHash.toHexString
    }

    "not produce collision in contractId" in {
      hashFetchNode(
        fetchNode.copy(
          coid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in package name" in {
      hashFetchNode(
        fetchNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in template ID" in {
      hashFetchNode(
        fetchNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in actingParties" in {
      hashFetchNode(
        fetchNode.copy(
          actingParties = Set[Party](Ref.Party.assertFromString("charlie"))
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in signatories" in {
      hashFetchNode(
        fetchNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("bob"))
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in stakeholders" in {
      hashFetchNode(
        fetchNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultFetchHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashFetchNode(fetchNode, hashTracer = hashTracer)
      hash shouldBe defaultFetchHash
      hashTracer.result shouldBe s"""$fetchNodeEncoding
                                    |""".stripMargin

      assertStringTracer(hashTracer, hash)
    }
  }

  protected lazy val defaultExerciseHash: Hash = Hash
    .fromHexStringRaw(exerciseNodeHash)
    .getOrElse(fail("Invalid hash"))

  protected def hashExerciseNode(
      node: Node.Exercise,
      subNodes: Map[NodeId, Node] = subNodesMap,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    hashNodeWithDefaultNodeSeedMap(
      node,
      nodeSeed = Some(nodeSeedExercise),
      subNodes = subNodes,
      hashTracer = hashTracer,
    )

  "ExerciseNodeBuilder" should {
    "be stable" in {
      hashExerciseNode(exerciseNode).toHexString shouldBe defaultExerciseHash.toHexString
    }

    "not include choiceAuthorizers" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashExerciseNode(
          exerciseNode.copy(choiceAuthorizers =
            Some(Set[Party](Ref.Party.assertFromString("alice")))
          )
        )
      )
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        hashExerciseNode(exerciseNode, subNodes = Map.empty)
      }
    }

    "not hash NodeIds" in {
      tryHashNode(
        exerciseNode
          // Shift all node ids by one and expect it to have no impact
          .copy(children = shiftNodeIds(exerciseNode.children)),
        subNodes = shiftNodeIds(subNodesMap),
        nodeSeed = Some(nodeSeedExercise),
        nodeSeeds = shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultExerciseHash
    }

    "not produce collision in contractId" in {
      hashExerciseNode(
        exerciseNode.copy(
          targetCoid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in package name" in {
      hashExerciseNode(
        exerciseNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in template ID" in {
      hashExerciseNode(
        exerciseNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in actingParties" in {
      hashExerciseNode(
        exerciseNode.copy(
          actingParties = Set[Party](Ref.Party.assertFromString("charlie"))
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in signatories" in {
      hashExerciseNode(
        exerciseNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("bob"))
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in stakeholders" in {
      hashExerciseNode(
        exerciseNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in choiceObservers" in {
      hashExerciseNode(
        exerciseNode.copy(
          choiceObservers = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in children" in {
      hashExerciseNode(
        exerciseNode.copy(
          children = exerciseNode.children.reverse
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in interface Id" in {
      hashExerciseNode(
        exerciseNode.copy(
          interfaceId = None
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in exercise result" in {
      hashExerciseNode(
        exerciseNode.copy(
          exerciseResult = None
        )
      ) should !==(defaultExerciseHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashExerciseNode(exerciseNode, hashTracer = hashTracer)
      hash shouldBe defaultExerciseHash
      hashTracer.result shouldBe exerciseNodeEncoding

      assertStringTracer(hashTracer, hash)
    }
  }

  private lazy val defaultRollbackHash = Hash
    .fromHexStringRaw(rollbackNodeHash)
    .getOrElse(fail("Invalid hash"))

  protected def hashRollbackNode(
      node: Node.Rollback,
      subNodes: Map[NodeId, Node] = subNodesMap,
      hashTracer: HashTracer = HashTracer.NoOp,
  ) =
    tryHashNode(
      node,
      nodeSeed = Some(nodeSeedRollback),
      nodeSeeds = defaultNodeSeedsMap,
      subNodes = subNodes,
      hashTracer = hashTracer,
    )

  "RollbackNode Builder" should {
    "be stable" in {
      hashRollbackNode(rollbackNode).toHexString shouldBe defaultRollbackHash.toHexString
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        hashRollbackNode(rollbackNode, subNodes = Map.empty)
      }
    }

    "not hash NodeIds" in {
      tryHashNode(
        rollbackNode
          // Change the node Ids values but not the nodes
          .copy(children = shiftNodeIds(rollbackNode.children)),
        subNodes = shiftNodeIds(subNodesMap),
        nodeSeed = Some(nodeSeedRollback),
        nodeSeeds = shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultRollbackHash
    }

    "not produce collision in children" in {
      hashRollbackNode(
        rollbackNode.copy(
          children = rollbackNode.children.reverse
        )
      ) should !==(defaultRollbackHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashRollbackNode(rollbackNode, hashTracer = hashTracer)
      hash shouldBe defaultRollbackHash
      hashTracer.result shouldBe rollbackNodeEncoding

      assertStringTracer(hashTracer, hash)
    }
  }

  private lazy val defaultTransactionHash = Hash
    .fromHexStringRaw(transactionHash)
    .getOrElse(fail("Invalid hash"))

  "TransactionBuilder" should {
    val roots = ImmArray(NodeId(0), NodeId(5))
    val transaction = VersionedTransaction(
      version = serializationVersion,
      roots = roots,
      nodes = subNodesMap,
    )

    "be stable" in {
      VersionedTransactionHasher
        .tryHashTransaction(hashingSchemeVersion, transaction, defaultNodeSeedsMap)
        .toHexString shouldBe defaultTransactionHash.toHexString
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        VersionedTransactionHasher.tryHashTransaction(
          HashingSchemeVersion.V2,
          VersionedTransaction(
            version = serializationVersion,
            roots = roots,
            nodes = Map.empty,
          ),
          defaultNodeSeedsMap,
        )
      }
    }

    "not hash NodeIds" in {
      VersionedTransactionHasher.tryHashTransaction(
        hashingSchemeVersion,
        VersionedTransaction(
          version = serializationVersion,
          roots = shiftNodeIds(roots),
          nodes = shiftNodeIds(subNodesMap),
        ),
        shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultTransactionHash
    }

    "not produce collision in children" in {
      VersionedTransactionHasher.tryHashTransaction(
        hashingSchemeVersion,
        VersionedTransaction(
          version = serializationVersion,
          roots = roots.reverse,
          nodes = subNodesMap,
        ),
        defaultNodeSeedsMap,
      ) should !==(defaultTransactionHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = VersionedTransactionHasher.tryHashTransaction(
        hashingSchemeVersion,
        transaction,
        defaultNodeSeedsMap,
        hashTracer = hashTracer,
      )
      hashTracer.result shouldBe transactionEncoding
      assertStringTracer(hashTracer, hash)
    }
  }

  private lazy val defaultFullTransactionHash = Hash
    .fromHexStringRaw(fullTransactionHash)
    .getOrElse(fail("Invalid hash"))

  "Full Transaction Hash" should {
    val roots = ImmArray(NodeId(0), NodeId(5))
    val transaction = VersionedTransaction(
      version = serializationVersion,
      roots = roots,
      nodes = subNodesMap,
    )

    "be stable" in {
      TransactionHash
        .tryHashTransactionWithMetadata(
          hashingSchemeVersion,
          transaction,
          defaultNodeSeedsMap,
          metadata(serializationVersion),
        )
        .toHexString shouldBe defaultFullTransactionHash.toHexString
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = TransactionHash.tryHashTransactionWithMetadata(
        hashingSchemeVersion,
        transaction,
        defaultNodeSeedsMap,
        metadata(serializationVersion),
        hashTracer = hashTracer,
      )
      hashTracer.result shouldBe fullTransactionEncoding
      assertStringTracer(hashTracer, hash)
    }
  }
}
