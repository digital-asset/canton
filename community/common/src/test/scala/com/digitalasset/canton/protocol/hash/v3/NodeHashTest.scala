// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v3

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.{BaseNodeHashTest, HashTracer}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value.ContractId

/** Hashing Scheme V3 functional correctness tests. Common tests are shared in NodeHashTest. This
  * suite only adds tests specific to V2.
  */
class NodeHashTest extends BaseNodeHashTest {

  override protected lazy val hashingSchemeVersion: HashingSchemeVersion = HashingSchemeVersion.V3
  override lazy val serializationVersion: SerializationVersion = SerializationVersion.V2

  // Add a keys to the nodes
  override protected lazy val createNode: Node.Create = super.createNode.copy(
    keyOpt = Some(globalKey)
  )
  override protected lazy val fetchNode: Node.Fetch = super.fetchNode.copy(
    keyOpt = Some(globalKey),
    byKey = true,
  )
  override protected lazy val exerciseNode: Node.Exercise = super.exerciseNode.copy(
    keyOpt = Some(globalKey),
    byKey = true,
  )

  override protected val createNodeEncoding: String = s"""# Create Node
                                              |# Node Version
                                              |'00000001' # 1 (int)
                                              |'32' # 2 (string)
                                              |'00' # Create Node Tag
                                              |# Node Seed
                                              |'01' # Some
                                              |'926bbb6f341bc0092ae65d06c6e284024907148cc29543ef6bff0930f5d52c19' # node seed
                                              |# Contract Id
                                              |'00000021' # 33 (int)
                                              |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                              |# Package Name
                                              |'0000000e' # 14 (int)
                                              |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                              |# Template Id
                                              |'00000007' # 7 (int)
                                              |'7061636b616765' # package (string)
                                              |'00000001' # 1 (int)
                                              |'00000006' # 6 (int)
                                              |'6d6f64756c65' # module (string)
                                              |'00000001' # 1 (int)
                                              |'00000004' # 4 (int)
                                              |'6e616d65' # name (string)
                                              |# Arg
                                              |'07' # Text Type Tag
                                              |'00000005' # 5 (int)
                                              |'68656c6c6f' # hello (string)
                                              |# Signatories
                                              |'00000002' # 2 (int)
                                              |'00000005' # 5 (int)
                                              |'616c696365' # alice (string)
                                              |'00000003' # 3 (int)
                                              |'626f62' # bob (string)
                                              |# Stakeholders
                                              |'00000002' # 2 (int)
                                              |'00000005' # 5 (int)
                                              |'616c696365' # alice (string)
                                              |'00000007' # 7 (int)
                                              |'636861726c6965' # charlie (string)
                                              |# Key
                                              |'01' # Some
                                              |# Key
                                              |# Package Name
                                              |'00000010' # 16 (int)
                                              |'7061636b6167655f6e616d655f6b6579' # package_name_key (string)
                                              |# Template Id
                                              |'00000007' # 7 (int)
                                              |'7061636b616765' # package (string)
                                              |'00000001' # 1 (int)
                                              |'0000000a' # 10 (int)
                                              |'6d6f64756c655f6b6579' # module_key (string)
                                              |'00000001' # 1 (int)
                                              |'00000004' # 4 (int)
                                              |'6e616d65' # name (string)
                                              |# Value
                                              |'07' # Text Type Tag
                                              |'00000005' # 5 (int)
                                              |'68656c6c6f' # hello (string)
                                              |'4d1741d27564ab1f1e74873034760583a1a71edaa90dbad6be3a78c0bf3eec7c' # Global Key Hash
                                              |# Key Maintainers
                                              |'00000001' # 1 (int)
                                              |'00000005' # 5 (int)
                                              |'6461766964' # david (string)
                                              |""".stripMargin
  override protected val createNodeHash: String =
    "0120d370509f54c07d8209f5af2d23f7f972997deb5fc5e0ebe886354395a3bc"
  override protected val fetchNodeEncoding: String = """# Fetch Node
                                                       |# Node Version
                                                       |'00000001' # 1 (int)
                                                       |'32' # 2 (string)
                                                       |'02' # Fetch Node Tag
                                                       |# Contract Id
                                                       |'00000021' # 33 (int)
                                                       |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                                       |# Package Name
                                                       |'0000000e' # 14 (int)
                                                       |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                                       |# Template Id
                                                       |'00000007' # 7 (int)
                                                       |'7061636b616765' # package (string)
                                                       |'00000001' # 1 (int)
                                                       |'00000006' # 6 (int)
                                                       |'6d6f64756c65' # module (string)
                                                       |'00000001' # 1 (int)
                                                       |'00000004' # 4 (int)
                                                       |'6e616d65' # name (string)
                                                       |# Signatories
                                                       |'00000001' # 1 (int)
                                                       |'00000005' # 5 (int)
                                                       |'616c696365' # alice (string)
                                                       |# Stakeholders
                                                       |'00000001' # 1 (int)
                                                       |'00000007' # 7 (int)
                                                       |'636861726c6965' # charlie (string)
                                                       |# Interface Id
                                                       |'00' # None
                                                       |# Acting Parties
                                                       |'00000002' # 2 (int)
                                                       |'00000005' # 5 (int)
                                                       |'616c696365' # alice (string)
                                                       |'00000003' # 3 (int)
                                                       |'626f62' # bob (string)
                                                       |# By Key
                                                       |'01' # true (bool)
                                                       |# Key
                                                       |'01' # Some
                                                       |# Key
                                                       |# Package Name
                                                       |'00000010' # 16 (int)
                                                       |'7061636b6167655f6e616d655f6b6579' # package_name_key (string)
                                                       |# Template Id
                                                       |'00000007' # 7 (int)
                                                       |'7061636b616765' # package (string)
                                                       |'00000001' # 1 (int)
                                                       |'0000000a' # 10 (int)
                                                       |'6d6f64756c655f6b6579' # module_key (string)
                                                       |'00000001' # 1 (int)
                                                       |'00000004' # 4 (int)
                                                       |'6e616d65' # name (string)
                                                       |# Value
                                                       |'07' # Text Type Tag
                                                       |'00000005' # 5 (int)
                                                       |'68656c6c6f' # hello (string)
                                                       |'4d1741d27564ab1f1e74873034760583a1a71edaa90dbad6be3a78c0bf3eec7c' # Global Key Hash
                                                       |# Key Maintainers
                                                       |'00000001' # 1 (int)
                                                       |'00000005' # 5 (int)
                                                       |'6461766964' # david (string)""".stripMargin
  override protected val fetchNodeHash: String =
    "20437df1980bb7d4d94ad53181dc6005bae37b5990719070c48a3960e8dda3a4"
  override protected val exerciseNodeEncoding: String = s"""# Exercise Node
                                                           |# Node Version
                                                           |'00000001' # 1 (int)
                                                           |'32' # 2 (string)
                                                           |'01' # Exercise Node Tag
                                                           |# Node Seed
                                                           |'a867edafa1277f46f879ab92c373a15c2d75c5d86fec741705cee1eb01ef8c9e' # seed
                                                           |# Contract Id
                                                           |'00000021' # 33 (int)
                                                           |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                                           |# Package Name
                                                           |'0000000e' # 14 (int)
                                                           |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                                           |# Template Id
                                                           |'00000007' # 7 (int)
                                                           |'7061636b616765' # package (string)
                                                           |'00000001' # 1 (int)
                                                           |'00000006' # 6 (int)
                                                           |'6d6f64756c65' # module (string)
                                                           |'00000001' # 1 (int)
                                                           |'00000004' # 4 (int)
                                                           |'6e616d65' # name (string)
                                                           |# Signatories
                                                           |'00000001' # 1 (int)
                                                           |'00000005' # 5 (int)
                                                           |'616c696365' # alice (string)
                                                           |# Stakeholders
                                                           |'00000001' # 1 (int)
                                                           |'00000007' # 7 (int)
                                                           |'636861726c6965' # charlie (string)
                                                           |# Acting Parties
                                                           |'00000002' # 2 (int)
                                                           |'00000005' # 5 (int)
                                                           |'616c696365' # alice (string)
                                                           |'00000003' # 3 (int)
                                                           |'626f62' # bob (string)
                                                           |# Interface Id
                                                           |'01' # Some
                                                           |'00000007' # 7 (int)
                                                           |'7061636b616765' # package (string)
                                                           |'00000001' # 1 (int)
                                                           |'00000010' # 16 (int)
                                                           |'696e746572666163655f6d6f64756c65' # interface_module (string)
                                                           |'00000001' # 1 (int)
                                                           |'0000000e' # 14 (int)
                                                           |'696e746572666163655f6e616d65' # interface_name (string)
                                                           |# Choice Id
                                                           |'00000006' # 6 (int)
                                                           |'63686f696365' # choice (string)
                                                           |# Chosen Value
                                                           |'02' # Int64 Type Tag
                                                           |'0000000000007a94' # 31380 (long)
                                                           |# Consuming
                                                           |'01' # true (bool)
                                                           |# Exercise Result
                                                           |'01' # Some
                                                           |'07' # Text Type Tag
                                                           |'00000006' # 6 (int)
                                                           |'726573756c74' # result (string)
                                                           |# Choice Observers
                                                           |'00000001' # 1 (int)
                                                           |'00000005' # 5 (int)
                                                           |'6461766964' # david (string)
                                                           |# By Key
                                                           |'01' # true (bool)
                                                           |# Key
                                                           |'01' # Some
                                                           |# Key
                                                           |# Package Name
                                                           |'00000010' # 16 (int)
                                                           |'7061636b6167655f6e616d655f6b6579' # package_name_key (string)
                                                           |# Template Id
                                                           |'00000007' # 7 (int)
                                                           |'7061636b616765' # package (string)
                                                           |'00000001' # 1 (int)
                                                           |'0000000a' # 10 (int)
                                                           |'6d6f64756c655f6b6579' # module_key (string)
                                                           |'00000001' # 1 (int)
                                                           |'00000004' # 4 (int)
                                                           |'6e616d65' # name (string)
                                                           |# Value
                                                           |'07' # Text Type Tag
                                                           |'00000005' # 5 (int)
                                                           |'68656c6c6f' # hello (string)
                                                           |'4d1741d27564ab1f1e74873034760583a1a71edaa90dbad6be3a78c0bf3eec7c' # Global Key Hash
                                                           |# Key Maintainers
                                                           |'00000001' # 1 (int)
                                                           |'00000005' # 5 (int)
                                                           |'6461766964' # david (string)
                                                           |# Children
                                                           |'00000002' # 2 (int)
                                                           |'$createNodeHash' # (Hashed Inner Node)
                                                           |'$fetchNodeHash' # (Hashed Inner Node)
                                                           |""".stripMargin
  override protected val exerciseNodeHash: String =
    "516c4689acabf9d1f0087a02a7dafad884d9080c48112a57a6d20b9f72d4b697"

  override protected val rollbackNodeEncoding: String = s"""# Rollback Node
                                                           |'03' # Rollback Node Tag
                                                           |# Children
                                                           |'00000002' # 2 (int)
                                                           |'$fetchNodeHash' # (Hashed Inner Node)
                                                           |'$exerciseNodeHash' # (Hashed Inner Node)
                                                           |""".stripMargin
  override protected val rollbackNodeHash: String =
    "ea61ab05f9ab9769ab5c91493a601ccad9e784bd4c0b3bc4b7777f0da68eab4d"

  override protected val transactionEncoding: String = s"""'00000030' # Hash Purpose
                                                          |# Serialization Version
                                                          |'00000001' # 1 (int)
                                                          |'32' # 2 (string)
                                                          |# Root Nodes
                                                          |'00000002' # 2 (int)
                                                          |'$createNodeHash' # (Hashed Root Node)
                                                          |'$rollbackNodeHash' # (Hashed Root Node)
                                                          |""".stripMargin
  override protected val transactionHash: String =
    "36f0d1f4e9742a5cf54795fd1633f46b8235fafa93da006dafe10eb358d465c4"
  override protected val metadataHash: String =
    "5a4c6aa89bb80d0af05a3f0614ba07bc7469795fff66d7b9e87c876eb0137e0a"
  override protected val fullTransactionEncoding: String = s"""'00000030' # Hash Purpose
                                                              |'03' # 03 (Hashing Scheme Version)
                                                              |'$transactionHash' # Transaction
                                                              |'$metadataHash' # Metadata
                                                              |""".stripMargin

  private val queryNodeHash: String =
    "72c45c5a05a875621f16f2fe64d6c6db662ff41ee124d6c4f281409f5899b96c"
  private val queryNodeEncoding: String = """# QueryByKey Node
                                            |# Node Version
                                            |'00000001' # 1 (int)
                                            |'32' # 2 (string)
                                            |'04' # QueryByKey Node Tag
                                            |# PackageName
                                            |'0000000e' # 14 (int)
                                            |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                            |# TemplateId
                                            |'00000007' # 7 (int)
                                            |'7061636b616765' # package (string)
                                            |'00000001' # 1 (int)
                                            |'00000006' # 6 (int)
                                            |'6d6f64756c65' # module (string)
                                            |'00000001' # 1 (int)
                                            |'00000004' # 4 (int)
                                            |'6e616d65' # name (string)
                                            |# Exhaustive
                                            |'01' # true (bool)
                                            |# Key
                                            |# Key
                                            |# Package Name
                                            |'00000010' # 16 (int)
                                            |'7061636b6167655f6e616d655f6b6579' # package_name_key (string)
                                            |# Template Id
                                            |'00000007' # 7 (int)
                                            |'7061636b616765' # package (string)
                                            |'00000001' # 1 (int)
                                            |'0000000a' # 10 (int)
                                            |'6d6f64756c655f6b6579' # module_key (string)
                                            |'00000001' # 1 (int)
                                            |'00000004' # 4 (int)
                                            |'6e616d65' # name (string)
                                            |# Value
                                            |'07' # Text Type Tag
                                            |'00000005' # 5 (int)
                                            |'68656c6c6f' # hello (string)
                                            |'4d1741d27564ab1f1e74873034760583a1a71edaa90dbad6be3a78c0bf3eec7c' # Global Key Hash
                                            |# Key Maintainers
                                            |'00000001' # 1 (int)
                                            |'00000005' # 5 (int)
                                            |'6461766964' # david (string)
                                            |# Result
                                            |'00000001' # 1 (int)
                                            |'00000021' # 33 (int)
                                            |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                            |""".stripMargin

  override protected val fullTransactionHash: String =
    "db37e4e251b83196260900fd43aacf934e1893b7ac58db4c1bfa8ced432b3b84"

  "CreateNodeBuilder" should {
    "not produce collision in keys" in {
      hashCreateNode(
        createNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in keys (2)" in {
      hashCreateNode(
        createNode.copy(
          keyOpt = None
        )
      ) should !==(defaultCreateHash)
    }

    "not produce collision in version" in {
      hashCreateNode(
        // Important: hash the default create node here (without keys)
        // So that it is a valid node (a LFS V1 node with keys is invalid)
        super.createNode.copy(
          // super.defaultCreateHash is computed with SerializationVersion.V2 because that's what [[serializationVersion]]
          // is overridden to in this test, so mutating to SerializationVersion.V1 should change the hash
          version = SerializationVersion.V1
        )
      ) should !==(super.defaultCreateHash)
    }
  }

  "FetchNodeBuilder" should {
    "not produce collision in keys" in {
      hashFetchNode(
        fetchNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in keys (2)" in {
      hashFetchNode(
        fetchNode.copy(
          keyOpt = None
        )
      ) should !==(defaultFetchHash)
    }

    "not produce collision in version" in {
      hashFetchNode(
        // Important: hash the default create node here (without keys)
        // So that it is a valid node (a LFS V1 node with keys is invalid)
        super.fetchNode.copy(
          // super.defaultCreateHash is computed with SerializationVersion.V2 because that's what [[serializationVersion]]
          // is overridden to in this test, so mutating to SerializationVersion.V1 should change the hash
          version = SerializationVersion.V1
        )
      ) should !==(super.defaultFetchHash)
    }
  }

  "ExerciseNodeBuilder" should {
    "not produce collision in keys" in {
      hashExerciseNode(
        exerciseNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in keys (2)" in {
      hashExerciseNode(
        exerciseNode.copy(
          keyOpt = None
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision with byKey" in {
      hashExerciseNode(
        exerciseNode.copy(
          byKey = false
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in version" in {
      hashExerciseNode(
        // Important: hash the default create node here (without keys)
        // So that it is a valid node (a LFS V1 node with keys is invalid)
        super.exerciseNode.copy(
          // super.defaultCreateHash is computed with SerializationVersion.V2 because that's what [[serializationVersion]]
          // is overridden to in this test, so mutating to SerializationVersion.V1 should change the hash
          version = SerializationVersion.V1
        )
      ) should !==(super.exerciseNode)
    }
  }

  private lazy val defaultQueryHash: Hash = Hash
    .fromHexStringRaw(queryNodeHash)
    .getOrElse(fail("Invalid hash"))

  private val nodeSeedQuery =
    LfHash.assertFromString("926bbb6f341bc0092ae65d06c6e284028467148cc29543ef6bff0930f5d52c19")

  private def hashQueryNode(
      node: Node.QueryByKey,
      subNodes: Map[NodeId, Node] = subNodesMap,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    hashNodeWithDefaultNodeSeedMap(
      node,
      nodeSeed = Some(nodeSeedQuery),
      subNodes = subNodes,
      hashTracer = hashTracer,
    )

  "QueryNodeBuilder" should {
    "be stable" in {
      hashQueryNode(queryNode).toHexString shouldBe defaultQueryHash.toHexString
    }

    "not produce collision in package name" in {
      hashQueryNode(
        queryNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultQueryHash)
    }

    "not produce collision in template ID" in {
      hashQueryNode(
        queryNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultQueryHash)
    }

    "not produce collision in exhaustive" in {
      hashQueryNode(
        queryNode.copy(
          exhaustive = !queryNode.exhaustive
        )
      ) should !==(defaultQueryHash)
    }

    "not produce collision in keys" in {
      hashQueryNode(
        queryNode.copy(
          key = globalKey
        )
      ) should !==(defaultExerciseHash)
    }

    "not produce collision in result (1)" in {
      hashQueryNode(
        queryNode.copy(
          // Change the CID
          result = Vector(
            ContractId.V1.assertFromString(contractId2)
          )
        )
      ) should !==(defaultQueryHash)
    }

    "not produce collision in result (2)" in {
      hashQueryNode(
        queryNode.copy(
          // Add one more
          result = queryNode.result ++ Vector(
            ContractId.V1.assertFromString(contractId2)
          )
        )
      ) should !==(defaultQueryHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashQueryNode(queryNode, hashTracer = hashTracer)
      hash.toHexString shouldBe defaultQueryHash.toHexString
      hashTracer.result shouldBe queryNodeEncoding
      assertStringTracer(hashTracer, hash)
    }
  }
}
