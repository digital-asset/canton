// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v2

import com.digitalasset.canton.protocol.hash.BaseNodeHashTest
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.*

/** Hashing Scheme V2 functional correctness tests. Common tests are shared in NodeHashTest. This
  * suite only adds tests specific to V2.
  */
class NodeHashTest extends BaseNodeHashTest {

  // Only SerializationVersion.V1 is supported by HashingSchemeV2
  override protected lazy val serializationVersion: SerializationVersion = SerializationVersion.V1

  override protected val createNodeEncoding: String = """'01' # 01 (Node Encoding Version)
                                     |# Create Node
                                     |# Node Version
                                     |'00000003' # 3 (int)
                                     |'322e31' # 2.1 (string)
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
                                     |""".stripMargin

  override protected val createNodeHash: String =
    "6d2cfe58c2294000592034f4bdfe397fe246901bb8b63e3b9e041bb478e174b7"
  override protected val fetchNodeEncoding: String = """'01' # 01 (Node Encoding Version)
                                    |# Fetch Node
                                    |# Node Version
                                    |'00000003' # 3 (int)
                                    |'322e31' # 2.1 (string)
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
                                    |'626f62' # bob (string)""".stripMargin
  override protected val fetchNodeHash: String =
    "c962c6098394f3d11cd6f0c795de9517d32a8e3e1979cec76cd2f66254efc610"
  override protected val exerciseNodeEncoding: String = s"""'01' # 01 (Node Encoding Version)
                                                         |# Exercise Node
                                                         |# Node Version
                                                         |'00000003' # 3 (int)
                                                         |'322e31' # 2.1 (string)
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
                                                         |# Children
                                                         |'00000002' # 2 (int)
                                                         |'$createNodeHash' # (Hashed Inner Node)
                                                         |'$fetchNodeHash' # (Hashed Inner Node)
                                                         |""".stripMargin
  override protected val exerciseNodeHash: String =
    "070970eb4b2de72561dafb67017ca33850650a8103e5134e16044ba78991f48c"

  override protected val rollbackNodeEncoding: String = s"""'01' # 01 (Node Encoding Version)
                                                           |# Rollback Node
                                                           |'03' # Rollback Node Tag
                                                           |# Children
                                                           |'00000002' # 2 (int)
                                                           |'$fetchNodeHash' # (Hashed Inner Node)
                                                           |'$exerciseNodeHash' # (Hashed Inner Node)
                                                           |""".stripMargin
  override protected val rollbackNodeHash: String =
    "7264d5da2fd714427453bedc0d1cdb21f52ac7aec8d4bb5ac0598d25c5fcaed9"

  override protected val transactionEncoding: String = s"""'00000030' # Hash Purpose
                                                          |# Serialization Version
                                                          |'00000003' # 3 (int)
                                                          |'322e31' # 2.1 (string)
                                                          |# Root Nodes
                                                          |'00000002' # 2 (int)
                                                          |'$createNodeHash' # (Hashed Root Node)
                                                          |'$rollbackNodeHash' # (Hashed Root Node)
                                                          |""".stripMargin
  override protected val transactionHash: String =
    "154f334d24a8a5e4d0ce51ac87d93821b3256f885f21d3f779a1640abf481983"
  override protected val metadataHash: String =
    "6e89fcbcc9605179a47919b5e65a864e470e7a133f4f9f39b1e4545b223db769"
  override protected val fullTransactionEncoding: String = s"""'00000030' # Hash Purpose
                                                      |'02' # 02 (Hashing Scheme Version)
                                                      |'$transactionHash' # Transaction
                                                      |'$metadataHash' # Metadata
                                                      |""".stripMargin
  override protected val fullTransactionHash: String =
    "8c311c848db25d36b36fbd59f9483714a11688c2214e3c7cae3e028763520250"

  "Encoding" should {
    "not encode query nodes" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy {
        tryHashNode(queryNode)
      }
    }
  }

  "CreateNodeBuilder" should {
    "fail global keys" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashCreateNode(
          createNode.copy(keyOpt = Some(globalKey2))
        )
      )
    }
  }

  "FetchNodeBuilder" should {
    "fail if node includes global keys" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashFetchNode(fetchNode.copy(keyOpt = Some(globalKey2)))
      )
    }

    "fail if node includes byKey" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashFetchNode(fetchNode.copy(byKey = true))
      )
    }
  }

  "ExerciseNodeBuilder" should {
    "not include global keys" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashExerciseNode(
          exerciseNode.copy(keyOpt = Some(globalKey2))
        )
      )
    }

    "not include byKey" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashExerciseNode(
          exerciseNode.copy(byKey = true)
        )
      )
    }
  }
  override protected lazy val hashingSchemeVersion: HashingSchemeVersion = HashingSchemeVersion.V2
}
