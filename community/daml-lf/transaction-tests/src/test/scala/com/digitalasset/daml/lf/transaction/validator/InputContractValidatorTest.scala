// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package validator

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value as V
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class InputContractValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  val tx = SubmittedTransaction(
    VersionedTransaction(
      version,
      HashMap.empty,
      ImmArray.empty,
    )
  )
  val createNode1 = Node.Create(
    coid = V.ContractId.V1(Hash.hashPrivateKey("#cid1")),
    packageName = Ref.PackageName.assertFromString("PkgName"),
    templateId = "DummyModule:dummyName",
    arg = V.ValueUnit,
    stakeholders = Set(Ref.Party.assertFromString("Alice")),
    signatories = Set(Ref.Party.assertFromString("Alice")),
    keyOpt = None,
    version = version,
  )
  val createNode2 = Node.Create(
    coid = V.ContractId.V1(Hash.hashPrivateKey("#cid2")),
    packageName = Ref.PackageName.assertFromString("PkgName"),
    templateId = "DummyModule:dummyName",
    arg = V.ValueUnit,
    stakeholders = Set(Ref.Party.assertFromString("Bob")),
    signatories = Set(Ref.Party.assertFromString("Bob")),
    keyOpt = None,
    version = version,
  )

  "limit to 1 input contract" - {
    val limits = Limits.Lenient.copy(transactionInputContracts = 1)

    "allow a transaction with no input contracts" in {
      val inputContracts = Map.empty[V.ContractId, FatContractInstance]

      InputContractValidator(metadata, inputContracts, limits).validate(tx) shouldBe empty
    }

    "allow a transaction with one input contract" in {
      val inputContracts = Map[V.ContractId, FatContractInstance](
        createNode1.coid -> FatContractInstance.fromCreateNode(
          createNode1,
          CreationTime.assertDecode(0),
          Bytes.Empty,
        )
      )

      InputContractValidator(metadata, inputContracts, limits).validate(tx) shouldBe empty
    }

    "disallow a transaction with two input contracts" in {
      val inputContracts = Map[V.ContractId, FatContractInstance](
        createNode1.coid -> FatContractInstance
          .fromCreateNode(createNode1, CreationTime.assertDecode(0), Bytes.Empty),
        createNode2.coid -> FatContractInstance.fromCreateNode(
          createNode2,
          CreationTime.assertDecode(0),
          Bytes.Empty,
        ),
      )

      InputContractValidator(metadata, inputContracts, limits).validate(tx) shouldBe Some(
        Limit.TransactionInputContracts(limits.transactionInputContracts)
      )
    }
  }
}
