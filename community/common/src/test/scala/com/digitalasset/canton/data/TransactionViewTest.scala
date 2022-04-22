// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.value.Value
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.canton.crypto.{HashOps, Salt}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.util.ShowUtil._
import org.scalatest.wordspec.AnyWordSpec

class TransactionViewTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val factory = new ExampleTransactionFactory()()

  val hashOps: HashOps = factory.cryptoOps

  val contractInst = ExampleTransactionFactory.contractInstance()
  val serContractInst = ExampleTransactionFactory.asSerializableRaw(contractInst)

  val createdId: LfContractId =
    ContractId.fromDiscriminator(
      ExampleTransactionFactory.lfHash(3),
      ExampleTransactionFactory.unicum(0),
    )
  val absoluteId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  val otherAbsoluteId: LfContractId = ExampleTransactionFactory.suffixedId(1, 1)
  val salt: Salt = factory.transactionSeed
  val nodeSeed = ExampleTransactionFactory.lfHash(1)
  val globalKey: LfGlobalKey =
    LfGlobalKey(LfTransactionBuilder.defaultTemplateId, Value.ValueInt64(100L))

  val defaultActionDescription =
    ActionDescription.tryFromLfActionNode(
      ExampleTransactionFactory.createNode(createdId, contractInst),
      Some(ExampleTransactionFactory.lfHash(5)),
    )

  forEvery(factory.standardHappyCases) { example =>
    s"The views of $example" when {

      forEvery(example.viewWithSubviews.zipWithIndex) { case ((view, subviews), index) =>
        s"processing $index-th view" can {
          "be folded" in {
            val foldedSubviews =
              view.foldLeft(Seq.newBuilder[TransactionView])((acc, v) => acc += v)

            foldedSubviews.result() should equal(subviews)
          }

          "be flattened" in {
            view.flatten should equal(subviews)
          }
        }
      }
    }
  }

  "A view" when {
    "a child view has the same view common data" must {
      val view = factory.SingleCreate(seed = ExampleTransactionFactory.lfHash(3)).view0
      "reject creation" in {
        TransactionView.create(hashOps)(
          view.viewCommonData,
          view.viewParticipantData,
          Seq(view),
        ) shouldEqual Left("The subview with index 0 has an equal viewCommonData.")
      }
    }
  }

  "A view common data" when {
    "a weight is not positive" must {
      "reject creation" in {
        Informee.create(ExampleTransactionFactory.signatory, -1) shouldEqual Left(
          "Unable to create a confirming party with non-positive weight -1."
        )
      }
    }
  }

  "A view participant data" when {

    def create(
        actionDescription: ActionDescription = defaultActionDescription,
        consumed: Set[LfContractId] = Set.empty,
        coreInputs: Map[LfContractId, SerializableContract] = Map.empty,
        createdIds: Seq[LfContractId] = Seq(createdId),
        archivedInSubviews: Set[LfContractId] = Set.empty,
        resolvedKeys: Map[LfGlobalKey, KeyResolution] = Map.empty,
    ): Either[String, ViewParticipantData] = {

      val created = createdIds.map { id =>
        val serializable = ExampleTransactionFactory.asSerializable(
          id,
          contractInstance = ExampleTransactionFactory.contractInstance(),
          metadata = ContractMetadata.empty,
        )
        CreatedContract.tryCreate(serializable, consumed.contains(id), rolledBack = false)
      }
      val coreInputs2 = coreInputs.transform { (id, contract) =>
        InputContract(contract, consumed.contains(id))
      }

      ViewParticipantData.create(hashOps)(
        coreInputs2,
        created,
        archivedInSubviews,
        resolvedKeys,
        actionDescription,
        RollbackContext.empty,
        salt,
      )
    }

    "a contract is created twice" must {
      "reject creation" in {
        create(createdIds = Seq(createdId, createdId)).left.value should
          startWith regex "createdCore contains the contract id .* multiple times at indices 0, 1"
      }
    }
    "a used contract has an inconsistent id" must {
      "reject creation" in {
        val usedContract =
          ExampleTransactionFactory.asSerializable(
            otherAbsoluteId,
            metadata = ContractMetadata.empty,
          )

        create(coreInputs = Map(absoluteId -> usedContract)).left.value should startWith(
          "Inconsistent ids for used contract: "
        )
      }
    }
    "an overlap between archivedInSubview and coreCreated" must {
      "reject creation" in {
        create(
          createdIds = Seq(createdId),
          archivedInSubviews = Set(createdId),
        ).left.value should startWith(
          "Contract created in a subview are also created in the core: "
        )
      }
    }
    "an overlap between archivedInSubview and coreInputs" must {
      "reject creation" in {
        val usedContract =
          ExampleTransactionFactory.asSerializable(absoluteId, metadata = ContractMetadata.empty)

        create(
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(absoluteId),
        ).left.value should startWith("Contracts created in a subview overlap with core inputs: ")
      }
    }
    "the created contract of the root action is not declared first" must {
      "reject creation" in {
        create(createdIds = Seq.empty).left.value should startWith(
          "No created core contracts declared for a view that creates contract"
        )

        val otherCantonId =
          ContractId.fromDiscriminator(
            ExampleTransactionFactory.lfHash(3),
            ExampleTransactionFactory.unicum(1),
          )
        create(createdIds = Seq(otherCantonId, createdId)).left.value should startWith(
          show"View with root action Create $createdId declares $otherCantonId as first created core contract."
        )
      }
    }
    "the used contract of the root action is not declared" must {
      "reject creation" in {
        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.exerciseNodeWithoutChildren(absoluteId),
            Some(nodeSeed),
          )
        ).left.value should startWith(
          show"Input contract $absoluteId of the Exercise root action is not declared as core input."
        )

        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.fetchNode(
              absoluteId,
              Set(ExampleTransactionFactory.submitter),
            ),
            None,
          )
        ).left.value should startWith(
          show"Input contract $absoluteId of the Fetch root action is not declared as core input."
        )

        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.lookupByKeyNode(
              globalKey,
              maintainers = Set(ExampleTransactionFactory.submitter),
            ),
            None,
          )
        ).left.value should startWith(
          show"Key $globalKey of LookupByKey root action is not resolved."
        )

      }
    }

    "deserialized" must {
      "reconstruct the original view participant data" in {
        val usedContract =
          ExampleTransactionFactory.asSerializable(
            absoluteId,
            metadata = ContractMetadata.tryCreate(
              Set.empty,
              Set.empty,
              Some(ExampleTransactionFactory.globalKeyWithMaintainers()),
            ),
          )

        val vpd = create(
          consumed = Set(absoluteId),
          createdIds = Seq(createdId),
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(otherAbsoluteId),
          resolvedKeys = Map(
            ExampleTransactionFactory.defaultGlobalKey ->
              AssignedKey(absoluteId)(ExampleTransactionFactory.transactionVersion)
          ),
        ).value

        ViewParticipantData
          .fromByteString(hashOps)(vpd.getCryptographicEvidence)
          .map(_.unwrap) shouldBe Right(Right(vpd))
      }
    }
  }
}
