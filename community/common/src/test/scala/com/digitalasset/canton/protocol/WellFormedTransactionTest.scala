// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{Stage, WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.util.LfTransactionBuilder.defaultTemplateId
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPackageId,
  LfPackageName,
  LfPartyId,
}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.NodeOps
import com.digitalasset.daml.lf.transaction.test.{TestIdFactory, TreeTransactionBuilder}
import com.digitalasset.daml.lf.transaction.{NodeId, SerializationVersion}
import com.digitalasset.daml.lf.value.Value.VersionedValue
import org.scalatest.prop.{TableFor3, TableFor4}
import org.scalatest.wordspec.AnyWordSpec

class WellFormedTransactionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  val lfAbs: LfContractId = suffixedId(0, 0)

  val contractArg: VersionedValue = ExampleTransactionFactory.defaultVersionedValue

  def createNode(
      cid: LfContractId,
      arg: VersionedValue = ExampleTransactionFactory.defaultVersionedValue,
      signatories: Set[LfPartyId] = Set(signatory),
      key: Option[LfGlobalKeyWithMaintainers] = None,
      packageId: LfPackageId = defaultTemplateId.packageId,
  ): LfNodeCreate =
    ExampleTransactionFactory.createNode(
      cid,
      signatories = signatories,
      arg = arg,
      key = key,
      templateId = Ref.Identifier(packageId, defaultTemplateId.qualifiedName),
    )

  def fetchNode(
      cid: LfContractId,
      packageId: LfPackageId = defaultTemplateId.packageId,
  ): LfNodeFetch =
    ExampleTransactionFactory.fetchNode(
      cid,
      actingParties = Set(submitter),
      signatories = Set(signatory),
      templateId = Ref.Identifier(packageId, defaultTemplateId.qualifiedName),
    )

  def exerciseNode(
      cid: LfContractId,
      children: Seq[Int] = Seq.empty,
      packageId: LfPackageId = defaultTemplateId.packageId,
  ): LfNodeExercises =
    ExampleTransactionFactory.exerciseNode(
      cid,
      children = children.toList.map(nodeId),
      signatories = Set(signatory),
      templateId = Ref.Identifier(packageId, defaultTemplateId.qualifiedName),
    )

  def queryByKeyNode(
      cids: Seq[LfContractId],
      packageId: LfPackageId = defaultTemplateId.packageId,
  ): LfNodeQueryByKey =
    ExampleTransactionFactory.queryByKeyNode(
      defaultGlobalKey.copy(templateId =
        Ref.Identifier(packageId, defaultTemplateId.qualifiedName)
      ),
      maintainers = Set(signatory),
      resolution = cids.toVector,
    )

  private def nid(i: Int): String = """NodeId\(""" + i.toString + """.*\)"""

  val malformedExamples
      : TableFor4[String, (LfVersionedTransaction, TransactionMetadata), Stage, String] =
    Table(
      ("Description", "Transaction and seeds", "Suffixing", "Expected Error Message"),
      (
        "Orphaned node",
        factory.versionedTransactionWithSeeds(Seq.empty, fetchNode(lfAbs)),
        WithSuffixes,
        "OrphanedNode: 0",
      ),
      (
        "Dangling root node",
        factory.versionedTransactionWithSeeds(Seq(0)),
        WithSuffixes,
        "DanglingNodeId: 0",
      ),
      (
        "Dangling exercise child",
        factory.versionedTransactionWithSeeds(Seq(0), exerciseNode(lfAbs, children = List(1))),
        WithSuffixes,
        "DanglingNodeId: 1",
      ),
      (
        "Cycle",
        factory.versionedTransactionWithSeeds(Seq(0), exerciseNode(lfAbs, children = List(0))),
        WithSuffixes,
        "AliasedNode: 0",
      ),
      (
        "Two parents",
        factory.versionedTransactionWithSeeds(Seq(0, 0), fetchNode(lfAbs)),
        WithSuffixes,
        "AliasedNode: 0",
      ),
      (
        "byKey node with no key",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          ExampleTransactionFactory.fetchNode(
            unsuffixedId(0),
            actingParties = Set(submitter),
            signatories = Set(signatory),
            byKey = true,
          ),
        ),
        WithoutSuffixes,
        "byKey nodes without a key: 0",
      ),
      (
        "Missing seed for create",
        (transaction(Seq(0), createNode(unsuffixedId(0))), factory.mkMetadata()),
        WithoutSuffixes,
        "Nodes without seeds: 0",
      ),
      (
        "Missing seed for exercise",
        (
          transaction(
            Seq(0),
            ExampleTransactionFactory.exerciseNode(lfAbs, signatories = Set(signatory)),
          ),
          factory.mkMetadata(),
        ),
        WithoutSuffixes,
        "Nodes without seeds: 0",
      ),
      (
        "Superfluous seed for fetch",
        (
          transaction(Seq(0), fetchNode(lfAbs)),
          factory.mkMetadata(Map(LfNodeId(0) -> ExampleTransactionFactory.lfHash(0))),
        ),
        WithSuffixes,
        "Nodes with superfluous seeds: 0",
      ),
      (
        "Duplicate create",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(unsuffixedId(0)),
          createNode(unsuffixedId(0)),
        ),
        WithoutSuffixes,
        s"Contract id ${unsuffixedId(0).coid} is created in nodes ${nid(0)} and ${nid(1)}",
      ),
      (
        "Create shadows previously referenced id",
        factory.versionedTransactionWithSeeds(Seq(0, 1), fetchNode(lfAbs), createNode(lfAbs)),
        WithSuffixes,
        s"Contract id ${lfAbs.coid} created in node ${nid(1)} is referenced before in ${nid(0)}",
      ),
      (
        "Missing signatory",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(unsuffixedId(0)).copy(signatories = Set.empty),
        ),
        WithoutSuffixes,
        "neither signatories nor maintainers present at nodes 0",
      ),
      (
        "Signatory not declared as informee",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(unsuffixedId(0)).copy(stakeholders = Set.empty),
        ),
        WithoutSuffixes,
        "signatory or maintainer not declared as informee: signatory::default at node 0",
      ),
      (
        "Missing fetch actors",
        factory
          .versionedTransactionWithSeeds(Seq(0), fetchNode(lfAbs).copy(actingParties = Set.empty)),
        WithSuffixes,
        "fetch nodes with unspecified acting parties at nodes 0",
      ),
      (
        "Failure to serialize - depth limit exceeded",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(unsuffixedId(0), arg = ExampleTransactionFactory.veryDeepVersionedValue),
          LfNodeExercises(
            targetCoid = suffixedId(2, -1),
            packageName = packageName,
            templateId = templateId,
            interfaceId = None,
            choiceId = LfChoiceName.assertFromString("choice"),
            consuming = false,
            actingParties = Set(ExampleTransactionFactory.submitter),
            chosenValue = ExampleTransactionFactory.veryDeepValue,
            stakeholders = Set(ExampleTransactionFactory.submitter),
            signatories = Set(ExampleTransactionFactory.submitter),
            choiceObservers = Set.empty,
            choiceAuthorizers = None,
            children = ImmArray.empty,
            exerciseResult = None,
            keyOpt = None,
            byKey = false,
            externalCallResults = ImmArray.empty,
            version = ExampleTransactionFactory.serializationVersion,
          ),
        ),
        WithoutSuffixes,
        "unable to encode value for NodeId.1.: Provided Daml-LF value to encode exceeds maximum nesting level of 100",
      ),
      (
        "Failure to parse party id",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(unsuffixedId(0), signatories = Set(LfPartyId.assertFromString("bubu"))),
        ),
        WithoutSuffixes,
        """Unable to parse party: Invalid unique identifier `bubu` .*""",
      ),
      (
        "Empty maintainers",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(
            unsuffixedId(1),
            signatories = Set(signatory),
            key = Some(
              LfGlobalKeyWithMaintainers(
                templateId,
                contractArg.unversioned,
                crypto.Hash.hashPrivateKey(contractArg.unversioned.toString),
                Set.empty,
                LfPackageName.assertFromString("package-name"),
              )
            ),
          ),
          ExampleTransactionFactory.exerciseNode(
            lfAbs,
            signatories = Set(signatory),
            actingParties = Set(signatory),
            key = Some(
              LfGlobalKeyWithMaintainers(
                templateId,
                contractArg.unversioned,
                crypto.Hash.hashPrivateKey(contractArg.unversioned.toString),
                Set.empty,
                packageName,
              )
            ),
          ),
        ),
        WithoutSuffixes,
        s"Key of node 0 has no maintainer, Key of node 1 has no maintainer",
      ),
    )

  // Well-formed transactions are mostly covered by ExampleTransactionFactoryTest. So we test only a special cases here.
  val wellFormedExamples: TableFor3[String, (LfVersionedTransaction, TransactionMetadata), Stage] =
    Table(
      ("Description", "Transaction and seeds", "Suffixing"),
      (
        "Suffixed discriminators need not be fresh",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1, 2),
          fetchNode(suffixedId(0, -1)),
          createNode(suffixedId(0, 0)),
          fetchNode(unsuffixedId(0)),
        ),
        WithSuffixes,
      ),
    )

  "A transaction" when {
    malformedExamples.forEvery {
      case (description, (transaction, metadata), state, expectedError) =>
        description must {
          "be reported as malformed" in {
            WellFormedTransaction
              .check(transaction, metadata, state)
              .left
              .value should fullyMatch regex expectedError
            an[IllegalArgumentException] must be thrownBy
              WellFormedTransaction.checkOrThrow(
                transaction,
                metadata,
                WithoutSuffixes,
              )
          }
        }
    }

    wellFormedExamples.forEvery { case (description, (transaction, metadata), state) =>
      description must {
        "be accepted as well-formed" in {
          WellFormedTransaction
            .check(transaction, metadata, state)
            .value shouldBe a[WellFormedTransaction[?]]
        }
      }
    }

    "node ids are not normalized" should {
      "be normalized" in {
        val tx = LfVersionedTransaction(
          SerializationVersion.V1,
          Map(
            NodeId(-3) -> createNode(unsuffixedId(0)),
            NodeId(0) -> exerciseNode(unsuffixedId(0), List(10)),
            NodeId(10) -> fetchNode(lfAbs),
          ),
          ImmArray.from(Seq(NodeId(-3), NodeId(0))),
        )
        val expectedTx = LfVersionedTransaction(
          SerializationVersion.V1,
          Map(
            NodeId(0) -> createNode(unsuffixedId(0)),
            NodeId(1) -> exerciseNode(unsuffixedId(0), List(2)),
            NodeId(2) -> fetchNode(lfAbs),
          ),
          ImmArray.from(Seq(NodeId(0), NodeId(1))),
        )

        val metadata = factory.mkMetadata(Map(NodeId(-3) -> lfHash(0), NodeId(0) -> lfHash(1)))
        val expectedMetadata =
          factory.mkMetadata(Map(NodeId(0) -> lfHash(0), NodeId(1) -> lfHash(1)))

        val wfTx = WellFormedTransaction
          .check(tx, metadata, WithoutSuffixes)
          .value
        wfTx.unwrap shouldBe expectedTx
        wfTx.metadata shouldBe expectedMetadata
      }
    }

    "validation packages are required" should {
      "provide packages" in {

        val idFactory = new TestIdFactory {}

        def nextSuffixedCid(): LfContractId = idFactory.newCid.suffixCid(_.bytes, identity).value
        def nextPackageId(): PackageId = idFactory.newPackageId

        val create1 = createNode(nextSuffixedCid(), packageId = nextPackageId())
        val exercise1 = exerciseNode(nextSuffixedCid(), packageId = nextPackageId())
        val exercise2 = exerciseNode(exercise1.targetCoid, packageId = nextPackageId())
        val exercise3 = exerciseNode(nextSuffixedCid(), packageId = nextPackageId())
        val fetch1 = fetchNode(nextSuffixedCid(), packageId = nextPackageId())
        val queryByKey1 =
          queryByKeyNode(Seq(nextSuffixedCid(), nextSuffixedCid()), packageId = nextPackageId())

        val tx = TreeTransactionBuilder.toVersionedTransaction(
          create1,
          exercise1,
          exercise2.withChildren(exercise3, fetch1),
          fetch1,
          queryByKey1,
        )

        val metadata = factory.mkMetadata(inventSeeds(tx))

        val wfTx = WellFormedTransaction
          .check(tx, metadata, WithSuffixes)
          .value

        wfTx.validatedContractPackageIds shouldBe Map(
          create1.coid -> Set(create1.templateId.packageId),
          exercise1.targetCoid -> Set(
            exercise1.templateId.packageId,
            exercise2.templateId.packageId,
          ),
          exercise3.targetCoid -> Set(
            exercise3.templateId.packageId
          ),
          fetch1.coid -> Set(
            fetch1.templateId.packageId
          ),
        ) ++ queryByKey1.cids.map(_ -> Set(queryByKey1.templateId.packageId)).toMap
      }
    }

  }
}
