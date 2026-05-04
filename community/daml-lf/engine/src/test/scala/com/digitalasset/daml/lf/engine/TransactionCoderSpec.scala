// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package engine

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageName, Party}
import com.digitalasset.daml.lf.engine.TransactionCoderSpec.{
  absCid,
  dummyPackageName,
  externalCallResult,
  normalizeCreate,
  normalizeExe,
  normalizeFetch,
  normalizeNode,
  normalizeQueryByKey,
  updateVersion,
  versionInIncreasingOrder,
  versionInStrictIncreasingOrder,
}
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, ExternalCallResult, Node, NodeId, SerializationVersion, TransactionOuterClass => proto, Util, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.google.protobuf.ByteString
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.Ordering.Implicits.infixOrderingOps

final class TransactionCoderSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  // TODO https://github.com/digital-asset/daml/issues/18457
  // Tests that messages with unknown field are rejected

  import com.digitalasset.daml.lf.value.test.ValueGenerators.*

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, sizeRange = 10)

  private[this] val serializationVersions =
    Table("serialization version", SerializationVersion.All*)

  "encode-decode" should {

    "do Node.Create" in {
      forAll(for {
        (nodeVersion, txVersion) <- versionInIncreasingOrder()
        createNode <- malformedCreateNodeGenWithVersion(nodeVersion)
      } yield (createNode, txVersion)) { case (createNode, txVersion) =>
        try {
          val versionedNode = normalizeCreate(createNode)
          val Right(encodedNode) = TransactionCoder.internal.encodeNode(
            enclosingVersion = txVersion,
            nodeId = NodeId(0),
            node = versionedNode,
          )

          TransactionCoder.internal.decodeNode(txVersion, encodedNode) shouldBe Right(
            (NodeId(0), versionedNode)
          )
        } catch {
          case scala.util.control.NonFatal(e) =>
            val x = e.getStackTrace
            x.foreach(x => println(x.toString))
            throw e
        }
      }
    }

    "do Node.Fetch" in {
      forAll(for {
        (nodeVersion, txVersion) <- versionInIncreasingOrder()
        fetchNode <- fetchNodeGenWithVersion(nodeVersion)
      } yield (fetchNode, txVersion)) { case (fetchNode, txVersion) =>
        val versionedNode = normalizeFetch(fetchNode)
        val encodedNode =
          TransactionCoder.internal
            .encodeNode(
              enclosingVersion = txVersion,
              nodeId = NodeId(0),
              node = versionedNode,
            )
            .toOption
            .get
        TransactionCoder.internal.decodeNode(txVersion, encodedNode) shouldBe Right(
          (NodeId(0), versionedNode)
        )
      }
    }

    "do Node.Exercise" in {
      forAll(for {
        (nodeVersion, txVersion) <- versionInIncreasingOrder()
        exerciseNode <- danglingRefExerciseNodeGenWithVersion(nodeVersion)
      } yield (exerciseNode, txVersion)) { case (exerciseNode, txVersion) =>
        val normalizedNode = normalizeExe(exerciseNode)
        val Right(encodedNode) =
          TransactionCoder.internal
            .encodeNode(
              enclosingVersion = txVersion,
              nodeId = NodeId(0),
              node = normalizedNode,
            )
        TransactionCoder.internal.decodeNode(txVersion, encodedNode) shouldBe Right(
          (NodeId(0), normalizedNode)
        )
      }
    }

    "reject Node.Exercise with external call results" in {
      forAll(danglingRefExerciseNodeGen) { exerciseNode =>
        val normalizedNode = normalizeExe(exerciseNode).copy(
          version = SerializationVersion.VDev,
          externalCallResults = ImmArray(externalCallResult()),
        )

        TransactionCoder.internal.encodeNode(
          enclosingVersion = SerializationVersion.VDev,
          nodeId = NodeId(0),
          node = normalizedNode,
        ) shouldBe Left(
          EncodeError("external call results are not supported by transaction encoding")
        )
      }
    }

    "do Node.Rollback" in {
      forAll(danglingRefRollbackNodeGen) { node =>
        forEvery(serializationVersions) { txVersion =>
          val normalizedNode = normalizeNode(node)
          val Right(encodedNode) =
            TransactionCoder.internal
              .encodeNode(
                enclosingVersion = txVersion,
                nodeId = NodeId(0),
                node = normalizedNode,
              )
          TransactionCoder.internal.decodeNode(txVersion, encodedNode) shouldBe Right(
            (NodeId(0), normalizedNode)
          )
        }
      }
    }

    "do Node.QueryByKey" in {
      forAll(for {
        (nodeVersion, txVersion) <- versionInIncreasingOrder(versions =
          List(SerializationVersion.V2, SerializationVersion.VDev)
        )
        queryByKeyNode <- queryByKeyNodeGenWithVersion(nodeVersion)
      } yield (nodeVersion, txVersion, queryByKeyNode)) {
        case (nodeVersion, txVersion, queryByKeyNode) =>
          val versionedNode = normalizeQueryByKey(queryByKeyNode.updateVersion(nodeVersion))
          val Right(encodedNode) =
            TransactionCoder.internal
              .encodeNode(
                enclosingVersion = txVersion,
                nodeId = NodeId(0),
                node = versionedNode,
              )
          TransactionCoder.internal.decodeNode(txVersion, encodedNode) shouldBe Right(
            (NodeId(0), versionedNode)
          )
      }
    }

    "do transactions" in
      forAll(noDanglingRefGenVersionedTransaction, minSuccessful(50)) { tx =>
        val tx2 = VersionedTransaction(
          tx.version,
          tx.nodes.transform((_, node) => normalizeNode(node)),
          tx.roots,
        )
        inside(TransactionCoder.encodeTransaction(tx2)) {
          case Left(EncodeError(msg)) =>
            // fuzzy sort of "failed because of the version override" test
            msg should include(tx2.version.pretty)
          case Right(encodedTx) =>
            val decodedVersionedTx = assertRight(
              TransactionCoder.decodeTransaction(encodedTx)
            )
            decodedVersionedTx shouldBe tx2
        }
      }

    "transactions decoding should fail when unsupported serialization version received" in
      forAll(noDanglingRefGenTransaction, minSuccessful(30)) { tx =>
        forAll(stringVersionGen, minSuccessful(10)) { badTxVer =>
          whenever(SerializationVersion.fromString(badTxVer).isLeft) {
            val encodedTxWithBadTxVer: proto.Transaction = assertRight(
              TransactionCoder
                .encodeTransaction(
                  VersionedTransaction(
                    SerializationVersion.VDev,
                    tx.nodes.view.mapValues(updateVersion(_, SerializationVersion.VDev)).toMap,
                    tx.roots,
                  )
                )
            ).toBuilder.setVersion(badTxVer).build()

            encodedTxWithBadTxVer.getVersion shouldEqual badTxVer

            TransactionCoder.decodeTransaction(
              protoTx = encodedTxWithBadTxVer
            ) shouldEqual Left(
              DecodeError(s"Unsupported serialization version '$badTxVer'")
            )
          }
        }
      }

    "do tx with a lot of root nodes" in {
      val version = SerializationVersion.StableVersions.max
      val node =
        Node.Create(
          coid = absCid("#test-cid"),
          packageName = dummyPackageName,
          templateId = Identifier.assertFromString("pkg-id:Test:Name"),
          arg = Value.ValueParty(Party.assertFromString("francesco")),
          signatories = Set(Party.assertFromString("alice")),
          stakeholders = Set(Party.assertFromString("alice"), Party.assertFromString("bob")),
          keyOpt = None,
          version = version,
        )

      forEvery(serializationVersions) { version =>
        val versionedNode = node.updateVersion(version)
        val roots = ImmArray.ImmArraySeq.range(0, 10000).map(NodeId(_)).toImmArray
        val nodes = roots.iterator.map(nid => nid -> versionedNode).toMap
        val tx = VersionedTransaction(
          version,
          nodes = nodes.view.mapValues(updateVersion(_, version)).toMap,
          roots = roots,
        )

        val decoded = TransactionCoder
          .decodeTransaction(
            protoTx = TransactionCoder
              .encodeTransaction(
                tx = tx
              )
              .toOption
              .get
          )
          .toOption
          .get
        tx shouldEqual decoded
      }
    }
  }

  "encodeVersionedNode" should {

    "fail if try to encode a node in a version newer than the transaction" in {

      forAll(danglingRefGenActionNode, versionInStrictIncreasingOrder(), minSuccessful(10)) {
        case ((nodeId, node), (txVersion, nodeVersion)) =>
          val normalizedNode = updateVersion(node, nodeVersion)

          TransactionCoder.internal
            .encodeNode(
              enclosingVersion = txVersion,
              nodeId = nodeId,
              node = normalizedNode,
            ) shouldBe Symbol("left")
      }
    }
  }

  "decodeNode" should {

    "succeed as expected when the node is encoded with a version older than the serialization version" in {

      val gen = for {
        ver <- versionInIncreasingOrder(SerializationVersion.All)
        (nodeVersion, _) = ver
        node <- danglingRefGenActionNodeWithVersion(nodeVersion)
      } yield (ver, node)

      forAll(gen, minSuccessful(5)) { case ((nodeVersion, txVersion), (nodeId, node)) =>
        val normalizedNode = updateVersion(node, nodeVersion)

        val Right(encoded) = TransactionCoder.internal
          .encodeNode(
            enclosingVersion = nodeVersion,
            nodeId = nodeId,
            node = normalizedNode,
          )

        TransactionCoder.internal.decodeNode(txVersion, encoded) shouldBe Right(
          (nodeId, normalizedNode)
        )
      }
    }

    "fail when the node is encoded with a version newer than the serialization version" in {

      forAll(
        danglingRefGenActionNode,
        versionInStrictIncreasingOrder(SerializationVersion.All),
        minSuccessful(5),
      ) { case ((nodeId, node), (v1, v2)) =>
        val normalizedNode = updateVersion(node, v2)

        val Right(encoded) = TransactionCoder.internal
          .encodeNode(
            enclosingVersion = v2,
            nodeId = nodeId,
            node = normalizedNode,
          )

        TransactionCoder.internal.decodeNode(v1, encoded) shouldBe a[Left[?, ?]]
      }
    }

    "fail if try to decode a node in a version newer than the enclosing Transaction message version" in {

      val gen = for {
        ver <- versionInStrictIncreasingOrder(SerializationVersion.All)
        (_, nodeVersion) = ver
        node <- danglingRefGenActionNodeWithVersion(nodeVersion)
      } yield (ver, node)

      forAll(gen) { case ((txVersion, nodeVersion), (nodeId, node)) =>
        val normalizedNode = updateVersion(node, nodeVersion)

        val Right(encoded) = TransactionCoder.internal
          .encodeNode(
            enclosingVersion = nodeVersion,
            nodeId = nodeId,
            node = normalizedNode,
          )

        TransactionCoder.internal.decodeNode(txVersion, encoded) shouldBe a[Left[?, ?]]
      }
    }

    "reject exercise nodes with external call results" in {
      forAll(danglingRefExerciseNodeGen) { exerciseNode =>
        val normalizedNode = normalizeExe(exerciseNode).copy(
          version = SerializationVersion.VDev,
          externalCallResults = ImmArray.Empty,
        )

        val Right(encoded) = TransactionCoder.internal
          .encodeNode(
            enclosingVersion = SerializationVersion.VDev,
            nodeId = NodeId(0),
            node = normalizedNode,
          )

        val withExternalCallResultsBuilder = encoded.toBuilder
        withExternalCallResultsBuilder
          .getExerciseBuilder
          .addExternalCallResults(
            proto.ExternalCallResult
              .newBuilder()
              .setExtensionId("ext")
              .setFunctionId("fn")
              .setConfig(ByteString.copyFromUtf8("cfg"))
              .setInput(ByteString.copyFromUtf8("in"))
              .setOutput(ByteString.copyFromUtf8("out"))
              .build()
          )
        val withExternalCallResults = withExternalCallResultsBuilder.build()

        TransactionCoder.internal.decodeNode(
          SerializationVersion.VDev,
          withExternalCallResults,
        ) shouldBe Left(
          DecodeError("external call results are not supported by transaction decoding")
        )
      }
    }
  }

}

private object TransactionCoderSpec {

  private def absCid(s: String): ContractId =
    ContractId.V1(Hash.hashPrivateKey(s))

  def versionNodes(
      version: SerializationVersion,
      nodes: Map[NodeId, Node],
  ): Map[NodeId, Node] =
    nodes.view.mapValues(updateVersion(_, version)).toMap

  private def versionInIncreasingOrder(
      versions: Seq[SerializationVersion] = SerializationVersion.All
  ): Gen[(SerializationVersion, SerializationVersion)] =
    for {
      v1 <- Gen.oneOf(versions)
      v2 <- Gen.oneOf(versions.filter(_ >= v1))
    } yield (v1, v2)

  private def versionInStrictIncreasingOrder(
      versions: Seq[SerializationVersion] = SerializationVersion.All
  ): Gen[(SerializationVersion, SerializationVersion)] =
    for {
      v1 <- Gen.oneOf(versions.dropRight(1))
      v2 <- Gen.oneOf(versions.filter(_ > v1))
    } yield (v1, v2)

  private def normalizeNode(node: Node) =
    node match {
      case rb: Node.Rollback => rb // nothing to normalize
      case exe: Node.Exercise => normalizeExe(exe)
      case fetch: Node.Fetch => normalizeFetch(fetch)
      case create: Node.Create => normalizeCreate(create)
      case queryByKey: Node.QueryByKey => normalizeQueryByKey(queryByKey)
    }

  private val dummyPackageName = PackageName.assertFromString("package-name")

  private def normalizeCreate(
      create: Node.Create
  ): Node.Create = {
    val maintainers = create.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories0 = create.signatories ++ maintainers
    val signatories =
      if (signatories0.isEmpty) Set(Party.assertFromString("alice")) else signatories0
    val stakeholders = signatories ++ create.stakeholders
    create.copy(
      signatories = signatories0,
      stakeholders = stakeholders,
      arg = normalize(create.arg, create.version),
      keyOpt = create.keyOpt.map(normalizeKey(_, create.version)),
    )
  }

  private def normalizeFetch(fetch: Node.Fetch) = {
    val maintainers = fetch.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories = fetch.signatories ++ maintainers
    val stakeholders = fetch.stakeholders ++ signatories
    fetch.copy(
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = fetch.keyOpt.map(normalizeKey(_, fetch.version)),
      byKey =
        if (fetch.version >= SerializationVersion.minContractKeys)
          fetch.byKey
        else false,
    )
  }

  private def normalizeExe(exe: Node.Exercise) = {
    val maintainers = exe.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories = exe.signatories ++ maintainers
    val stakeholders = exe.stakeholders ++ signatories
    exe.copy(
      signatories = signatories,
      stakeholders = stakeholders,
      interfaceId = exe.interfaceId,
      chosenValue = normalize(exe.chosenValue, exe.version),
      exerciseResult = exe.exerciseResult.map(normalize(_, exe.version)),
      choiceObservers = exe.choiceObservers,
      choiceAuthorizers = exe.choiceAuthorizers match {
        case Some(_) if exe.version <= SerializationVersion.minChoiceAuthorizers => None
        case Some(parties) if parties.isEmpty => None
        case otherwise => otherwise
      },
      keyOpt = exe.keyOpt.map(normalizeKey(_, exe.version)),
      externalCallResults = ImmArray.Empty,
      byKey =
        if (exe.version >= SerializationVersion.minContractKeys)
          exe.byKey
        else false,
    )
  }

  private def externalCallResult() =
    ExternalCallResult(
      extensionId = "ext",
      functionId = "fn",
      config = data.Bytes.fromByteArray(Array[Byte](1, 2, 3)),
      input = data.Bytes.fromByteArray(Array[Byte](4, 5, 6)),
      output = data.Bytes.fromByteArray(Array[Byte](7, 8, 9)),
    )

  private def normalizeKey(
      key: GlobalKeyWithMaintainers,
      version: SerializationVersion,
  ) =
    key.copy(globalKey =
      GlobalKey.assertBuild(
        key.globalKey.templateId,
        key.globalKey.packageName,
        normalize(key.value, version),
        key.globalKey.hash,
      )
    )

  private def normalizeQueryByKey(queryByKey: Node.QueryByKey): Node.QueryByKey =
    queryByKey.copy(
      key = normalizeKey(queryByKey.key, queryByKey.version)
    )

  private def normalize(
      value0: Value,
      version: SerializationVersion,
  ): Value = Util.assertNormalizeValue(value0, version)

  private def updateVersion(
      node: Node,
      version: SerializationVersion,
  ): Node = node match {
    case node: Node.Action => normalizeNode(node.updateVersion(version))
    case node: Node.Rollback => node
  }

}
