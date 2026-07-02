// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import com.daml.ledger.api.v2.interactive.interactive_submission_service.DamlTransaction.Node.VersionedNode
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data as isdv1
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data.Node.NodeType
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset
import com.digitalasset.canton.data.LedgerTimeBoundaries
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.ledger.participant.state.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.EnrichedTransactionData.ExternalInputContract
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.PreparedTransactionCodec.*
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.{
  PrepareTransactionData,
  PreparedTransactionDecoder,
  PreparedTransactionEncoder,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, GeneratorsTopology, Synchronizer}
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  GeneratorsLf,
  HasExecutionContext,
  LedgerCommandId,
  LedgerSubmissionId,
  LedgerUserId,
  LfTimestamp,
}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  ExternalCallResult,
  FatContractInstance,
  Node,
  NodeId,
  SerializationVersion as LfSerializationVersion,
  SubmittedTransaction,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.test.ValueGenerators
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.UUID

class PreparedTransactionCodecV1Spec
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with ScalaCheckPropertyChecks
    with HasExecutionContext {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val decoder = new PreparedTransactionDecoder(loggerFactory)
  private val externalCallResult = ExternalCallResult(
    extensionId = "test-extension",
    functionId = "test-function",
    config = Bytes.assertFromString("00"),
    input = Bytes.assertFromString("01"),
    output = Bytes.assertFromString("02"),
  )
  private val protoExternalCallResult = isdv1.ExternalCallResult(
    extensionId = externalCallResult.extensionId,
    functionId = externalCallResult.functionId,
    config = externalCallResult.config.toByteString,
    input = externalCallResult.input.toByteString,
    output = externalCallResult.output.toByteString,
  )
  private val preparedHashTestTransactionUuid =
    UUID.fromString("4c6471d3-4e09-49dd-addf-6cd90e19c583")

  private lazy val generatorsTopology = new GeneratorsTopology(testedProtocolVersion)
  private lazy val generatorsLf = new GeneratorsLf(generatorsTopology)
  private lazy val generatorsInteractiveSubmission =
    new GeneratorsInteractiveSubmission(
      generatorsLf,
      generatorsTopology,
      exclusiveMaxSerializationVersion = LfSerializationVersion.VDev,
    )

  private def mkPrepareTransactionData(
      transaction: VersionedTransaction,
      submissionSeed: Hash,
      nodeSeeds: ImmArray[(NodeId, Hash)],
      inputContracts: Map[ContractId, ExternalInputContract] = Map.empty,
      synchronizer: Synchronizer =
        DefaultTestIdentities.physicalSynchronizerId.forExternalTransactionHashing,
  ): PrepareTransactionData =
    PrepareTransactionData(
      submitterInfo = SubmitterInfo(
        actAs = List.empty,
        readAs = List.empty,
        userId = LedgerUserId.assertFromString("app"),
        commandId = LedgerCommandId.assertFromString("command"),
        deduplicationPeriod = DeduplicationOffset(None),
        submissionId = None,
        externallySignedSubmission = None,
        // transactionHash is computed later from the verified signature during execution
        // (see EnrichedTransactionData / ExternalTransactionProcessor); unknown at this point.
        transactionHash = None,
      ),
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = LfTimestamp.Epoch,
        workflowId = None,
        preparationTime = LfTimestamp.Epoch,
        submissionSeed = submissionSeed,
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        optUsedPackages = None,
        optNodeSeeds = Some(nodeSeeds),
        optByKeyNodes = None,
      ),
      transaction = SubmittedTransaction(transaction),
      inputContracts = inputContracts,
      synchronizer = synchronizer,
      mediatorGroup = 0,
      transactionUUID = preparedHashTestTransactionUuid,
      maxRecordTime = None,
    )

  "Prepared transaction" should {
    import generatorsInteractiveSubmission.*

    "round trip encode and decode any LF transaction" in {
      forAll(minSuccessful(1000)) {
        (transaction: VersionedTransaction, nodeSeeds: Option[ImmArray[(NodeId, Hash)]]) =>
          val result = for {
            encoded <- encoder.serializeTransaction(transaction, nodeSeeds)
            decoded <- decoder.transactionTransformer
              .transform(encoded)
              .toFutureWithLoggedFailuresDecode("Failed to decode transaction", logger)
          } yield {
            decoded shouldEqual transaction
          }

          timeouts.default.await_("Round trip")(result)
      }
    }

    "support interfaceId on exercise node" in {
      implicit val nodeGen: Arbitrary[Node.Exercise] = Arbitrary(
        for {
          exerciseNode <- ValueGenerators.danglingRefExerciseNodeGenWithVersion(
            LfSerializationVersion.V1
          )
          normalized = normalizeNode(exerciseNode).copy(
            interfaceId = Some(ValueGenerators.idGen.sample.value)
          )
        } yield normalized
      )

      forAll { (node: Node.Exercise) =>
        val encoded =
          encoder.v1.exerciseTransformer(LfSerializationVersion.V1).transform(node).asEither.value
        decoder.v1.exerciseTransformer.transform(encoded).asEither.value shouldEqual node
      }
    }

    "support interfaceId on fetch node" in {
      implicit val nodeGen: Arbitrary[Node.Fetch] = Arbitrary(
        for {
          fetchNode <- ValueGenerators.fetchNodeGenWithVersion(LfSerializationVersion.V1)
          normalized = normalizeNode(fetchNode).copy(
            interfaceId = Some(ValueGenerators.idGen.sample.value)
          )
        } yield normalized
      )

      forAll { (node: Node.Fetch) =>
        val encoded =
          encoder.v1.fetchTransformer(LfSerializationVersion.V1).transform(node).asEither.value
        decoder.v1.fetchTransformer.transform(encoded).asEither.value shouldEqual node
      }
    }

    "reject exercise nodes with external call results before LF dev" in {
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.V2)
          .sample
          .value
      ).copy(children = ImmArray.Empty)

      val encoded =
        encoder.v1
          .exerciseTransformer(LfSerializationVersion.V2)
          .transform(exerciseNode)
          .asEither
          .value
      val tampered = encoded.copy(externalCallResults = Seq(protoExternalCallResult))

      decoder.v1.exerciseTransformer.transform(tampered).asEither match {
        case Left(errors) =>
          errors.toString should include(
            "External call results are not supported in nodes with LF Serialization version V2"
          )
        case Right(decoded) => fail(s"Decoded malformed prepared exercise node: $decoded")
      }
    }

    "reject encoding exercise nodes with external call results before LF dev" in {
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.V2)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        externalCallResults = ImmArray(externalCallResult),
      )

      encoder.v1
        .exerciseTransformer(LfSerializationVersion.V2)
        .transform(exerciseNode)
        .asEither match {
        case Left(errors) =>
          errors.toString should include(
            "External call results are not supported in nodes with LF Serialization version V2"
          )
        case Right(encoded) => fail(s"Encoded malformed prepared exercise node: $encoded")
      }
    }

    "reject external call result ids that are invalid Daml Text" in {
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.VDev)
          .sample
          .value
      ).copy(children = ImmArray.Empty)

      val encoded =
        encoder.v1
          .exerciseTransformer(LfSerializationVersion.VDev)
          .transform(exerciseNode)
          .asEither
          .value

      Seq(
        protoExternalCallResult.copy(extensionId = "bad\u0000extension") ->
          "Invalid external call extension_id: Text contains null character",
        protoExternalCallResult.copy(functionId = "bad\u0000function") ->
          "Invalid external call function_id: Text contains null character",
      ).foreach { case (externalCallResult, expectedError) =>
        val tampered = encoded.copy(externalCallResults = Seq(externalCallResult))

        decoder.v1.exerciseTransformer.transform(tampered).asEither match {
          case Left(errors) =>
            errors.toString should include(expectedError)
          case Right(decoded) => fail(s"Decoded malformed prepared exercise node: $decoded")
        }
      }
    }

    "preserve external call results when round tripping VDev transactions" in {
      val nodeId = NodeId(0)
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.VDev)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        externalCallResults = ImmArray(externalCallResult),
        version = LfSerializationVersion.VDev,
      )
      val transaction = VersionedTransaction(
        LfSerializationVersion.VDev,
        Map(nodeId -> exerciseNode),
        ImmArray(nodeId),
      )

      val result = for {
        encoded <- encoder.serializeTransaction(transaction, nodeSeeds = None)
        decoded <- decoder.transactionTransformer
          .transform(encoded)
          .toFutureWithLoggedFailuresDecode("Failed to decode transaction", logger)
      } yield {
        val encodedExerciseNodes = encoded.nodes.flatMap {
          _.versionedNode.v1.value.nodeType match {
            case NodeType.Exercise(exercise) => Seq(exercise)
            case _ => Seq.empty
          }
        }
        encodedExerciseNodes should have size 1
        encodedExerciseNodes.head.externalCallResults shouldBe Seq(protoExternalCallResult)

        val decodedExerciseNodes = decoded.nodes.values.collect { case exercise: Node.Exercise =>
          exercise
        }.toSeq
        decodedExerciseNodes should have size 1
        decodedExerciseNodes.head.externalCallResults shouldBe ImmArray(externalCallResult)
      }

      timeouts.default.await_("Round trip VDev external-call transaction")(result)
    }

    "reject VDev prepared hashes on stable protocol versions" in {
      val nodeId = NodeId(0)
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.VDev)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        choiceAuthorizers = None,
        externalCallResults = ImmArray(externalCallResult),
        version = LfSerializationVersion.VDev,
      )
      val transaction = VersionedTransaction(
        LfSerializationVersion.VDev,
        Map(nodeId -> exerciseNode),
        ImmArray(nodeId),
      )
      val prepareTransactionData = mkPrepareTransactionData(
        transaction = transaction,
        submissionSeed = Hash.hashPrivateKey("prepared-vdev-test"),
        nodeSeeds = ImmArray(nodeId -> Hash.hashPrivateKey("prepared-vdev-node")),
      )

      prepareTransactionData
        .computeHash(HashingSchemeVersion.V4, ProtocolVersion.v35)
        .left
        .value
        .message should include("Hashing scheme version V4 is not supported on protocol version")

      val vdevNodeStableV3Error = prepareTransactionData
        .computeHash(HashingSchemeVersion.V3, ProtocolVersion.v35)
        .left
        .value
        .message
      vdevNodeStableV3Error should (include("LF serialization version VDev") and include(
        "Please use hashing scheme V4 or higher"
      ))

      val vdevNodeV3Error = prepareTransactionData
        .computeHash(HashingSchemeVersion.V3, ProtocolVersion.dev)
        .left
        .value
        .message
      vdevNodeV3Error should include("LF serialization version VDev")
      vdevNodeV3Error should include("Please use hashing scheme V4 or higher")

      val v2NodeId = NodeId(1)
      val v2ExerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.V2)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        choiceAuthorizers = None,
        externalCallResults = ImmArray.Empty,
        version = LfSerializationVersion.V2,
      )
      val topLevelVDevPrepareTransactionData = mkPrepareTransactionData(
        transaction = VersionedTransaction(
          LfSerializationVersion.VDev,
          Map(v2NodeId -> v2ExerciseNode),
          ImmArray(v2NodeId),
        ),
        submissionSeed = Hash.hashPrivateKey("prepared-vdev-wrapper-test"),
        nodeSeeds = ImmArray(v2NodeId -> Hash.hashPrivateKey("prepared-vdev-wrapper-node")),
      )

      val topLevelVDevV3Error = topLevelVDevPrepareTransactionData
        .computeHash(HashingSchemeVersion.V3, ProtocolVersion.dev)
        .left
        .value
        .message
      topLevelVDevV3Error should include("LF serialization version VDev")
      topLevelVDevV3Error should include("Please use hashing scheme V4 or higher")

      prepareTransactionData.computeHash(HashingSchemeVersion.V4, ProtocolVersion.dev).value
    }

    "reject transactions whose top-level LF version is older than a contained node" in {
      val nodeId = NodeId(0)
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.V2)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        externalCallResults = ImmArray(externalCallResult),
        version = LfSerializationVersion.VDev,
      )
      val transaction = VersionedTransaction(
        LfSerializationVersion.VDev,
        Map(nodeId -> exerciseNode),
        ImmArray(nodeId),
      )

      val result = for {
        encoded <- encoder.serializeTransaction(transaction, nodeSeeds = None)
      } yield {
        val tampered =
          encoded.copy(version = LfSerializationVersion.toProtoValue(LfSerializationVersion.V2))

        decoder.transactionTransformer.transform(tampered).asEither match {
          case Left(errors) =>
            errors.toString should include(
              "A transaction of version V2 cannot contain node of newer version (version VDev)"
            )
          case Right(decoded) => fail(s"Decoded malformed prepared transaction: $decoded")
        }
      }

      timeouts.default.await_("Reject malformed prepared transaction")(result)
    }

    "change the prepared hash when external-call output changes" in {
      val physicalSynchronizerId =
        DefaultTestIdentities.physicalSynchronizerId.copy(protocolVersion = ProtocolVersion.dev)
      val hashVersion = HashingSchemeVersion.V4
      val nodeId = NodeId(0)
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.VDev)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        choiceAuthorizers = None,
        externalCallResults = ImmArray(externalCallResult),
        version = LfSerializationVersion.VDev,
      )
      val changedExerciseNode = exerciseNode.copy(
        externalCallResults = ImmArray(
          externalCallResult.copy(output = Bytes.assertFromString("ffff"))
        )
      )
      val inputContract = FatContractInstance.fromCreateNode(
        normalizeNode(
          ValueGenerators
            .malformedCreateNodeGenWithVersion(LfSerializationVersion.VDev)
            .sample
            .value
        ).copy(coid = exerciseNode.targetCoid, keyOpt = None),
        CreationTime.CreatedAt(LfTimestamp.Epoch),
        Bytes.Empty,
      )

      def prepareData(node: Node.Exercise): PrepareTransactionData =
        mkPrepareTransactionData(
          transaction = VersionedTransaction(
            LfSerializationVersion.VDev,
            Map(nodeId -> node),
            ImmArray(nodeId),
          ),
          submissionSeed = Hash.hashPrivateKey("prepared-external-call-hash-output-test"),
          nodeSeeds =
            ImmArray(nodeId -> Hash.hashPrivateKey("prepared-external-call-hash-output-node")),
          inputContracts =
            Map(inputContract.contractId -> ExternalInputContract(inputContract, inputContract)),
          synchronizer = physicalSynchronizerId.forExternalTransactionHashing,
        )

      val originalHash =
        prepareData(exerciseNode)
          .computeHash(hashVersion, physicalSynchronizerId.protocolVersion)
          .value
      val changedHash =
        prepareData(changedExerciseNode)
          .computeHash(hashVersion, physicalSynchronizerId.protocolVersion)
          .value

      changedHash should not be originalHash
    }

    "compute matching prepare and execute hashes for external-call transactions" in {
      val physicalSynchronizerId =
        DefaultTestIdentities.physicalSynchronizerId.copy(protocolVersion = ProtocolVersion.dev)
      val hashVersion = HashingSchemeVersion.V4
      val nodeId = NodeId(0)
      val exerciseNode = normalizeNode(
        ValueGenerators
          .danglingRefExerciseNodeGenWithVersion(LfSerializationVersion.VDev)
          .sample
          .value
      ).copy(
        children = ImmArray.Empty,
        choiceAuthorizers = None,
        externalCallResults = ImmArray(externalCallResult),
        version = LfSerializationVersion.VDev,
      )
      val transaction = VersionedTransaction(
        LfSerializationVersion.VDev,
        Map(nodeId -> exerciseNode),
        ImmArray(nodeId),
      )
      val inputContract = FatContractInstance.fromCreateNode(
        normalizeNode(
          ValueGenerators
            .malformedCreateNodeGenWithVersion(LfSerializationVersion.VDev)
            .sample
            .value
        ).copy(coid = exerciseNode.targetCoid, keyOpt = None),
        CreationTime.CreatedAt(LfTimestamp.Epoch),
        Bytes.Empty,
      )
      val prepareTransactionData = mkPrepareTransactionData(
        transaction = transaction,
        submissionSeed = Hash.hashPrivateKey("prepared-external-call-hash-test"),
        nodeSeeds = ImmArray(nodeId -> Hash.hashPrivateKey("prepared-external-call-hash-node")),
        inputContracts =
          Map(inputContract.contractId -> ExternalInputContract(inputContract, inputContract)),
        synchronizer = physicalSynchronizerId.forExternalTransactionHashing,
      )
      val prepareHash =
        prepareTransactionData
          .computeHash(hashVersion, physicalSynchronizerId.protocolVersion)
          .value

      val result = for {
        encoded <- encoder.encode(prepareTransactionData)
        executeTransactionData <- decoder.decode(
          ExecuteRequest(
            LedgerUserId.assertFromString("app"),
            LedgerSubmissionId.assertFromString(UUID.randomUUID().toString),
            DeduplicationOffset(None),
            Map.empty,
            encoded,
            hashVersion,
            tentativeLedgerEffectiveTime = LfTimestamp.now(),
          )
        )
        tampered = encoded.copy(transaction =
          encoded.transaction.map(transaction =>
            transaction.copy(nodes = transaction.nodes.map { node =>
              val tamperedVersionedNode = node.versionedNode match {
                case VersionedNode.V1(v1Node) =>
                  val tamperedNodeType = v1Node.nodeType match {
                    case NodeType.Exercise(exercise) =>
                      NodeType.Exercise(
                        exercise.copy(externalCallResults =
                          exercise.externalCallResults.map(
                            _.copy(output = Bytes.assertFromString("ff").toByteString)
                          )
                        )
                      )
                    case other => other
                  }
                  VersionedNode.V1(v1Node.copy(nodeType = tamperedNodeType))
                case VersionedNode.Empty => VersionedNode.Empty
              }

              node.copy(versionedNode = tamperedVersionedNode)
            })
          )
        )
        tamperedExecuteTransactionData <- decoder.decode(
          ExecuteRequest(
            LedgerUserId.assertFromString("app"),
            LedgerSubmissionId.assertFromString(UUID.randomUUID().toString),
            DeduplicationOffset(None),
            Map.empty,
            tampered,
            hashVersion,
            tentativeLedgerEffectiveTime = LfTimestamp.now(),
          )
        )
      } yield {
        encoded.metadata.value.synchronizerId shouldBe physicalSynchronizerId.toProtoPrimitive
        val encodedExerciseNodes = encoded.transaction.value.nodes.flatMap {
          _.versionedNode.v1.value.nodeType match {
            case NodeType.Exercise(exercise) => Seq(exercise)
            case _ => Seq.empty
          }
        }
        encodedExerciseNodes should have size 1
        encodedExerciseNodes.head.externalCallResults shouldBe Seq(protoExternalCallResult)
        executeTransactionData
          .computeHash(hashVersion, physicalSynchronizerId.protocolVersion)
          .value shouldBe prepareHash
        tamperedExecuteTransactionData
          .computeHash(hashVersion, physicalSynchronizerId.protocolVersion)
          .value should not be prepareHash
      }

      timeouts.default.await_("Compute matching prepare and execute hashes")(result)
    }

    "sort sets of parties" in {
      forAll { (transaction: VersionedTransaction, nodeSeeds: Option[ImmArray[(NodeId, Hash)]]) =>
        val result = for {
          encoded <- encoder.serializeTransaction(transaction, nodeSeeds)
        } yield {
          val partiesLists = encoded.nodes.flatMap {
            _.versionedNode.v1.value.nodeType match {
              case NodeType.Empty => Seq.empty
              case NodeType.Create(value) => Seq(value.signatories, value.stakeholders)
              case NodeType.Fetch(value) =>
                Seq(value.signatories, value.stakeholders, value.actingParties)
              case NodeType.Exercise(value) =>
                Seq(
                  value.signatories,
                  value.stakeholders,
                  value.actingParties,
                  value.choiceObservers,
                )
              case NodeType.Rollback(_) => Seq.empty
              case NodeType.QueryByKey(_) => Seq.empty
            }
          }
          partiesLists.foreach { partiesList =>
            partiesList.sorted should contain theSameElementsInOrderAs (partiesList)
          }
        }

        timeouts.default.await_("Round trip")(result)
      }
    }
  }
}
