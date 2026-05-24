// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data.Node.NodeType
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.PreparedTransactionCodec.*
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.{
  PreparedTransactionDecoder,
  PreparedTransactionEncoder,
}
import com.digitalasset.canton.topology.{GeneratorsTopology, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.canton.{BaseTest, GeneratorsLf, HasExecutionContext}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.{
  Node,
  NodeId,
  SerializationVersion as LfSerializationVersion,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.test.ValueGenerators
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Duration

class PreparedTransactionCodecV1Spec
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with ScalaCheckPropertyChecks
    with HasExecutionContext {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val decoder = new PreparedTransactionDecoder(loggerFactory)

  private lazy val generatorsTopology = new GeneratorsTopology(testedProtocolVersion)
  private lazy val generatorsLf = new GeneratorsLf(generatorsTopology)
  private lazy val generatorsInteractiveSubmission =
    new GeneratorsInteractiveSubmission(
      generatorsLf,
      generatorsTopology,
      exclusiveMaxSerializationVersion = LfSerializationVersion.VDev,
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

    "compute the same prepare and execute hashes for a physical synchronizer" in {
      forAll(minSuccessful(100)) {
        (prepareTransactionData: PrepareTransactionData, psid: PhysicalSynchronizerId) =>
          val prepareDataWithPhysicalSynchronizer =
            prepareTransactionData.copy(synchronizer = psid)
          val protocolVersion = psid.protocolVersion
          val hashVersion =
            if (prepareDataWithPhysicalSynchronizer.transaction.version == LfSerializationVersion.VDev)
              HashingSchemeVersion.V3
            else HashingSchemeVersion.V2
          val prepareHash = prepareDataWithPhysicalSynchronizer
            .computeHash(hashVersion, protocolVersion, psid)
            .value

          val result = for {
            preparedTransaction <- encoder.encode(prepareDataWithPhysicalSynchronizer)
            executeTransactionData <- decoder.decode(
              ExecuteRequest(
                userId = Ref.UserId.assertFromString("interactive-test-user"),
                submissionId = Ref.SubmissionId.assertFromString("interactive-test-submission"),
                deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
                signatures = Map.empty,
                preparedTransaction = preparedTransaction,
                serializationVersion = hashVersion,
                tentativeLedgerEffectiveTime = Time.Timestamp.Epoch,
              )
            )
          } yield {
            val executeHash = executeTransactionData
              .computeHash(hashVersion, protocolVersion, psid)
              .value

            preparedTransaction.metadata.value.synchronizerId shouldEqual psid.logical.toProtoPrimitive
            SynchronizerId
              .fromProtoPrimitive(
                preparedTransaction.metadata.value.synchronizerId,
                "synchronizer_id",
              )
              .value shouldEqual psid.logical
            executeHash shouldEqual prepareHash
          }

          timeouts.default.await_("Compute matching prepare and execute hashes")(result)
      }
    }
  }
}
