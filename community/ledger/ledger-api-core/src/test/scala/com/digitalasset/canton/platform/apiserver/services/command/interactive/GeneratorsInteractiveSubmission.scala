// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import com.digitalasset.canton.config.GeneratorsConfig.*
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.data.{DeduplicationPeriod, LedgerTimeBoundaries, Offset}
import com.digitalasset.canton.ledger.participant.state.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.EnrichedTransactionData.ExternalInputContract
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.PrepareTransactionData
import com.digitalasset.canton.protocol.LfFatContractInst
import com.digitalasset.canton.topology.{GeneratorsTopology, SynchronizerId}
import com.digitalasset.canton.{GeneratorsLf, LedgerUserId, LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  GlobalKey,
  Node,
  NodeId,
  SubmittedTransaction,
  Transaction,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.test.ValueGenerators
import magnolify.scalacheck.auto.genArbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

import scala.jdk.CollectionConverters.*
import scala.util.Random

final class GeneratorsInteractiveSubmission(
    generatorsLf: GeneratorsLf,
    generatorsTopology: GeneratorsTopology,
) {
  import com.digitalasset.canton.Generators.*
  import generatorsLf.*
  import generatorsTopology.*

  // Updated nodes that filter out fields not supported in LF 2.1
  def normalizeNodeForV1[N <: Node](node: N): N = node match {
    case node: Node.Create =>
      node
        .copy(
          version = LanguageVersion.v2_1,
          keyOpt = None,
          // signatories should be a subset of stakeholders for the node to be valid
          // take a random size subset of stakeholders, but 1 minimum
          signatories = node.stakeholders.take(Random.nextInt(10) + 1),
        )
        .asInstanceOf[N]
    case node: Node.Exercise =>
      node
        .copy(
          version = LanguageVersion.v2_1,
          keyOpt = None,
          byKey = false,
          choiceAuthorizers = None,
        )
        .asInstanceOf[N]
    case node: Node.Fetch =>
      node
        .copy(
          version = LanguageVersion.v2_1,
          keyOpt = None,
          byKey = false,
        )
        .asInstanceOf[N]
    case node => node
  }

  private val nodeIdGen = Arbitrary.arbInt.arbitrary.map(NodeId(_))

  val noDanglingRefGenTransaction: Gen[Transaction] =
    ValueGenerators.noDanglingRefGenTransaction.map { tx =>
      tx.copy(
        nodes = tx.nodes.map { case (nodeId, node) =>
          nodeId -> normalizeNodeForV1(node)
        }
      )
    }

  private val versionedTransactionGenerator = for {
    transaction <- noDanglingRefGenTransaction
  } yield VersionedTransaction(LanguageVersion.v2_1, transaction.nodes, transaction.roots)

  implicit val transactionArb: Arbitrary[VersionedTransaction] = Arbitrary(
    versionedTransactionGenerator
  )

  private implicit val genHash: Gen[crypto.Hash] =
    Gen
      .containerOfN[Array, Byte](
        crypto.Hash.underlyingHashLength,
        arbitrary[Byte],
      )
      .map(crypto.Hash.assertFromByteArray)

  private implicit val nodeSeed: Gen[(NodeId, Hash)] = for {
    nodeId <- nodeIdGen
    hash <- genHash
  } yield (nodeId, hash)

  private val nodeSeedsGen: Gen[Option[ImmArray[(NodeId, Hash)]]] = for {
    seeds <- Gen.listOf(nodeSeed).map(ImmArray.from)
    optSeeds <- Gen.option(seeds)
  } yield optSeeds

  implicit val nodeSeedsArbitrary: Arbitrary[Option[ImmArray[(NodeId, Hash)]]] = Arbitrary(
    nodeSeedsGen
  )

  private implicit val byKeyNodesArbitrary: Arbitrary[Option[ImmArray[NodeId]]] = Arbitrary(
    Gen.option(Gen.listOf(nodeIdGen).map(ImmArray.from(_)))
  )

  private implicit val genDeduplicationDuration: Gen[DeduplicationPeriod.DeduplicationDuration] =
    Gen
      .choose(1, 200L)
      .map(PositiveFiniteDuration.ofMinutes)
      .map(d => DeduplicationPeriod.DeduplicationDuration(d.asJava))
  private implicit val genDeduplicationOffset: Gen[DeduplicationPeriod.DeduplicationOffset] =
    Arbitrary
      .arbitrary[PositiveLong]
      .map(_.value)
      .map(Offset.tryFromLong)
      .map(Option.apply)
      .map(DeduplicationPeriod.DeduplicationOffset.apply)
  private implicit val genDeduplicationPeriodArb: Arbitrary[DeduplicationPeriod] =
    Arbitrary(Gen.oneOf(genDeduplicationDuration, genDeduplicationOffset))

  private implicit val timeBoundariesGen: Gen[LedgerTimeBoundaries] = for {
    t1 <- Gen.option(Arbitrary.arbitrary[Time.Timestamp])
    t2 <- Gen.option(Arbitrary.arbitrary[Time.Timestamp])
  } yield {
    (t1, t2) match {
      case (Some(t1), Some(t2)) if t1 > t2 => LedgerTimeBoundaries(Time.Range(t2, t1))
      case _ => LedgerTimeBoundaries.fromConstraints(t1, t2)
    }
  }

  private implicit val submitterInfoGen: Gen[SubmitterInfo] = for {
    actAs <- Arbitrary.arbitrary[List[LfPartyId]]
    readAs <- Arbitrary.arbitrary[List[LfPartyId]]
    userId <- Arbitrary.arbitrary[LedgerUserId]
    commandId <- lfCommandIdArb.arbitrary
    deduplicationPeriod <- genDeduplicationPeriodArb.arbitrary
    submissionIdO <- Gen.option(lfSubmissionIdArb.arbitrary)
  } yield SubmitterInfo(
    actAs,
    readAs,
    userId,
    commandId,
    deduplicationPeriod,
    submissionIdO,
    externallySignedSubmission = None,
  )

  private def transactionMetaGen(transaction: VersionedTransaction): Gen[TransactionMeta] = for {
    ledgerEffectiveTime <- Arbitrary.arbitrary[Time.Timestamp]
    workflowIdO <- Gen.option(lfWorkflowIdArb.arbitrary)
    preparationTime <- Arbitrary.arbitrary[Time.Timestamp]
    submissionSeed <- Arbitrary.arbitrary[crypto.Hash]
    timeBoundaries <- timeBoundariesGen
    usedPackagesO <- Arbitrary.arbitrary[Option[Set[LfPackageId]]]
    optNodeSeedsO <- Gen
      .listOfN(transaction.nodes.size, Arbitrary.arbitrary[crypto.Hash])
      .map(seeds => transaction.nodes.keySet.zip(seeds))
    optByKeyNodeO <- Arbitrary.arbitrary[Option[ImmArray[NodeId]]]
  } yield TransactionMeta(
    ledgerEffectiveTime,
    workflowIdO,
    preparationTime,
    submissionSeed,
    timeBoundaries,
    usedPackagesO,
    Some(ImmArray.from(optNodeSeedsO)),
    optByKeyNodeO,
  )

  private val globalKeyMappingGen: Gen[Map[GlobalKey, Option[Value.ContractId]]] =
    boundedMapGen[GlobalKey, Option[Value.ContractId]]

  private def inputContractsGen(overrideCid: Value.ContractId): Gen[LfFatContractInst] = for {
    create <- ValueGenerators
      .malformedCreateNodeGenWithVersion(LanguageVersion.v2_1)
      .map(normalizeNodeForV1)
    createdAt <- Arbitrary.arbitrary[Time.Timestamp]
    driverMetadata <- Arbitrary.arbitrary[Array[Byte]].map(Bytes.fromByteArray)
  } yield FatContractInstance.fromCreateNode(
    create.copy(coid = overrideCid),
    CreationTime.CreatedAt(createdAt),
    driverMetadata,
  )

  private val preparedTransactionDataGen: Gen[PrepareTransactionData] = for {
    submitterInfo <- submitterInfoGen
    synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
    transaction <- versionedTransactionGenerator.map(SubmittedTransaction(_))
    transactionMeta <- transactionMetaGen(transaction)
    globalKeyMapping <- globalKeyMappingGen
    coids <- boundedListGen(ValueGenerators.coidGen)
    inputContracts <- Gen.sequence(coids.map(inputContractsGen))
    enrichedInputContracts <- Gen.sequence(coids.map(inputContractsGen))
    mediatorGroup <- Arbitrary.arbitrary[PositiveInt]
    transactionUUID <- Gen.uuid
  } yield PrepareTransactionData(
    submitterInfo,
    transactionMeta,
    transaction,
    globalKeyMapping,
    inputContracts.asScala
      .zip(enrichedInputContracts.asScala)
      .map { case (originalFci, enrichedFci) =>
        originalFci.contractId -> ExternalInputContract(originalFci, enrichedFci)
      }
      .toMap,
    synchronizerId,
    mediatorGroup.value,
    transactionUUID,
  )

  implicit val preparedTransactionDataArb: Arbitrary[PrepareTransactionData] = Arbitrary(
    preparedTransactionDataGen
  )
}
