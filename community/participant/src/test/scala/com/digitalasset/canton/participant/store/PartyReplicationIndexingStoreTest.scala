// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractReassignment,
  ContractsReassignmentBatch,
}
import com.digitalasset.canton.participant.admin.party.GeneratesUniqueUpdateIds
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.{
  ActivationChange,
  ContractActivationChangeBatch,
}
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStoreTest.ContractIndexingInfo
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{
  ExampleContractFactory,
  ExampleTransactionFactory,
  LfContractId,
  UpdateId,
}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPackageId,
  ProtocolVersionChecksAsyncWordSpec,
  ReassignmentCounter,
  RepairCounter,
}
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.annotation.nowarn

@nowarn("msg=match may not be exhaustive")
trait PartyReplicationIndexingStoreTest
    extends AsyncWordSpecLike
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  protected lazy val storeSynchronizerStr: String300 =
    String300.tryCreate("party-replication-indexing-store::default")
  protected lazy val storeSynchronizerId: SynchronizerId =
    SynchronizerId.tryFromString(storeSynchronizerStr.unwrap)
  private val alice = PartyId.tryFromProtoPrimitive("alice::namespace")
  private val bob = PartyId.tryFromProtoPrimitive("bob::namespace")
  private val toc1_0 = TimeOfChange(CantonTimestamp.ofEpochSecond(1), Some(RepairCounter.Genesis))
  private val toc1_1 = TimeOfChange(CantonTimestamp.ofEpochSecond(1), Some(RepairCounter.One))
  private val toc2_2 = TimeOfChange(CantonTimestamp.ofEpochSecond(2), Some(RepairCounter(2)))
  private val toc4_0 = TimeOfChange(CantonTimestamp.ofEpochSecond(4), Some(RepairCounter.Genesis))
  private val sourceValidationPackageId = Source(
    LfPackageId.assertFromString("source-validation-package-id")
  )
  private val targetValidationPackageId = Target(
    LfPackageId.assertFromString("target-validation-package-id")
  )
  private val isIndexed = true

  private def generateReassignments(
      howManyReassignments: PositiveInt
  ): NonEmpty[Seq[ContractReassignment]] =
    ContractsReassignmentBatch
      .create((0 until howManyReassignments.unwrap).map { ctr =>
        (
          ExampleContractFactory.build(),
          sourceValidationPackageId,
          targetValidationPackageId,
          ReassignmentCounter.apply(ctr.toLong),
        )
      })
      .valueOrFail("Failed to create reassignment batch")
      .contracts

  private def expectedAfterImport(
      acsImport: Map[TimeOfChange, NonEmpty[Seq[ContractReassignment]]]
  ): Seq[ActivationChange] = acsImport
    .flatMap { case (toc, reassignments) =>
      reassignments.map { case ContractReassignment(contract, _, _, ctr) =>
        (toc, contract.contractId, ChangeType.Activation, None, !isIndexed, ctr)
      }
    }
    .toSeq
    .sorted(
      Ordering.by[ActivationChange, (TimeOfChange, String)] { case (toc, cid, _, _, _, _) =>
        (toc, cid.coid)
      }
    )

  private def indexerBatch: ActivationChange => ContractIndexingInfo = {
    case (_, cid, change, _, _, ctr) =>
      cid -> (change, ctr)
  }

  private def markIndexing(
      batchCounter: NonNegativeLong,
      batchUpdateId: UpdateId,
  ): PartialFunction[ActivationChange, ActivationChange] = {
    case (toc, cid, change, _, false, ctr) =>
      (toc, cid, change, Some((batchCounter, batchUpdateId)), false, ctr)
  }

  private def markIndexed(
      batchCounter: NonNegativeLong,
      batchUpdateId: UpdateId,
  ): PartialFunction[ActivationChange, ActivationChange] = {
    case (toc, cid, change, Some((`batchCounter`, `batchUpdateId`)), false, ctr) =>
      (toc, cid, change, Some((batchCounter, batchUpdateId)), true, ctr)
  }

  private def expectedBatch(
      updateId: UpdateId,
      activationChanges: Seq[ActivationChange],
  ): Option[ContractActivationChangeBatch] =
    NonEmpty
      .from(activationChanges.map(indexerBatch).toMap)
      .map(ContractActivationChangeBatch(updateId, _))

  def partyReplicationIndexingStore(mk: () => PartyReplicationIndexingStore): Unit =
    "PartyReplicationIndexingStore" when {
      lazy val onprAcsImports1_0 = generateReassignments(PositiveInt.three)
      lazy val onprAcsImports1_1 = generateReassignments(PositiveInt.tryCreate(10))
      lazy val onprAcsImports2_2 = generateReassignments(PositiveInt.tryCreate(4))
      lazy val onprAcsImports4_0 = generateReassignments(PositiveInt.one)

      "addImportedContractActivations" should {
        // TODO(#26775): Change from Protocol.dev to Protocol.v3x
        "insert contract activations" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          val store = mk()
          (for {
            storedAtGenesis <- store.listContractActivationChanges(alice)
            _ <- store.addImportedContractActivations(alice, toc1_0, onprAcsImports1_0)
            storedAtToc1_0 <- store.listContractActivationChanges(alice)
            _ <- store.addImportedContractActivations(alice, toc1_1, onprAcsImports1_1)
            storedAtToc1_1 <- store.listContractActivationChanges(alice)
            _ <- store.addImportedContractActivations(alice, toc2_2, onprAcsImports2_2)
            storedAtToc2_2 <- store.listContractActivationChanges(alice)
          } yield {
            storedAtGenesis shouldBe Seq.empty
            storedAtToc1_0 shouldBe expectedAfterImport(Map(toc1_0 -> onprAcsImports1_0))
            storedAtToc1_1 shouldBe expectedAfterImport(
              Map(toc1_0 -> onprAcsImports1_0, toc1_1 -> onprAcsImports1_1)
            )
            storedAtToc2_2 shouldBe expectedAfterImport(
              Map(
                toc1_0 -> onprAcsImports1_0,
                toc1_1 -> onprAcsImports1_1,
                toc2_2 -> onprAcsImports2_2,
              )
            )
          }).failOnShutdown
        }
      }

      "getAndUpdateMarkIndexingContractActivationChanges" should {
        "update indexing contract activation changes" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          val store = mk()
          val Seq(
            (batch0Counter, batch0UpdateId),
            (batch1Counter, batch1UpdateId),
            (batch2Counter, batch2UpdateId),
            (batch3Counter, batch3UpdateId),
            (batch4Counter, batch4UpdateId),
          ) = (0 to 4).map(i =>
            (NonNegativeLong.tryCreate(i.toLong), ExampleTransactionFactory.updateId(i))
          )
          val maxBatchSize = PositiveInt.tryCreate(5)

          def injectUpdateId(updateId: UpdateId): GeneratesUniqueUpdateIds =
            new GeneratesUniqueUpdateIds {
              override def uniqueUpdateId(
                  onprBatchCounter: NonNegativeLong,
                  batch: Seq[(TimeOfChange, LfContractId, ChangeType, ReassignmentCounter)],
              ): UpdateId = updateId
            }

          (for {
            _ <- store.addImportedContractActivations(alice, toc1_0, onprAcsImports1_0)
            _ <- store.addImportedContractActivations(alice, toc1_1, onprAcsImports1_1)
            _ <- store.addImportedContractActivations(alice, toc2_2, onprAcsImports2_2)
            storedAfterImport <- store.listContractActivationChanges(alice)
            batch0 <- store.consumeNextActivationChangesBatch(
              alice,
              batch0Counter,
              maxBatchSize,
            )(injectUpdateId(batch0UpdateId))
            storedAfterIndexingBatch0 <- store.listContractActivationChanges(alice)
            batch1 <- store.consumeNextActivationChangesBatch(
              alice,
              batch1Counter,
              maxBatchSize,
            )(injectUpdateId(batch1UpdateId))
            storedAfterIndexingBatch1 <- store.listContractActivationChanges(alice)
            batch2 <- store.consumeNextActivationChangesBatch(
              alice,
              batch2Counter,
              maxBatchSize,
            )(injectUpdateId(batch2UpdateId))
            storedAfterIndexingBatch2 <- store.listContractActivationChanges(alice)
            batch3 <- store.consumeNextActivationChangesBatch(
              alice,
              batch3Counter,
              maxBatchSize,
            )(injectUpdateId(batch3UpdateId))
            batch4 <- store.consumeNextActivationChangesBatch(
              alice,
              batch4Counter,
              maxBatchSize,
            )(injectUpdateId(batch4UpdateId))

            storedAfterIndexingBatch3 <- store.listContractActivationChanges(alice)
            _ <- store.markContractActivationChangesAsIndexed(batch0UpdateId)
            storedAfterIndexedBatch0 <- store.listContractActivationChanges(alice)
            _ <- store.markContractActivationChangesAsIndexed(batch1UpdateId)
            storedAfterIndexedBatch1 <- store.listContractActivationChanges(alice)
            _ <- store.markContractActivationChangesAsIndexed(batch2UpdateId)
            storedAfterIndexedBatch2 <- store.listContractActivationChanges(alice)
            _ <- store.markContractActivationChangesAsIndexed(batch3UpdateId)
            storedAfterIndexedBatch3 <- store.listContractActivationChanges(alice)

            _ <- store.addImportedContractActivations(bob, toc4_0, onprAcsImports4_0)
            _ <- store.purgeContractActivationChanges(alice)
            storedAfterDelete <- store.listContractActivationChanges(alice)
            storedOnBehalfOfBob <- store.listContractActivationChanges(bob)
          } yield {
            val activationsAfterImport = expectedAfterImport(
              Map(
                toc1_0 -> onprAcsImports1_0,
                toc1_1 -> onprAcsImports1_1,
                toc2_2 -> onprAcsImports2_2,
              )
            )
            storedAfterImport shouldBe activationsAfterImport

            val (batch0Head, batch0Tail) = activationsAfterImport.splitAt(maxBatchSize.unwrap)
            batch0 shouldBe expectedBatch(batch0UpdateId, batch0Head)
            val batch0Indexing = batch0Head.collect(markIndexing(batch0Counter, batch0UpdateId))
            storedAfterIndexingBatch0 shouldBe batch0Indexing ++ batch0Tail

            val (batch1Head, batch1Tail) = batch0Tail.splitAt(maxBatchSize.unwrap)
            batch1 shouldBe expectedBatch(batch1UpdateId, batch1Head)
            val batch1Indexing = batch1Head.collect(markIndexing(batch1Counter, batch1UpdateId))
            storedAfterIndexingBatch1 shouldBe batch0Indexing ++ batch1Indexing ++ batch1Tail

            val (batch2Head, batch2Tail) = batch1Tail.splitAt(maxBatchSize.unwrap)
            batch2 shouldBe expectedBatch(batch2UpdateId, batch2Head)
            val batch2Indexing = batch2Head.collect(markIndexing(batch2Counter, batch2UpdateId))
            storedAfterIndexingBatch2 shouldBe batch0Indexing ++ batch1Indexing ++ batch2Indexing ++ batch2Tail

            val (batch3Head, batch3Tail) = batch2Tail.splitAt(maxBatchSize.unwrap)
            batch3 shouldBe expectedBatch(batch3UpdateId, batch3Head)
            val batch3Indexing = batch3Head.collect(markIndexing(batch3Counter, batch3UpdateId))
            storedAfterIndexingBatch3 shouldBe batch0Indexing ++ batch1Indexing ++ batch2Indexing ++ batch3Indexing
            batch3Tail shouldBe Nil

            batch4 shouldBe None

            val batch0Indexed = batch0Indexing.collect(markIndexed(batch0Counter, batch0UpdateId))
            val batch1Indexed = batch1Indexing.collect(markIndexed(batch1Counter, batch1UpdateId))
            val batch2Indexed = batch2Indexing.collect(markIndexed(batch2Counter, batch2UpdateId))
            val batch3Indexed = batch3Indexing.collect(markIndexed(batch3Counter, batch3UpdateId))
            storedAfterIndexedBatch0 shouldBe batch0Indexed ++ batch1Indexing ++ batch2Indexing ++ batch3Indexing
            storedAfterIndexedBatch1 shouldBe batch0Indexed ++ batch1Indexed ++ batch2Indexing ++ batch3Indexing
            storedAfterIndexedBatch2 shouldBe batch0Indexed ++ batch1Indexed ++ batch2Indexed ++ batch3Indexing
            storedAfterIndexedBatch3 shouldBe batch0Indexed ++ batch1Indexed ++ batch2Indexed ++ batch3Indexed

            storedAfterDelete shouldBe Nil

            storedOnBehalfOfBob shouldBe expectedAfterImport(
              Map(toc4_0 -> onprAcsImports4_0)
            )
          }).failOnShutdown
        }
      }
    }
}

object PartyReplicationIndexingStoreTest {
  type ContractIndexingInfo = (LfContractId, (ChangeType, ReassignmentCounter))
}
