// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.OptionT
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractReassignment,
  ContractsReassignmentBatch,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.{
  ActivationChange,
  ContractActivationChangeBatch,
  Watermark,
}
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStoreTest.ContractIndexingInfo
import com.digitalasset.canton.protocol.{ExampleContractFactory, LfContractId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPackageId,
  ProtocolVersionChecksAsyncWordSpec,
  ReassignmentCounter,
}
import com.digitalasset.nonempty.NonEmpty
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpecLike

trait PartyReplicationIndexingStoreTest
    extends AsyncWordSpecLike
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  protected lazy val storeSynchronizerStr: String300 =
    String300.tryCreate("party-replication-indexing-store::default")
  protected lazy val storeSynchronizerId: SynchronizerId =
    SynchronizerId.tryFromString(storeSynchronizerStr.unwrap)
  private val ts1 = CantonTimestamp.ofEpochSecond(1)
  private val ts2 = CantonTimestamp.ofEpochSecond(2)
  private val ts4 = CantonTimestamp.ofEpochSecond(4)
  private val sourceValidationPackageId = Source(
    LfPackageId.assertFromString("source-validation-package-id")
  )
  private val targetValidationPackageId = Target(
    LfPackageId.assertFromString("target-validation-package-id")
  )
  private def generateReassignments(
      howManyReassignments: NonNegativeLong
  ): NonEmpty[Seq[ContractReassignment]] =
    ContractsReassignmentBatch
      .create((0L until howManyReassignments.unwrap).map { ctr =>
        (
          ExampleContractFactory.build(),
          sourceValidationPackageId,
          targetValidationPackageId,
          ReassignmentCounter.apply(ctr),
        )
      })
      .valueOrFail("Failed to create reassignment batch")
      .contracts

  private def expectedAfterImport(
      ts: CantonTimestamp,
      acsImport: NonEmpty[Seq[ContractReassignment]],
  ): Seq[ActivationChange] = acsImport.zipWithIndex.map {
    case (ContractReassignment(contract, _, _, ctr), index) =>
      ActivationChange(
        Watermark(ts, NonNegativeLong.tryCreate(index.toLong)),
        contract.contractId,
        ChangeType.Activation,
        false,
        false,
        ctr,
      )
  }

  private def indexerBatch: ActivationChange => ContractIndexingInfo = {
    case ActivationChange(_, cid, change, _, _, ctr) =>
      cid -> (change, ctr)
  }

  private def markIndexing: PartialFunction[ActivationChange, ActivationChange] = {
    case ActivationChange(toc, cid, change, _, false, ctr) =>
      ActivationChange(toc, cid, change, true, false, ctr)
  }

  private def markIndexed: PartialFunction[ActivationChange, ActivationChange] = {
    case ActivationChange(toc, cid, change, true, false, ctr) =>
      ActivationChange(toc, cid, change, true, true, ctr)
  }

  private def expectedBatch(
      activationChanges: Seq[ActivationChange]
  ): NonEmpty[Seq[(LfContractId, (ActiveContractStore.ChangeType, ReassignmentCounter))]] =
    valueOrFail(NonEmpty.from(activationChanges.map(indexerBatch)))("expect non empty")

  def partyReplicationIndexingStore(mk: () => PartyReplicationIndexingStore): Unit =
    "PartyReplicationIndexingStore" when {
      val (sizeImport1, sizeImport2, sizeImport3, sizeImport4) =
        (
          NonNegativeLong.tryCreate(3L),
          NonNegativeLong.tryCreate(10L),
          NonNegativeLong.tryCreate(4L),
          NonNegativeLong.one,
        )
      val (onprAcsImports1, watermark1) =
        (generateReassignments(sizeImport1), Watermark(ts1, NonNegativeLong.zero))
      val (onprAcsImports2, watermark2) =
        (generateReassignments(sizeImport2), Watermark(ts1, sizeImport1))
      val (onprAcsImports3, watermark3) =
        (generateReassignments(sizeImport3), Watermark(ts1, sizeImport1 + sizeImport2))
      val (onprAcsImports4, watermark4) =
        (
          generateReassignments(sizeImport4),
          Watermark(ts1, sizeImport1 + sizeImport2 + sizeImport3),
        )

      "addImportedContractActivations" should {
        // TODO(#26775): Change from Protocol.dev to Protocol.v3x
        "insert contract activations" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          val store = mk()
          (for {
            storedAtGenesis <- store.listContractActivationChanges()
            _ <- store.addImportedContractActivations(watermark1, onprAcsImports1)
            storedAt1 <- store.listContractActivationChanges()
            _ <- store.addImportedContractActivations(watermark2, onprAcsImports2)
            // double insert is ignored
            _ <- store.addImportedContractActivations(watermark2, onprAcsImports2)
            storedAt2 <- store.listContractActivationChanges()
            _ <- store.addImportedContractActivations(watermark3, onprAcsImports3)
            storedAt3 <- store.listContractActivationChanges()
          } yield {
            storedAtGenesis shouldBe Seq.empty
            storedAt1 shouldBe expectedAfterImport(ts1, onprAcsImports1)
            storedAt2 shouldBe expectedAfterImport(ts1, onprAcsImports1 ++ onprAcsImports2)
            storedAt3 shouldBe expectedAfterImport(
              ts1,
              onprAcsImports1 ++ onprAcsImports2 ++ onprAcsImports3,
            )
          }).failOnShutdown
        }
      }

      "consumeNextActivationChangesBatch" should {
        "update indexing contract activation changes" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          val store = mk()
          val maxBatchSize = PositiveInt.tryCreate(5)

          def nonEmpty[T](
              fus: => FutureUnlessShutdown[Option[T]]
          ): String => FutureUnlessShutdown[T] = valueOrFailUS(OptionT(fus))

          (for {
            _ <- store.addImportedContractActivations(watermark1, onprAcsImports1)
            _ <- store.addImportedContractActivations(watermark2, onprAcsImports2)
            _ <- store.addImportedContractActivations(watermark3, onprAcsImports3)
            storedAfterImport <- store.listContractActivationChanges()
            batch0 <- nonEmpty(store.consumeNextActivationChangesBatch(maxBatchSize))("batch0")
            storedAfterIndexingBatch0 <- store.listContractActivationChanges()
            batch1 <- nonEmpty(store.consumeNextActivationChangesBatch(maxBatchSize))("batch1")
            storedAfterIndexingBatch1 <- store.listContractActivationChanges()
            batch2 <- nonEmpty(store.consumeNextActivationChangesBatch(maxBatchSize))("batch2")
            storedAfterIndexingBatch2 <- store.listContractActivationChanges()
            batch3 <- nonEmpty(store.consumeNextActivationChangesBatch(maxBatchSize))("batch3")
            batch4O <- store.consumeNextActivationChangesBatch(maxBatchSize)

            storedAfterIndexingBatch3 <- store.listContractActivationChanges()
            _ <- store.markContractActivationChangesAsIndexed(batch0.onprBatchWatermark)
            storedAfterIndexedBatch0 <- store.listContractActivationChanges()
            _ <- store.markContractActivationChangesAsIndexed(batch1.onprBatchWatermark)
            storedAfterIndexedBatch1 <- store.listContractActivationChanges()
            _ <- store.markContractActivationChangesAsIndexed(batch2.onprBatchWatermark)
            storedAfterIndexedBatch2 <- store.listContractActivationChanges()
            _ <- store.markContractActivationChangesAsIndexed(batch3.onprBatchWatermark)
            storedAfterIndexedBatch3 <- store.listContractActivationChanges()

            _ <- store.purgeContractActivationChanges()
            _ <- store.addImportedContractActivations(
              Watermark(ts4, NonNegativeLong.zero),
              onprAcsImports4,
            )
            storedOnBehalfOfBob <- store.listContractActivationChanges()
          } yield {
            val activationsAfterImport =
              expectedAfterImport(ts1, onprAcsImports1 ++ onprAcsImports2 ++ onprAcsImports3)
            storedAfterImport shouldBe activationsAfterImport

            val (batch0Head, batch0Tail) = activationsAfterImport.splitAt(maxBatchSize.unwrap)
            batch0.activationChanges shouldBe expectedBatch(batch0Head)
            val batch0Indexing = batch0Head.collect(markIndexing)
            storedAfterIndexingBatch0 shouldBe batch0Indexing ++ batch0Tail

            val (batch1Head, batch1Tail) = batch0Tail.splitAt(maxBatchSize.unwrap)
            batch1.activationChanges shouldBe expectedBatch(batch1Head)
            val batch1Indexing = batch1Head.collect(markIndexing)
            storedAfterIndexingBatch1 shouldBe batch0Indexing ++ batch1Indexing ++ batch1Tail

            val (batch2Head, batch2Tail) = batch1Tail.splitAt(maxBatchSize.unwrap)
            batch2.activationChanges shouldBe expectedBatch(batch2Head)
            val batch2Indexing = batch2Head.collect(markIndexing)
            storedAfterIndexingBatch2 shouldBe batch0Indexing ++ batch1Indexing ++ batch2Indexing ++ batch2Tail

            val (batch3Head, batch3Tail) = batch2Tail.splitAt(maxBatchSize.unwrap)
            batch3.activationChanges shouldBe expectedBatch(batch3Head)
            val batch3Indexing = batch3Head.collect(markIndexing)
            storedAfterIndexingBatch3 shouldBe batch0Indexing ++ batch1Indexing ++ batch2Indexing ++ batch3Indexing
            batch3Tail shouldBe Nil

            batch4O shouldBe None

            val batch0Indexed = batch0Indexing.collect(markIndexed)
            val batch1Indexed = batch1Indexing.collect(markIndexed)
            val batch2Indexed = batch2Indexing.collect(markIndexed)
            val batch3Indexed = batch3Indexing.collect(markIndexed)
            storedAfterIndexedBatch0 shouldBe batch0Indexed ++ batch1Indexing ++ batch2Indexing ++ batch3Indexing
            storedAfterIndexedBatch1 shouldBe batch0Indexed ++ batch1Indexed ++ batch2Indexing ++ batch3Indexing
            storedAfterIndexedBatch2 shouldBe batch0Indexed ++ batch1Indexed ++ batch2Indexed ++ batch3Indexing
            storedAfterIndexedBatch3 shouldBe batch0Indexed ++ batch1Indexed ++ batch2Indexed ++ batch3Indexed

            storedOnBehalfOfBob shouldBe expectedAfterImport(ts4, onprAcsImports4)
          }).failOnShutdown
        }

        "trims changes to the same contracts into different batches" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          val store = mk()
          val contractReassignment = onprAcsImports1.take(1)
          (for {
            // Activate the first contract
            _ <- store.addContractActivationChanges(
              ts1,
              contractReassignment.map { case ContractReassignment(contract, _, _, ctr) =>
                contract.contractId -> (ActiveContractStore.ChangeType.Activation, ctr)
              },
            )
            // Deactivate the same contract
            _ <- store.addContractActivationChanges(
              ts2,
              contractReassignment.map { case ContractReassignment(contract, _, _, ctr) =>
                contract.contractId -> (ActiveContractStore.ChangeType.Deactivation, ctr)
              },
            )
            batch1 <- store.consumeNextActivationChangesBatch(maxBatchSize =
              PositiveInt.tryCreate(5)
            )
            batch2 <- store.consumeNextActivationChangesBatch(maxBatchSize =
              PositiveInt.tryCreate(5)
            )
          } yield {
            val contractInfo = contractReassignment.head
            // batch1 only contains activation of the contracts
            batch1 shouldBe Some(
              ContractActivationChangeBatch(
                NonEmpty(
                  Seq,
                  contractInfo.contract.contractId -> (ActiveContractStore.ChangeType.Activation, contractInfo.counter),
                ),
                Watermark(ts1, NonNegativeLong.zero),
              )
            )
            // while batch2 holds the deactivation, showing that the same contract does not end up in the
            // same batch multiple times
            batch2 shouldBe Some(
              ContractActivationChangeBatch(
                NonEmpty(
                  Seq,
                  contractInfo.contract.contractId -> (ActiveContractStore.ChangeType.Deactivation, contractInfo.counter),
                ),
                Watermark(ts2, NonNegativeLong.zero),
              )
            )
          }).failOnShutdown
        }
      }

      "addContractActivationChanges" should {
        "append to store when interleaved with acs import" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          val store = mk()

          (for {
            _ <- store.addImportedContractActivations(watermark1, onprAcsImports1)
            _ <- store.addContractActivationChanges(
              ts2,
              onprAcsImports3.map { case ContractReassignment(contract, _, _, ctr) =>
                contract.contractId -> (ActiveContractStore.ChangeType.Activation: ActiveContractStore.ChangeType, ctr)
              } ++ onprAcsImports2.map { case ContractReassignment(contract, _, _, ctr) =>
                contract.contractId -> (ActiveContractStore.ChangeType.Deactivation, ctr)
              },
            )
            _ <- store.addImportedContractActivations(watermark2, onprAcsImports2)
            storedAfterImportAndConcurrentTransactions <- store.listContractActivationChanges()
          } yield {
            storedAfterImportAndConcurrentTransactions shouldBe expectedAfterImport(
              ts1,
              onprAcsImports1 ++ onprAcsImports2,
            ) ++
              expectedAfterImport(ts2, onprAcsImports3) ++
              (expectedAfterImport(ts2, onprAcsImports2).map(
                _.focus(_.change)
                  // batch 2 has been deactivated
                  .replace(ChangeType.Deactivation)
                  // and needs to be shifted by the size of batch 3 activated before at the same timestamp
                  .focus(_.watermark.counter)
                  .modify(_ + sizeImport3)
              ))
          }).failOnShutdown
        }
      }

      "properly retain unindexed status for remaining events after a mid-stream deleteSince" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val store = mk()
        val batchSize = PositiveInt.tryCreate(sizeImport1.increment.unwrap.toInt)

        val changesAtTs3 = onprAcsImports2.map { case ContractReassignment(contract, _, _, ctr) =>
          contract.contractId -> (ActiveContractStore.ChangeType.Activation, ctr)
        }.forgetNE

        (for {
          _ <- store.addImportedContractActivations(watermark1, onprAcsImports1)
          deleteSinceWatermark = Watermark(ts2, NonNegativeLong.zero)
          _ <- store.addImportedContractActivations(deleteSinceWatermark, onprAcsImports2)
          _ <- store.addContractActivationChanges(ts4, changesAtTs3)

          // Move indexing watermark past deleteSinceWatermark
          _ <- store.consumeNextActivationChangesBatch(batchSize)
          _ <- store.deleteSince(deleteSinceWatermark.timestamp)

          // Reimport
          _ <- store.addImportedContractActivations(deleteSinceWatermark, onprAcsImports2)
          changesAfterFirstDeleteSince <- store.listContractActivationChanges()

          // Delete everything and check watermarks
          _ <- store.deleteSince(CantonTimestamp.MinValue)
          _ <- store.addImportedContractActivations(watermark1, onprAcsImports1)
          changesAfterDeleteAll <- store.listContractActivationChanges()
        } yield {
          changesAfterFirstDeleteSince.foreach {
            case ac @ ActivationChange(Watermark(ts, _), _, _, isIndexing, _, _) =>
              if (ts >= deleteSinceWatermark.timestamp) {
                isIndexing shouldBe false
              }
          }
          changesAfterFirstDeleteSince shouldNot be(empty)

          changesAfterDeleteAll.foreach { case ac @ ActivationChange(_, _, _, isIndexing, _, _) =>
            isIndexing shouldBe false
          }
          changesAfterDeleteAll shouldNot be(empty)
        }).failOnShutdown
      }
    }
}

object PartyReplicationIndexingStoreTest {
  type ContractIndexingInfo = (LfContractId, (ChangeType, ReassignmentCounter))
}
