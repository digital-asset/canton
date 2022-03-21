// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import java.util.UUID
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology._
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.{TransferData, TransferOutRequest}
import com.digitalasset.canton.participant.store.TransferStore._
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  suffixedId,
  transactionId,
}
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{RequestId, TransferId}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.util.{Checked, FutureUtil}
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait TransferStoreTest {
  this: AsyncWordSpec with BaseTest =>

  import TransferStoreTest._

  def transferStore(mk: DomainId => TransferStore): Unit = {
    val transferData = FutureUtil.noisyAwaitResult(
      mkTransferData(transfer10, mediator1),
      "make transfer data",
      10.seconds,
    )
    val transferOutResult = mkTransferOutResult(transferData)
    val withTransferOutResult = transferData.copy(transferOutResult = Some(transferOutResult))
    val toc = TimeOfChange(0L, CantonTimestamp.ofEpochSecond(3))

    "lookup" should {
      "find previously stored transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          lookup10 <- valueOrFail(store.lookup(transfer10))(
            "lookup failed to find the stored transfer"
          )
        } yield assert(lookup10 == transferData, "lookup finds the stored data")
      }

      "not invent transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          lookup10 <- store.lookup(transfer11).value
        } yield assert(
          lookup10 == Left(UnknownTransferId(transfer11)),
          "lookup finds the stored data",
        )
      }
    }

    "find" should {
      "filter by party" in {
        val store = mk(targetDomain)
        for {
          aliceTransfer <- mkTransferData(
            transfer10,
            mediator1,
            LfPartyId.assertFromString("alice"),
          )
          bobTransfer <- mkTransferData(transfer11, mediator1, LfPartyId.assertFromString("bob"))
          eveTransfer <- mkTransferData(transfer20, mediator2, LfPartyId.assertFromString("eve"))
          _ <- valueOrFail(store.addTransfer(aliceTransfer))("add alice failed")
          _ <- valueOrFail(store.addTransfer(bobTransfer))("add bob failed")
          _ <- valueOrFail(store.addTransfer(eveTransfer))("add eve failed")
          lookup <- store.find(None, None, Some(LfPartyId.assertFromString("bob")), 10)
        } yield {
          assert(lookup.toList == List(bobTransfer))
        }
      }

      "filter by timestamp" in {
        val store = mk(targetDomain)

        for {
          transfer1 <- mkTransferData(
            TransferId(domain1, CantonTimestamp.ofEpochMilli(100L)),
            mediator1,
          )
          transfer2 <- mkTransferData(
            TransferId(domain1, CantonTimestamp.ofEpochMilli(200L)),
            mediator1,
          )
          transfer3 <- mkTransferData(
            TransferId(domain1, CantonTimestamp.ofEpochMilli(300L)),
            mediator1,
          )
          _ <- valueOrFail(store.addTransfer(transfer1))("add1 failed")
          _ <- valueOrFail(store.addTransfer(transfer2))("add2 failed")
          _ <- valueOrFail(store.addTransfer(transfer3))("add3 failed")
          lookup <- store.find(None, Some(CantonTimestamp.Epoch.plusMillis(200L)), None, 10)
        } yield {
          assert(lookup.toList == List(transfer2))
        }
      }
      "filter by domain" in {
        val store = mk(targetDomain)
        for {
          transfer1 <- mkTransferData(
            TransferId(domain1, CantonTimestamp.ofEpochMilli(100L)),
            mediator1,
          )
          transfer2 <- mkTransferData(
            TransferId(domain2, CantonTimestamp.ofEpochMilli(200L)),
            mediator2,
          )
          _ <- valueOrFail(store.addTransfer(transfer1))("add1 failed")
          _ <- valueOrFail(store.addTransfer(transfer2))("add2 failed")
          lookup <- store.find(Some(domain2), None, None, 10)
        } yield {
          assert(lookup.toList == List(transfer2))
        }
      }
      "limit the number of results" in {
        val store = mk(targetDomain)
        for {
          transferData10 <- mkTransferData(transfer10, mediator1)
          transferData11 <- mkTransferData(transfer11, mediator1)
          transferData20 <- mkTransferData(transfer20, mediator2)
          _ <- valueOrFail(store.addTransfer(transferData10))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData11))("second add failed")
          _ <- valueOrFail(store.addTransfer(transferData20))("third add failed")
          lookup <- store.find(None, None, None, 2)
        } yield {
          assert(lookup.length == 2)
        }
      }
      "apply filters conjunctively" in {
        val store = mk(targetDomain)

        for {
          // Correct timestamp
          transfer1 <- mkTransferData(
            TransferId(domain1, CantonTimestamp.Epoch.plusMillis(200L)),
            mediator1,
            LfPartyId.assertFromString("party1"),
          )
          // Correct submitter
          transfer2 <- mkTransferData(
            TransferId(domain1, CantonTimestamp.Epoch.plusMillis(100L)),
            mediator1,
            LfPartyId.assertFromString("party2"),
          )
          // Correct domain
          transfer3 <- mkTransferData(
            TransferId(domain2, CantonTimestamp.Epoch.plusMillis(100L)),
            mediator2,
            LfPartyId.assertFromString("party2"),
          )
          // Correct transfer
          transfer4 <- mkTransferData(
            TransferId(domain2, CantonTimestamp.Epoch.plusMillis(200L)),
            mediator2,
            LfPartyId.assertFromString("party2"),
          )
          _ <- valueOrFail(store.addTransfer(transfer1))("first add failed")
          _ <- valueOrFail(store.addTransfer(transfer2))("second add failed")
          _ <- valueOrFail(store.addTransfer(transfer3))("third add failed")
          _ <- valueOrFail(store.addTransfer(transfer4))("fourth add failed")
          lookup <- store.find(
            Some(domain2),
            Some(CantonTimestamp.Epoch.plusMillis(200L)),
            Some(LfPartyId.assertFromString("party2")),
            10,
          )
        } yield { assert(lookup.toList == List(transfer4)) }

      }
    }

    "addTransfer" should {
      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData))("second add failed")
        } yield succeed
      }

      "detect modified transfer data" in {
        val store = mk(targetDomain)
        val modifiedContract =
          asSerializable(
            transferData.contract.contractId,
            contractInstance(),
            contract.metadata,
            CantonTimestamp.ofEpochMilli(1),
          )
        val transferDataModified = transferData.copy(contract = modifiedContract)

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("first add failed")
          add2 <- store.addTransfer(transferDataModified).value
        } yield assert(
          add2 == Left(TransferDataAlreadyExists(transferData, transferDataModified)),
          "second add failed",
        )
      }

      "handle transfer-out results" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(withTransferOutResult))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData))("second add failed")
          lookup2 <- valueOrFail(store.lookup(transfer10))("TransferOutResult missing")
          _ <- valueOrFail(store.addTransfer(withTransferOutResult))("third add failed")
        } yield assert(lookup2 == withTransferOutResult, "TransferOutResult remains")
      }

      "add several transfers" in {
        val store = mk(targetDomain)
        for {
          transferData10 <- mkTransferData(transfer10, mediator1)
          transferData11 <- mkTransferData(transfer11, mediator1)
          transferData20 <- mkTransferData(transfer20, mediator2)
          _ <- valueOrFail(store.addTransfer(transferData10))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData11))("second add failed")
          _ <- valueOrFail(store.addTransfer(transferData20))("third add failed")
          lookup10 <- valueOrFail(store.lookup(transfer10))("first transfer not found")
          lookup11 <- valueOrFail(store.lookup(transfer11))("second transfer not found")
          lookup20 <- valueOrFail(store.lookup(transfer20))("third transfer not found")
        } yield {
          lookup10 shouldBe transferData10
          lookup11 shouldBe transferData11
          lookup20 shouldBe transferData20
        }
      }

      "complain about transfers for a different domain" in {
        val store = mk(domain1)
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.addTransfer(transferData),
          _.getMessage shouldBe "Domain domain1::DOMAIN1: Transfer store cannot store transfer for domain target::DOMAIN",
        )
      }
    }

    "addTransferOutResult" should {

      "report missing transfers" in {
        val store = mk(targetDomain)
        for {
          missing <- store.addTransferOutResult(transferOutResult).value
        } yield missing shouldBe Left(UnknownTransferId(transfer10))
      }

      "add the result" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          lookup <- valueOrFail(store.lookup(transfer10))("transfer not found")
        } yield assert(
          lookup == transferData.copy(transferOutResult = Some(transferOutResult)),
          "result is stored",
        )
      }

      "report mismatching results" in {
        val store = mk(targetDomain)
        val modifiedTransferOutResult = transferOutResult.copy(
          result = transferOutResult.result.copy(
            content =
              transferOutResult.result.content.copy(timestamp = CantonTimestamp.ofEpochSecond(2))
          )
        )
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          modified <- store.addTransferOutResult(modifiedTransferOutResult).value
          lookup <- valueOrFail(store.lookup(transfer10))("transfer not found")
        } yield {
          assert(
            modified == Left(
              TransferOutResultAlreadyExists(
                transfer10,
                transferOutResult,
                modifiedTransferOutResult,
              )
            ),
            "modified result is flagged",
          )
          assert(
            lookup == transferData.copy(transferOutResult = Some(transferOutResult)),
            "result is not overwritten stored",
          )
        }
      }
    }

    "completeTransfer" should {
      "mark the transfer as completed" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("completion failed")
          lookup <- store.lookup(transfer10).value
        } yield lookup shouldBe Left(TransferCompleted(transfer10, toc))
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("first completion failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("second completion failed")
        } yield succeed
      }

      "be allowed before the result" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("first completion failed")
          lookup1 <- store.lookup(transfer10).value
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          lookup2 <- store.lookup(transfer10).value
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("second completion failed")
        } yield {
          lookup1 shouldBe Left(TransferCompleted(transfer10, toc))
          lookup2 shouldBe Left(TransferCompleted(transfer10, toc))
        }
      }

      "detect mismatches" in {
        val store = mk(targetDomain)
        val toc2 = TimeOfChange(0L, CantonTimestamp.ofEpochSecond(4))
        val modifiedTransferData = transferData.copy(transferOutRequestCounter = 100L)
        val modifiedTransferOutResult = transferOutResult.copy(
          result = transferOutResult.result.copy(content =
            transferOutResult.result.content.copy(counter = 120L)
          )
        )

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("first completion failed")
          complete2 <- store.completeTransfer(transfer10, toc2).value
          add2 <- store.addTransfer(modifiedTransferData).value
          addResult2 <- store.addTransferOutResult(modifiedTransferOutResult).value
        } yield {
          complete2 shouldBe Checked.continue(TransferAlreadyCompleted(transfer10, toc2))
          add2 shouldBe Left(TransferDataAlreadyExists(withTransferOutResult, modifiedTransferData))
          addResult2 shouldBe Left(
            TransferOutResultAlreadyExists(transfer10, transferOutResult, modifiedTransferOutResult)
          )
        }
      }

      "store the first completion" in {
        val store = mk(targetDomain)
        val toc2 = TimeOfChange(1L, CantonTimestamp.ofEpochSecond(4))
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc2))("later completion failed")
          complete2 <- store.completeTransfer(transfer10, toc).value
          lookup <- store.lookup(transfer10).value
        } yield {
          complete2 shouldBe Checked.continue(TransferAlreadyCompleted(transfer10, toc))
          lookup shouldBe Left(TransferCompleted(transfer10, toc2))
        }
      }

    }

    "delete" should {
      "remove the transfer" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- store.deleteTransfer(transfer10)
          lookup <- store.lookup(transfer10).value
        } yield lookup shouldBe Left(UnknownTransferId(transfer10))
      }

      "purge completed transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("completion failed")
          _ <- store.deleteTransfer(transfer10)
        } yield succeed
      }

      "ignore unknown transfer IDs" in {
        val store = mk(targetDomain)
        for {
          () <- store.deleteTransfer(transfer10)
        } yield succeed
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          () <- store.deleteTransfer(transfer10)
          () <- store.deleteTransfer(transfer10)
        } yield succeed
      }
    }

    "transfer stores should be isolated" in {
      val storeTarget = mk(targetDomain)
      val store1 = mk(domain1)
      for {
        _ <- valueOrFail(storeTarget.addTransfer(transferData))("add failed")
        found <- store1.lookup(transferData.transferId).value
      } yield found shouldBe Left(UnknownTransferId(transferData.transferId))
    }

    "deleteCompletionsSince" should {
      "remove the completions from the criterion on" in {
        val store = mk(targetDomain)
        val toc1 = TimeOfChange(1L, CantonTimestamp.ofEpochSecond(5))
        val toc2 = TimeOfChange(2L, CantonTimestamp.ofEpochSecond(7))

        for {
          aliceTransfer <-
            mkTransferData(transfer10, mediator1, LfPartyId.assertFromString("alice"))
          bobTransfer <- mkTransferData(transfer11, mediator1, LfPartyId.assertFromString("bob"))
          eveTransfer <- mkTransferData(transfer20, mediator2, LfPartyId.assertFromString("eve"))
          _ <- valueOrFail(store.addTransfer(aliceTransfer))("add alice failed")
          _ <- valueOrFail(store.addTransfer(bobTransfer))("add bob failed")
          _ <- valueOrFail(store.addTransfer(eveTransfer))("add eve failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("completion alice failed")
          _ <- valueOrFail(store.completeTransfer(transfer11, toc1))("completion bob failed")
          _ <- valueOrFail(store.completeTransfer(transfer20, toc2))("completion eve failed")
          _ <- store.deleteCompletionsSince(1L)
          alice <- leftOrFail(store.lookup(transfer10))("alice must still be completed")
          bob <- valueOrFail(store.lookup(transfer11))("bob must not be completed")
          eve <- valueOrFail(store.lookup(transfer20))("eve must not be completed")
          _ <- valueOrFail(store.completeTransfer(transfer11, toc2))("second completion bob failed")
          _ <- valueOrFail(store.completeTransfer(transfer20, toc1))("second completion eve failed")
        } yield {
          alice shouldBe TransferCompleted(transfer10, toc)
          bob shouldBe bobTransfer
          eve shouldBe eveTransfer
        }
      }
    }
  }
}

object TransferStoreTest {

  val coidAbs1 = suffixedId(1, 0)
  val contract = asSerializable(
    coidAbs1,
    contractInstance = contractInstance(),
    ledgerTime = CantonTimestamp.Epoch,
  )
  val transactionId1 = transactionId(1)

  val domain1 = DomainId(UniqueIdentifier.tryCreate("domain1", "DOMAIN1"))
  val mediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator1", "DOMAIN1"))
  val domain2 = DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2"))
  val mediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator2", "DOMAIN2"))
  val targetDomain = DomainId(UniqueIdentifier.tryCreate("target", "DOMAIN"))

  val transfer10 = TransferId(domain1, CantonTimestamp.Epoch)
  val transfer11 = TransferId(domain1, CantonTimestamp.ofEpochMilli(1))
  val transfer20 = TransferId(domain2, CantonTimestamp.Epoch)

  val loggerFactoryNotUsed = NamedLoggerFactory.unnamedKey("test", "NotUsed-TransferStoreTest")
  implicit val ec = DirectExecutionContext(
    TracedLogger(loggerFactoryNotUsed.getLogger(TransferStoreTest.getClass))
  )
  val cryptoFactory =
    TestingIdentityFactory(loggerFactoryNotUsed).forOwnerAndDomain(DefaultTestIdentities.sequencer)
  val sequencerKey =
    TestingIdentityFactory(loggerFactoryNotUsed)
      .newSigningPublicKey(DefaultTestIdentities.sequencer)
      .fingerprint
  val privateCrypto = cryptoFactory.crypto.privateCrypto
  val pureCryptoApi: CryptoPureApi = cryptoFactory.pureCrypto

  def sign(str: String): Signature = {
    val hash =
      pureCryptoApi.build(HashPurpose.TransferResultSignature).addWithoutLengthPrefix(str).finish()
    Await.result(
      privateCrypto
        .sign(hash, sequencerKey)
        .valueOr(err => throw new RuntimeException(err.toString)),
      10.seconds,
    )
  }
  val seedGenerator = new SeedGenerator(privateCrypto, pureCryptoApi)

  def mkTransferDataForDomain(
      transferId: TransferId,
      originMediator: MediatorId,
      submittingParty: LfPartyId = LfPartyId.assertFromString("submitter"),
      targetDomainId: DomainId,
  ): Future[TransferData] = {
    val transferOutRequest = TransferOutRequest(
      submittingParty,
      Set(submittingParty),
      Set.empty,
      coidAbs1,
      transferId.originDomain,
      originMediator,
      targetDomainId,
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, domainId = targetDomainId),
    )
    val uuid = new UUID(10L, 0L)
    for {
      seed <- seedGenerator
        .generateSeedForTransferOut(transferOutRequest, uuid)
        .valueOr(err => throw new IllegalStateException(err.toString))
    } yield {
      val fullTransferOutViewTree =
        transferOutRequest.toFullTransferOutTree(pureCryptoApi, pureCryptoApi, seed, uuid)
      TransferData(
        transferId.requestTimestamp,
        0L,
        fullTransferOutViewTree,
        CantonTimestamp.ofEpochSecond(10),
        contract,
        transactionId1,
        None,
      )
    }
  }

  private def mkTransferData(
      transferId: TransferId,
      originMediator: MediatorId,
      submitter: LfPartyId = LfPartyId.assertFromString("submitter"),
  ) =
    mkTransferDataForDomain(transferId, originMediator, submitter, targetDomain)

  def mkTransferOutResult(transferData: TransferData): DeliveredTransferOutResult =
    DeliveredTransferOutResult {
      val requestId = RequestId(transferData.transferOutTimestamp)

      val mediatorMessage = transferData.transferOutRequest.tree.mediatorMessage
      val result = mediatorMessage.createMediatorResult(
        requestId,
        Verdict.Approve,
        mediatorMessage.allInformees,
      )
      val signedResult = SignedProtocolMessage(result, sign("TransferOutResult-mediator"))
      val batch = Batch.of(signedResult -> RecipientsTest.testInstance)
      val deliver =
        Deliver.create(
          1L,
          CantonTimestamp.ofEpochMilli(10),
          transferData.originDomain,
          Some(MessageId.tryCreate("1")),
          batch,
        )
      SignedContent(
        deliver,
        sign("TransferOutResult-sequencer"),
        Some(transferData.transferOutTimestamp),
      )
    }
}
