// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.syntax.traverse._
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data._
import com.digitalasset.canton.domain.mediator.{MediatorRequestNotFound, ResponseAggregation}
import com.digitalasset.canton.protocol.messages.{InformeeMessage, Verdict}
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RequestId, RootHash}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingIdentityFactory}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfPartyId}
import io.functionmeta.functionFullName
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

trait FinalizedResponseStoreTest extends BeforeAndAfterAll {
  this: AsyncWordSpec with BaseTest =>

  def ts(n: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(n.toLong)
  def requestIdTs(n: Int): RequestId = RequestId(ts(n))

  val requestId = RequestId(CantonTimestamp.Epoch)
  val fullInformeeTree = {
    val domainId = DefaultTestIdentities.domainId
    val mediatorId = DefaultTestIdentities.mediator

    val alice = PlainInformee(LfPartyId.assertFromString("alice"))
    val bob = ConfirmingParty(LfPartyId.assertFromString("bob"), 2)
    val hashOps = new SymbolicPureCrypto
    def h(i: Int): Hash = TestHash.digest(i)
    def rh(index: Int): RootHash = RootHash(h(index))
    def s(i: Int): Salt = TestSalt.generateSalt(i)
    val viewCommonData =
      ViewCommonData.create(hashOps)(
        Set(alice, bob),
        NonNegativeInt.tryCreate(2),
        s(999),
        defaultProtocolVersion,
      )
    val view = TransactionView(hashOps)(viewCommonData, BlindedNode(rh(0)), Nil)
    val commonMetadata = CommonMetadata(hashOps)(
      ConfirmationPolicy.Signatory,
      domainId,
      mediatorId,
      s(5417),
      new UUID(0L, 0L),
      defaultProtocolVersion,
    )
    FullInformeeTree(
      GenTransactionTree(hashOps)(
        BlindedNode(rh(11)),
        commonMetadata,
        BlindedNode(rh(12)),
        MerkleSeq.fromSeq(hashOps)(view :: Nil),
      )
    )
  }
  val informeeMessage = InformeeMessage(fullInformeeTree)
  val currentVersion =
    ResponseAggregation(
      requestId,
      informeeMessage,
      requestId.unwrap,
      Verdict.Timeout,
      TraceContext.empty,
    )(loggerFactory)

  def finalizedResponseStore(mk: () => FinalizedResponseStore): Unit = {
    "when storing responses" should {
      "get error message if trying to fetch a non existing response" in {
        val sut = mk()
        sut.fetch(requestId).value.map { result =>
          result shouldBe Left(MediatorRequestNotFound(requestId))
        }
      }
      "should be able to fetch previously stored response" in {
        val sut = mk()
        for {
          _ <- sut.store(currentVersion)
          result <- sut.fetch(requestId).value
        } yield result shouldBe Right(currentVersion)
      }
      "should allow the same response to be stored more than once" in {
        // can happen after a crash and event replay
        val sut = mk()
        for {
          _ <- sut.store(currentVersion)
          _ <- sut.store(currentVersion)
        } yield succeed
      }
    }

    "pruning" should {
      "remove all responses up and including timestamp" in {
        val sut = mk()

        val requests = (1 to 3).map(n =>
          currentVersion.copy(requestId = requestIdTs(n))(currentVersion.requestTraceContext)(
            loggerFactory
          )
        )

        for {
          _ <- requests.toList.traverse(sut.store)
          _ <- sut.prune(ts(2))
          error1 <- leftOrFail(sut.fetch(requestIdTs(1)))("fetch(ts1)")
          error2 <- leftOrFail(sut.fetch(requestIdTs(2)))("fetch(ts2)")
          _ <- valueOrFail(sut.fetch(requestIdTs(3)))("fetch(ts3)")
        } yield {
          error1 shouldBe MediatorRequestNotFound(requestIdTs(1))
          error2 shouldBe MediatorRequestNotFound(requestIdTs(2))
        }
      }
    }
  }
}

class FinalizedResponseStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with FinalizedResponseStoreTest {
  "InMemoryFinalizedResponseStore" should {
    behave like finalizedResponseStore(() => new InMemoryFinalizedResponseStore(loggerFactory))
  }
}

trait DbFinalizedResponseStoreTest
    extends AsyncWordSpec
    with BaseTest
    with FinalizedResponseStoreTest {
  this: DbTest =>

  val pureCryptoApi: CryptoPureApi = TestingIdentityFactory.pureCrypto()

  def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api._
    storage.update(sqlu"truncate table response_aggregations", functionFullName)
  }
  "DbFinalizedResponseStore" should {
    behave like finalizedResponseStore(() =>
      new DbFinalizedResponseStore(
        storage,
        pureCryptoApi,
        defaultProtocolVersion,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class FinalizedResponseStoreTestH2 extends DbFinalizedResponseStoreTest with H2Test

class FinalizedResponseStoreTestPostgres extends DbFinalizedResponseStoreTest with PostgresTest
