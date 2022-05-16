// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data._
import com.digitalasset.canton.domain.mediator.store.{InMemoryFinalizedResponseStore, MediatorState}
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MediatorStateTest extends AsyncWordSpec with BaseTest {

  "MediatorState" when {
    val requestId = RequestId(CantonTimestamp.Epoch)
    val fullInformeeTree = {
      val domainId = DefaultTestIdentities.domainId
      val mediatorId = DefaultTestIdentities.mediator
      val alice = PlainInformee(LfPartyId.assertFromString("alice"))
      val bob = ConfirmingParty(LfPartyId.assertFromString("bob"), 2)
      val hashOps: HashOps = new SymbolicPureCrypto
      val h: Int => Hash = TestHash.digest
      val s: Int => Salt = TestSalt.generateSalt
      def rh(index: Int): RootHash = RootHash(h(index))
      val viewCommonData =
        ViewCommonData.create(hashOps)(Set(alice, bob), NonNegativeInt.tryCreate(2), s(999))
      val view = TransactionView(hashOps)(viewCommonData, BlindedNode(rh(0)), Nil)
      val commonMetadata = CommonMetadata(hashOps)(
        ConfirmationPolicy.Signatory,
        domainId,
        mediatorId,
        s(5417),
        new UUID(0, 0),
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
    val currentVersion = ResponseAggregation(requestId, informeeMessage)(loggerFactory)

    def mediatorState: MediatorState = {
      val sut =
        new MediatorState(
          new InMemoryFinalizedResponseStore(loggerFactory),
          DomainTestMetrics.mediator,
          timeouts,
          loggerFactory,
        )
      Await.result(sut.add(currentVersion), 1.second)
      sut
    }

    "fetching unfinalized items" should {
      val sut = mediatorState
      "respect the limit filter" in {
        sut.pendingRequestIdsBefore(CantonTimestamp.MinValue) shouldBe empty
        sut.pendingRequestIdsBefore(
          CantonTimestamp.MaxValue
        ) should contain only currentVersion.requestId
        Future.successful(succeed)
      }
      "have no more unfinalized after finalization" in {
        for {
          _ <- sut.replace(currentVersion, currentVersion.timeout(currentVersion.version)).value
        } yield {
          sut.pendingRequestIdsBefore(CantonTimestamp.MaxValue) shouldBe empty
        }
      }
    }

    "fetching items" should {
      "fetch only existing items" in {
        val sut = mediatorState
        for {
          progress <- sut.fetch(requestId).value
          noItem <- sut.fetch(RequestId(CantonTimestamp.MinValue)).value
        } yield {
          progress shouldBe Right(currentVersion)
          noItem shouldBe Left(MediatorRequestNotFound(RequestId(CantonTimestamp.MinValue)))
        }
      }
    }

    "updating items" should {
      val sut = mediatorState
      val newVersionTs = currentVersion.version.plusSeconds(1)
      val newVersion = currentVersion.copy(version = newVersionTs)(
        currentVersion.requestTraceContext
      )(loggerFactory)

      // this should be handled by the processor that shouldn't be requesting the replacement
      "prevent updating to the same version" in {
        sut.replace(newVersion, newVersion).value.map { result =>
          result shouldBe Left(StaleVersion(requestId, newVersionTs, currentVersion.version))
        }
      }

      "allow updating to a newer version" in {
        for {
          result <- sut.replace(currentVersion, newVersion).value
        } yield result shouldBe Right(())
      }
    }
  }
}
