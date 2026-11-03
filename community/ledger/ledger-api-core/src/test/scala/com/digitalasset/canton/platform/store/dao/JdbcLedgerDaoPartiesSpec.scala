// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import io.grpc.StatusRuntimeException
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

private[dao] trait JdbcLedgerDaoPartiesSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite with OptionValues =>

  behavior of "JdbcLedgerDao (parties)"

  it should "store and retrieve all parties" in {
    val alice = IndexerPartyDetails(
      party = Ref.Party.assertFromString(s"Alice-${UUID.randomUUID()}"),
      isLocal = true,
    )
    val bob = IndexerPartyDetails(
      party = Ref.Party.assertFromString(s"Bob-${UUID.randomUUID()}"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(alice, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(bob, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- listKnownParties(None, filterParty = None, 1000)
    } yield {
      parties should contain.allOf(alice, bob)
    }
  }

  it should "filter parties" in {
    val randomSuffix = UUID.randomUUID()
    val alice = IndexerPartyDetails(
      party = "FilterAlice::" + randomSuffix,
      isLocal = true,
    )
    val bob = IndexerPartyDetails(
      party = "FilterBob::" + randomSuffix,
      isLocal = true,
    )
    val storeF = MonadUtil.sequentialTraverse_(
      Seq(alice, bob)
    )(storePartyEntry(_, nextOffset()))
    for {
      _ <- storeF
      onlyAlicePartial <- listKnownParties(
        None,
        filterParty = Some(String185.tryCreate("FilterAlice")),
        1000,
      )
      onlyAliceFull <- listKnownParties(
        None,
        filterParty = Some(String185.tryCreate(alice.party)),
        1000,
      )
      bothFilter <- listKnownParties(
        None,
        filterParty = Some(String185.tryCreate("Filter")),
        1000,
      )
    } yield {
      onlyAlicePartial shouldBe List(alice)
      onlyAliceFull shouldBe List(alice)
      bothFilter shouldBe List(alice, bob)
    }
  }

  it should "retrieve all parties in two chunks" in {
    val randomSuffix = UUID.randomUUID()
    def genParty(name: String) =
      IndexerPartyDetails(
        party = Ref.Party.assertFromString(s"$name-$randomSuffix"),
        isLocal = true,
      )
    val newParties = List("Wes", "Zeb", "Les", "Mel").map(genParty)

    for {
      partiesBefore <- listKnownParties(None, filterParty = None, 1000)
      _ <- MonadUtil.sequentialTraverse_(newParties)(storePartyEntry(_, nextOffset()))
      parties1 <- listKnownParties(None, filterParty = None, partiesBefore.size + 1)
      parties2 <- listKnownParties(
        parties1.lastOption.map(_.party),
        filterParty = None,
        partiesBefore.size + newParties.size,
      )
    } yield {
      parties1 ++ parties2 should contain.allElementsOf(newParties)
      parties1 ++ parties2 should contain.allElementsOf(partiesBefore)
      parties1.size + parties2.size should equal(newParties.size + partiesBefore.size)
    }
  }

  it should "list known parties consistently at a pinned event sequence ID" in {
    val randomSuffix = UUID.randomUUID()
    def genParty(name: String) =
      IndexerPartyDetails(
        party = Ref.Party.assertFromString(s"Pinned$name-$randomSuffix"),
        isLocal = true,
      )
    val early = genParty("Early")
    val late = genParty("Late")
    val prefix = Some(String185.tryCreate("Pinned"))

    for {
      _ <- storePartyEntry(early, nextOffset())
      firstPage <- ledgerDao.listKnownParties(None, prefix, 1000, validAtEventSeqId = None)
      _ <- storePartyEntry(late, nextOffset())
      pinnedPage <- ledgerDao.listKnownParties(
        None,
        prefix,
        1000,
        validAtEventSeqId = Some(firstPage.validAtEventSeqId),
      )
      currentPage <- ledgerDao.listKnownParties(None, prefix, 1000, validAtEventSeqId = None)
    } yield {
      pinnedPage.validAtEventSeqId shouldBe firstPage.validAtEventSeqId
      pinnedPage.parties should contain(early)
      pinnedPage.parties should not contain late
      currentPage.parties should contain.allOf(early, late)
      currentPage.validAtEventSeqId should be > firstPage.validAtEventSeqId
    }
  }

  it should "reject listing known parties beyond the ledger end event sequence ID" in {
    for {
      current <- ledgerDao.listKnownParties(None, None, 1, validAtEventSeqId = None)
      beyondLedgerEnd = current.validAtEventSeqId + 1000L
      failure <- recoverToExceptionIf[StatusRuntimeException](
        ledgerDao.listKnownParties(
          None,
          None,
          1,
          validAtEventSeqId = Some(beyondLedgerEnd),
        )
      )
    } yield {
      failure.getStatus.getDescription should include("beyond the ledger end event sequential ID")
    }
  }

  it should "retrieve zero parties" in {
    for {
      noPartyDetails <- ledgerDao.getParties(Seq.empty)
    } yield {
      noPartyDetails should be(Seq.empty)
    }
  }

  it should "return no parties at event sequence ID zero" in {
    ledgerDao.listKnownParties(None, None, 10, validAtEventSeqId = Some(0L)).map { page =>
      page.validAtEventSeqId shouldBe 0L
      page.parties shouldBe empty
    }
  }

  it should "retrieve a single party, if they exist" in {
    val party = Ref.Party.assertFromString(s"Carol-${UUID.randomUUID()}")
    val nonExistentParty = UUID.randomUUID().toString
    val carol = IndexerPartyDetails(
      party = party,
      isLocal = true,
    )
    for {
      response <- storePartyEntry(carol, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      carolPartyDetails <- ledgerDao.getParties(Seq(party))
      noPartyDetails <- ledgerDao.getParties(Seq(nonExistentParty))
    } yield {
      carolPartyDetails should be(Seq(carol))
      noPartyDetails should be(Seq.empty)
    }
  }

  it should "retrieve multiple parties" in {
    val danParty = Ref.Party.assertFromString(s"Dan-${UUID.randomUUID()}")
    val eveParty = Ref.Party.assertFromString(s"Eve-${UUID.randomUUID()}")
    val nonExistentParty = UUID.randomUUID().toString
    val dan = IndexerPartyDetails(
      party = danParty,
      isLocal = true,
    )
    val eve = IndexerPartyDetails(
      party = eveParty,
      isLocal = true,
    )
    for {
      response <- storePartyEntry(dan, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(eve, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.getParties(Seq(danParty, eveParty, nonExistentParty))
    } yield {
      parties should contain.only(dan, eve)
    }
  }

  private def listKnownParties(
      fromExcl: Option[Ref.Party],
      filterParty: Option[String185],
      maxResults: Int,
  ) =
    ledgerDao
      .listKnownParties(fromExcl, filterParty, maxResults, validAtEventSeqId = None)
      .map(_.parties)

  private def storePartyEntry(
      partyDetails: IndexerPartyDetails,
      offset: Offset,
      recordTime: Timestamp = Timestamp.now(),
  ) =
    ledgerDao
      .storePartyAdded(
        offset,
        recordTime,
        partyDetails,
      )
      .map { response =>
        previousOffset.set(Some(offset))
        response
      }
}
