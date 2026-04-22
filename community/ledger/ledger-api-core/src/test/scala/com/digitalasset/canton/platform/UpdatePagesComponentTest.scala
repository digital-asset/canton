// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.base.error.ErrorCode.LoggedApiException
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.api.messages.update.GetUpdatesPageRequest
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference

class UpdatePagesComponentTest extends AnyWordSpec with IndexComponentTest {
  private val recordTimeRef = new AtomicReference(CantonTimestamp.now())
  private val nextRecordTime: () => CantonTimestamp =
    () => recordTimeRef.updateAndGet(_.immediateSuccessor)

  private var previousPruneUpTo: Option[Offset] = None
  private def nextPruneUpTo(pruneUpTo: Offset): Option[Offset] = {
    val ret = previousPruneUpTo
    previousPruneUpTo = Some(pruneUpTo)
    ret
  }

  def updateFormat(transactionShape: TransactionShape) = UpdateFormat(
    includeTransactions = Some(
      TransactionFormat(
        eventFormat = EventFormat(
          filtersByParty = Map(dsoParty -> CumulativeFilter.templateWildcardFilter(true)),
          filtersForAnyParty = None,
          verbose = false,
        ),
        transactionShape = transactionShape,
      )
    ),
    includeReassignments = None,
    includeTopologyEvents = None,
  )

  "update pages ascending with dynamic bounds" should {
    "return an empty pages with an empty ledger" in {
      val request = GetUpdatesPageRequest(
        startExclusive = None,
        endInclusive = None,
        continueStreamFromIncl = None,
        maxPageSize = 20,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = false,
        requestChecksum = ByteString.EMPTY,
        participantChecksum = ByteString.EMPTY,
      )
      restartIndexer()
      val page = index.updatesPage(request).futureValue

      page.updates shouldBe empty
      page.nextPageToken shouldNot be(empty)
      page.lowestPageOffsetExclusive shouldEqual 0
      page.highestPageOffsetInclusive shouldEqual 0
    }
  }
  "update pages descending with dynamic bounds" should {
    "return an empty pages with an empty ledger" in {
      val request = GetUpdatesPageRequest(
        startExclusive = None,
        endInclusive = None,
        continueStreamFromIncl = None,
        maxPageSize = 20,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = true,
        requestChecksum = ByteString.EMPTY,
        participantChecksum = ByteString.EMPTY,
      )
      restartIndexer()
      val page = index.updatesPage(request).futureValue

      page.updates shouldBe empty
      page.nextPageToken shouldBe empty
      page.lowestPageOffsetExclusive shouldEqual 0
      page.highestPageOffsetInclusive shouldEqual 0
    }

    "not throw an exception if hitting pruning offset in a middle of a page" in {
      val request = GetUpdatesPageRequest(
        startExclusive = None,
        endInclusive = None,
        continueStreamFromIncl = None,
        maxPageSize = 2,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = true,
        requestChecksum = ByteString.EMPTY,
        participantChecksum = ByteString.EMPTY,
      )
      val create1 = creates(nextRecordTime, 10)(1)
      val create2 = creates(nextRecordTime, 10)(1)
      val create3 = creates(nextRecordTime, 10)(1)
      val create4 = creates(nextRecordTime, 10)(1)

      val pruneTo = ingestUpdates(create1)
      ingestUpdates(create2)
      ingestUpdates(create3)
      ingestUpdates(create4)

      val firstPage = index.updatesPage(request).futureValue
      index.prune(nextPruneUpTo(pruneTo), Vector(), pruneTo, Vector()).futureValue
      eventually() {
        index.indexDbPrunedUpto.futureValue shouldEqual Some(pruneTo)
      }
      val secondPage = index
        .updatesPage(
          request.copy(continueStreamFromIncl =
            Some(Offset.tryFromLong(firstPage.lowestPageOffsetExclusive))
          )
        )
        .futureValue

      firstPage.updates should have length 2
      secondPage.updates should have length 1
      secondPage.nextPageToken shouldBe empty
    }

    "not throw an exception if hitting pruning index after page was fetched" in {
      val request = GetUpdatesPageRequest(
        startExclusive = None,
        endInclusive = None,
        continueStreamFromIncl = None,
        maxPageSize = 3,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = true,
        requestChecksum = ByteString.EMPTY,
        participantChecksum = ByteString.EMPTY,
      )
      val create1 = creates(nextRecordTime, 10)(1)
      val create2 = creates(nextRecordTime, 10)(1)
      val create3 = creates(nextRecordTime, 10)(1)
      val create4 = creates(nextRecordTime, 10)(1)

      ingestUpdates(create1)
      val pruneTo = ingestUpdates(create2)
      ingestUpdates(create3)
      ingestUpdates(create4)

      val firstPage = index.updatesPage(request).futureValue
      index.prune(nextPruneUpTo(pruneTo), Vector(), pruneTo, Vector()).futureValue
      eventually() {
        index.indexDbPrunedUpto.futureValue shouldEqual Some(pruneTo)
      }
      val secondPage = index
        .updatesPage(
          request.copy(continueStreamFromIncl =
            Some(Offset.tryFromLong(firstPage.lowestPageOffsetExclusive))
          )
        )
        .futureValue

      firstPage.updates should have length 3
      secondPage.updates should have length 0
      secondPage.nextPageToken shouldBe empty
    }
  }

  "update with ascending pages" should {
    "not fail if pruning bound does not catch up with current page generation" in {
      val create1 = creates(nextRecordTime, 10)(1)
      val create2 = creates(nextRecordTime, 10)(1)
      val create3 = creates(nextRecordTime, 10)(1)
      val create4 = creates(nextRecordTime, 10)(1)

      val fetchFrom = ingestUpdates(create1)
      val pruneTo = ingestUpdates(create2)
      ingestUpdates(create3)
      val fetchTo = ingestUpdates(create4)

      val request = GetUpdatesPageRequest(
        startExclusive = Some(fetchFrom.decrement),
        endInclusive = Some(fetchTo),
        continueStreamFromIncl = None,
        maxPageSize = 2,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = false,
        requestChecksum = ByteString.EMPTY,
        participantChecksum = ByteString.EMPTY,
      )

      val firstPage = index.updatesPage(request).futureValue
      index.prune(nextPruneUpTo(pruneTo), Vector(), pruneTo, Vector()).futureValue
      eventually() {
        index.indexDbPrunedUpto.futureValue shouldEqual Some(pruneTo)
      }
      val secondPage = index
        .updatesPage(
          request.copy(continueStreamFromIncl =
            Some(Offset.tryFromLong(firstPage.highestPageOffsetInclusive + 1))
          )
        )
        .futureValue

      firstPage.updates should have length 2
      secondPage.updates should have length 2
      secondPage.nextPageToken shouldBe empty
    }

    "fail if pruning bound does catches with current page generation" in {
      val create1 = creates(nextRecordTime, 10)(1)
      val create2 = creates(nextRecordTime, 10)(1)
      val create3 = creates(nextRecordTime, 10)(1)
      val create4 = creates(nextRecordTime, 10)(1)

      val fetchFrom = ingestUpdates(create1)
      ingestUpdates(create2)
      val pruneTo = ingestUpdates(create3)
      val fetchTo = ingestUpdates(create4)
      val request = GetUpdatesPageRequest(
        startExclusive = Some(fetchFrom.decrement),
        endInclusive = Some(fetchTo),
        continueStreamFromIncl = None,
        maxPageSize = 2,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = false,
        requestChecksum = ByteString.EMPTY,
        participantChecksum = ByteString.EMPTY,
      )

      val firstPage = index.updatesPage(request).futureValue
      index.prune(nextPruneUpTo(pruneTo), Vector(), pruneTo, Vector()).futureValue
      val exception = index
        .updatesPage(
          request.copy(continueStreamFromIncl =
            Some(Offset.tryFromLong(firstPage.lowestPageOffsetExclusive))
          )
        )
        .failed
        .futureValue

      exception shouldBe a[LoggedApiException]
      firstPage.updates should have length 2
    }
  }

}
