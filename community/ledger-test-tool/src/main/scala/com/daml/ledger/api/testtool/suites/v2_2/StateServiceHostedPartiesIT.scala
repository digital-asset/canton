// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import cats.implicits.toFunctorOps
import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesPageRequest,
  GetHostedPartiesPageResponse,
  GetHostedPartiesRequest,
  GetHostedPartiesResponse,
  HostedPartiesFilter,
  HostedParty,
  HostingState,
  ParticipantPermission,
}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final class StateServiceHostedPartiesIT extends LedgerTestSuite {

  test(
    "StateServiceHostedPartiesStreamSingleParty",
    "Return complete local hosting information for only the requested party in both APIs",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, _))) =>
    val filter = Some(
      HostedPartiesFilter(
        partyFilter = Seq(alice.getValue),
        allParticipants = false,
        synchronizerIdFilter = Seq.empty,
      )
    )
    for {
      participantId <- ledger.getParticipantId()
      offset <- ledger.currentEnd()
      streamed <- snapshotFromBothApis(ledger, offset, filter)
      _ <- assertAllocatedOnParticipant(
        ledger = ledger,
        hostedAtOffset = offset,
        actual = assertSingleton("streamed party", streamed),
        expectedParty = alice,
        expectedParticipantId = participantId,
      )
    } yield ()
  })

  test(
    "StateServiceHostedPartiesStreamMultipleParties",
    "Return complete local hosting information for exactly the requested parties in both APIs",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, _))) =>
    val requested = Seq(alice, bob, charlie)
    val filter = Some(
      HostedPartiesFilter(
        partyFilter = requested.map(_.getValue),
        allParticipants = false,
        synchronizerIdFilter = Seq.empty,
      )
    )
    for {
      participantId <- ledger.getParticipantId()
      offset <- ledger.currentEnd()
      streamed <- snapshotFromBothApis(ledger, offset, filter)
      _ = assertLength("parties", requested.size, streamed)
      _ <- assertAllocatedOnParticipant(
        ledger = ledger,
        hostedAtOffset = offset,
        actual = streamed.find(_.getHostedParty.party == alice.getValue).value,
        expectedParty = alice,
        expectedParticipantId = participantId,
      )
      _ <- assertAllocatedOnParticipant(
        ledger = ledger,
        hostedAtOffset = offset,
        actual = streamed.find(_.getHostedParty.party == bob.getValue).value,
        expectedParty = bob,
        expectedParticipantId = participantId,
      )
      _ <- assertAllocatedOnParticipant(
        ledger = ledger,
        hostedAtOffset = offset,
        actual = streamed.find(_.getHostedParty.party == charlie.getValue).value,
        expectedParty = charlie,
        expectedParticipantId = participantId,
      )
    } yield ()
  })

  test(
    "StateServiceHostedPartiesEmptySnapshot",
    "The snapshot at ledger begin is empty in both APIs, with or without filters and empty tokens",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    Future
      .sequence(for {
        filter <- Seq(
          None,
          Some(
            HostedPartiesFilter(
              partyFilter = Seq(party.getValue),
              allParticipants = false,
              synchronizerIdFilter = Seq.empty,
            )
          ),
        )
        token <- Seq(None, Some(ByteString.EMPTY))
      } yield snapshotFromBothApis(ledger, 0L, filter, token).map(assertIsEmpty))
      .void
  })

  test(
    "StateServiceHostedPartiesUnknownFilters",
    "Unknown parties and synchronizers yield empty snapshots in both APIs",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val synchronizerId = party.initialSynchronizers.headOption.value
    for {
      offset <- ledger.currentEnd()
      known <- snapshotFromBothApis(
        ledger,
        offset,
        Some(
          HostedPartiesFilter(
            partyFilter = Seq(party.getValue),
            allParticipants = false,
            synchronizerIdFilter = Seq(synchronizerId),
          )
        ),
      )
      unknownParty <- snapshotFromBothApis(
        ledger,
        offset,
        Some(
          HostedPartiesFilter(
            partyFilter = Seq(ledger.nextPartyId()),
            allParticipants = false,
            synchronizerIdFilter = Seq.empty,
          )
        ),
      )
      unknownSynchronizerId <- snapshotFromBothApis(
        ledger,
        offset,
        Some(
          HostedPartiesFilter(
            partyFilter = Seq(party.getValue),
            allParticipants = false,
            synchronizerIdFilter = Seq(unknownSynchronizer(ledger)),
          )
        ),
      )
    } yield {
      assertEquals(
        "party on its known synchronizer",
        assertSingleton("known synchronizer", known).getHostedParty.party,
        party.getValue,
      )
      assertIsEmpty(unknownParty)
      assertIsEmpty(unknownSynchronizerId)
    }
  })

  test(
    "StateServiceHostedPartiesContinuation",
    "Stream tokens resume the exact suffix and repeated requests return identical results",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, parties)) =>
    val filter = HostedPartiesFilter(
      partyFilter = parties.map(_.getValue),
      allParticipants = false,
      synchronizerIdFilter = parties.headOption.value.initialSynchronizers,
    )
    for {
      offset <- ledger.currentEnd()
      request = GetHostedPartiesRequest(
        hostedAtOffset = Some(offset),
        filter = Some(filter),
        streamContinuationToken = None,
      )
      stream <- ledger.getHostedParties(request)
      _ = assertEquals("stream size", stream.size, parties.size)
      _ = assert(
        stream.forall(!_.streamContinuationToken.isEmpty),
        "Every party must have a continuation token",
      )
      continueAfter = (response: GetHostedPartiesResponse) =>
        request.copy(streamContinuationToken = Some(response.streamContinuationToken))
      first = stream.headOption.value
      afterFirst <- ledger.getHostedParties(continueAfter(first))
      afterSecond <- ledger.getHostedParties(continueAfter(stream.lift(1).value))
      afterThird <- ledger.getHostedParties(continueAfter(stream.lift(2).value))
      repeatedStream <- ledger.getHostedParties(request)
      repeatedAfterFirst <- ledger.getHostedParties(continueAfter(first))
      emptyTokenStream <- ledger.getHostedParties(
        request.copy(streamContinuationToken = Some(ByteString.EMPTY))
      )
      equivalentFilterStream <- ledger.getHostedParties(
        continueAfter(first).copy(
          filter = Some(
            filter.copy(
              partyFilter = filter.partyFilter.reverse ++ filter.partyFilter,
              synchronizerIdFilter =
                filter.synchronizerIdFilter.reverse ++ filter.synchronizerIdFilter,
            )
          )
        )
      )
    } yield {
      assertSameParties(stream.map(_.getHostedParty), parties)
      assertEquals(
        "distinct continuation tokens",
        stream.map(_.streamContinuationToken).distinct.size,
        stream.size,
      )
      assertEquals("suffix after the first party", afterFirst, stream.drop(1))
      assertEquals("suffix after the second party", afterSecond, stream.drop(2))
      assertIsEmpty(afterThird)
      assertEquals("repeated request returns the same snapshot", repeatedStream, stream)
      assertEquals("repeated continuation returns the same suffix", repeatedAfterFirst, afterFirst)
      assertEquals("empty token starts at the beginning", emptyTokenStream, stream)
      assertEquals(
        "filter order and duplicates do not affect tokens",
        equivalentFilterStream,
        afterFirst,
      )
    }
  })

  test(
    "StateServiceHostedPartiesContinuationRequestMismatch",
    "Stream tokens cannot be reused with a different snapshot or filter",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, parties)) =>
    val filter = HostedPartiesFilter(
      partyFilter = parties.map(_.getValue),
      allParticipants = false,
      synchronizerIdFilter = parties.headOption.value.initialSynchronizers,
    )
    for {
      offset <- ledger.currentEnd()
      request = GetHostedPartiesRequest(
        hostedAtOffset = Some(offset),
        filter = Some(filter),
        streamContinuationToken = None,
      )
      stream <- ledger.getHostedParties(request)
      _ = assertEquals("stream size", stream.size, parties.size)
      resumed = request.copy(
        streamContinuationToken = Some(stream.headOption.value.streamContinuationToken)
      )
      _ <- ledger
        .getHostedParties(resumed.copy(hostedAtOffset = Some(0L)))
        .mustFailWith(
          "continuation token with a changed offset",
          RequestValidationErrors.InvalidHostedPartiesContinuationToken,
        )
      _ <- ledger
        .getHostedParties(
          resumed.copy(filter = Some(filter.copy(partyFilter = filter.partyFilter.take(1))))
        )
        .mustFailWith(
          "continuation token with a changed party filter",
          RequestValidationErrors.InvalidHostedPartiesContinuationToken,
        )
      _ <- ledger
        .getHostedParties(
          resumed.copy(filter = Some(filter.copy(synchronizerIdFilter = Seq.empty)))
        )
        .mustFailWith(
          "continuation token with a changed synchronizer filter",
          RequestValidationErrors.InvalidHostedPartiesContinuationToken,
        )
      _ <- ledger
        .getHostedParties(resumed.copy(filter = Some(filter.copy(allParticipants = true))))
        .mustFailWith(
          "continuation token with a changed participant filter",
          RequestValidationErrors.InvalidHostedPartiesContinuationToken,
        )
    } yield ()
  })

  test(
    "StateServiceHostedPartiesMalformedTokens",
    "Both APIs reject malformed tokens even for an empty snapshot",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val malformedToken = ByteString.copyFrom(Array[Byte](0))
    for {
      _ <- ledger
        .getHostedParties(
          GetHostedPartiesRequest(
            hostedAtOffset = Some(0L),
            filter = None,
            streamContinuationToken = Some(malformedToken),
          )
        )
        .mustFailWith(
          "malformed continuation token",
          RequestValidationErrors.InvalidField,
          Some(
            "Invalid field stream_continuation_token: Invalid continuation token for GetHostedPartiesRequest"
          ),
        )
      _ <- ledger
        .getHostedPartiesPage(
          GetHostedPartiesPageRequest(
            hostedAtOffset = Some(0L),
            filter = None,
            pageToken = Some(malformedToken),
            maxPageSize = None,
          )
        )
        .mustFailWith(
          "malformed page token",
          RequestValidationErrors.InvalidField,
          Some("Invalid field page_token: Invalid page token for GetHostedPartiesPageRequest"),
        )
    } yield ()
  })

  test(
    "StateServiceHostedPartiesOffsetBeyondLedgerEnd",
    "Both APIs reject a snapshot offset beyond the ledger end",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      end <- ledger.currentEnd()
      // a margin that concurrently running tests cannot advance the ledger end past
      beyondLedgerEnd = end + 1000000L
      _ <- assertRejectedByBothApis(
        ledger,
        beyondLedgerEnd,
        None,
        RequestValidationErrors.ParticipantDataAccessedAfterLedgerEnd,
        Some(s"Hosted parties request at offset $beyondLedgerEnd is beyond ledger end offset"),
      )
    } yield ()
  })

  test(
    "StateServiceHostedPartiesHistoricalSnapshot",
    "Historical snapshots in both APIs exclude parties allocated after the requested offset",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      before <- ledger.currentEnd()
      bob <- ledger.allocateParty()
      after <- ledger.currentEnd()
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq(bob.getValue),
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      )
      beforeBob <- snapshotFromBothApis(ledger, before, filter)
      afterBob <- snapshotFromBothApis(ledger, after, filter)
    } yield {
      assert(after > before, "Party allocation must advance the ledger end")
      assertIsEmpty(beforeBob)
      assertEquals(
        "bob after his allocation",
        assertSingleton("after bob", afterBob).getHostedParty.party,
        bob.getValue,
      )
    }
  })

  test(
    "StateServiceHostedPartiesPageLatestOffset",
    "A page request without an offset uses the ledger end and returns that offset",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    def latestPage(filter: Option[HostedPartiesFilter]) =
      ledger.getHostedPartiesPage(
        GetHostedPartiesPageRequest(
          hostedAtOffset = None,
          filter = filter,
          pageToken = None,
          maxPageSize = None,
        )
      )
    for {
      before <- ledger.currentEnd()
      page <- latestPage(
        Some(
          HostedPartiesFilter(
            partyFilter = Seq(alice.getValue),
            allParticipants = false,
            synchronizerIdFilter = Seq.empty,
          )
        )
      )
      after <- ledger.currentEnd()
      bob <- ledger.allocateParty()
      bothParties = Some(
        HostedPartiesFilter(
          partyFilter = Seq(alice.getValue, bob.getValue),
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      )
      // the returned offset pins the snapshot taken before bob was allocated
      pinned <- snapshotFromBothApis(ledger, page.hostedAtOffset, bothParties)
      latest <- latestPage(bothParties)
    } yield {
      assert(
        page.hostedAtOffset >= before && page.hostedAtOffset <= after,
        s"Expected a snapshot offset between $before and $after, got ${page.hostedAtOffset}",
      )
      assertEquals(
        "latest party",
        assertSingleton("latest page", page.hostedParties).party,
        alice.getValue,
      )
      assertIsEmpty(page.nextPageToken)
      assertEquals("pinned snapshot", pinned.map(_.getHostedParty), page.hostedParties)
      assert(
        latest.hostedAtOffset > page.hostedAtOffset,
        "Expected the ledger end to advance with the allocation of bob",
      )
      assertSameParties(latest.hostedParties, Seq(alice, bob))
    }
  })

  test(
    "StateServiceHostedPartiesStreamLatestOffset",
    "Continuation requests repeat the first request's offset, while the token pins the snapshot",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, parties)) =>
    val request = GetHostedPartiesRequest(
      hostedAtOffset = None,
      filter = Some(
        HostedPartiesFilter(
          partyFilter = parties.map(_.getValue),
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      streamContinuationToken = None,
    )
    for {
      before <- ledger.currentEnd()
      stream <- ledger.getHostedParties(request)
      after <- ledger.currentEnd()
      _ = assertLength("stream", 2, stream)
      first = stream.headOption.value
      second = stream.lift(1).value
      // advances the ledger end past the snapshot offset
      _ <- ledger.allocateParty()
      resumed = request.copy(streamContinuationToken = Some(first.streamContinuationToken))
      _ <- ledger
        .getHostedParties(resumed.copy(hostedAtOffset = Some(first.hostedAtOffset)))
        .mustFailWith(
          "continuation with a different offset than the first request",
          RequestValidationErrors.InvalidHostedPartiesContinuationToken,
        )
      afterFirst <- ledger.getHostedParties(resumed)
    } yield {
      assert(
        first.hostedAtOffset >= before && first.hostedAtOffset <= after,
        s"Expected a snapshot offset between $before and $after, got ${first.hostedAtOffset}",
      )
      assertEquals("second snapshot offset", second.hostedAtOffset, first.hostedAtOffset)
      assertEquals("resumed at the snapshot offset", afterFirst, Seq(second))
    }
  })

  test(
    "StateServiceHostedPartiesPageLatestOffsetContinuation",
    "Next page requests repeat the first request's offset, while the page token pins the snapshot",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, parties)) =>
    val request = GetHostedPartiesPageRequest(
      hostedAtOffset = None,
      filter = Some(
        HostedPartiesFilter(
          partyFilter = parties.map(_.getValue),
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      pageToken = None,
      maxPageSize = Some(1),
    )
    for {
      firstPage <- ledger.getHostedPartiesPage(request)
      // advances the ledger end past the snapshot offset
      _ <- ledger.allocateParty()
      nextPage = request.copy(pageToken = firstPage.nextPageToken)
      _ <- ledger
        .getHostedPartiesPage(nextPage.copy(hostedAtOffset = Some(firstPage.hostedAtOffset)))
        .mustFailWith(
          "next page with a different offset than the first request",
          RequestValidationErrors.InvalidHostedPartiesPageToken,
        )
      secondPage <- ledger.getHostedPartiesPage(nextPage)
    } yield {
      assertLength("first page", 1, firstPage.hostedParties)
      assertEquals("pinned snapshot offset", secondPage.hostedAtOffset, firstPage.hostedAtOffset)
      assertSameParties(firstPage.hostedParties ++ secondPage.hostedParties, parties)
      assertIsEmpty(secondPage.nextPageToken)
    }
  })

  test(
    "StateServiceHostedPartiesInvalidPageSize",
    "The page API rejects a non-positive page size and one exceeding the server limit",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      _ <- ledger
        .getHostedPartiesPage(
          GetHostedPartiesPageRequest(
            hostedAtOffset = None,
            filter = None,
            pageToken = None,
            maxPageSize = Some(0),
          )
        )
        .mustFailWith(
          "invalid max page size",
          RequestValidationErrors.InvalidArgument,
          Some(
            "The submitted request has invalid arguments: Requested page size must be positive, but got 0"
          ),
        )
      _ <- ledger
        .getHostedPartiesPage(
          GetHostedPartiesPageRequest(
            hostedAtOffset = None,
            filter = None,
            pageToken = None,
            maxPageSize = Some(10000000),
          )
        )
        .mustFailWith(
          "too big max page size",
          RequestValidationErrors.InvalidArgument,
          Some(
            "The submitted request has invalid arguments: Requested page size must not exceed 10000, but got 10000000"
          ),
        )
    } yield ()
  })

  private def unknownSynchronizer(ledger: ParticipantTestContext): String =
    s"${ledger.nextPartyId()}::1220${"0" * 64}"

  /** Fetches the snapshot through both APIs, asserting that the pages of the page endpoint echo the
    * offset and, with one party, two parties or the default number of parties per page, return the
    * same parties as the stream.
    */
  private def snapshotFromBothApis(
      ledger: ParticipantTestContext,
      offset: Long,
      filter: Option[HostedPartiesFilter],
      token: Option[ByteString] = None,
  )(implicit ec: ExecutionContext): Future[Vector[GetHostedPartiesResponse]] = {
    val pageRequest = GetHostedPartiesPageRequest(
      hostedAtOffset = Some(offset),
      filter = filter,
      pageToken = token,
      maxPageSize = None,
    )
    for {
      stream <- ledger.getHostedParties(
        GetHostedPartiesRequest(
          hostedAtOffset = Some(offset),
          filter = filter,
          streamContinuationToken = token,
        )
      )
      pagesOfOne <- allPages(ledger, pageRequest.copy(maxPageSize = Some(1)))
      pagesOfTwo <- allPages(ledger, pageRequest.copy(maxPageSize = Some(2)))
      defaultPages <- allPages(ledger, pageRequest)
    } yield {
      val streamed = stream.map(_.getHostedParty)
      assertEquals("pages of one party", pagesOfOne.map(_.hostedParties), pagesOf(streamed, 1))
      assertEquals("pages of two parties", pagesOfTwo.map(_.hostedParties), pagesOf(streamed, 2))
      assertEquals("default pages match stream", defaultPages.flatMap(_.hostedParties), streamed)
      (pagesOfOne ++ pagesOfTwo ++ defaultPages).foreach(page =>
        assertEquals("page snapshot offset", page.hostedAtOffset, offset)
      )
      stream
    }
  }

  // the page endpoint returns a single empty page for an empty snapshot
  private def pagesOf(parties: Seq[HostedParty], pageSize: Int): Seq[Seq[HostedParty]] =
    if (parties.isEmpty) Seq(Seq.empty) else parties.grouped(pageSize).toSeq

  private def allPages(
      ledger: ParticipantTestContext,
      request: GetHostedPartiesPageRequest,
  )(implicit ec: ExecutionContext): Future[Vector[GetHostedPartiesPageResponse]] =
    ledger.getHostedPartiesPage(request).flatMap { page =>
      page.nextPageToken.fold(Future.successful(Vector(page)))(next =>
        allPages(ledger, request.copy(pageToken = Some(next))).map(page +: _)
      )
    }

  private def assertSameParties(actual: Seq[HostedParty], expected: Seq[Party]): Unit =
    assertSameElements(actual.map(_.party), expected.map(_.getValue))

  private def assertAllocatedOnParticipant(
      ledger: ParticipantTestContext,
      hostedAtOffset: Long,
      actual: GetHostedPartiesResponse,
      expectedParty: Party,
      expectedParticipantId: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    assertEquals("party", actual.getHostedParty.party, expectedParty.getValue)
    assert(!actual.streamContinuationToken.isEmpty, "Expected a continuation token")

    val hostings = actual.getHostedParty.hostingParticipants
    assert(hostings.nonEmpty, "Expected hosting participants")
    assertEquals(
      "participants",
      hostings.map(_.participantId).toSet,
      Set(expectedParticipantId),
    )
    assertEquals(
      "synchronizers",
      hostings.map(_.synchronizerId).toSet,
      expectedParty.initialSynchronizers.toSet,
    )
    assertEquals(
      "no duplicate synchronizers",
      hostings.size,
      expectedParty.initialSynchronizers.distinct.size,
    )
    Future
      .traverse(hostings) { hosting =>
        assert(hosting.recordTime.nonEmpty, "Expected a hosting record time")
        assert(
          hosting.offset > 0L && hosting.offset <= hostedAtOffset,
          "Expected an offset in the snapshot",
        )
        assertEquals(
          "participant permission",
          hosting.participantPermission,
          ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
        )
        assertEquals("hosting state", hosting.hostingState, HostingState.HOSTING_STATE_ACTIVE)
        ledger.topologyTransactionByOffset(hosting.offset, Seq(expectedParty)).map { transaction =>
          assertEquals(
            "hosting synchronizer",
            transaction.synchronizerId,
            hosting.synchronizerId,
          )
          assert(
            transaction.events.exists(
              _.event.participantAuthorizationAdded.exists(_.partyId == expectedParty.getValue)
            ),
            "Expected the hosting offset to point at the allocation topology transaction",
          )
          assertEquals("hosting record time", hosting.recordTime, transaction.recordTime)
        }
      }
      .void
  }

  private def assertRejectedByBothApis(
      ledger: ParticipantTestContext,
      offset: Long,
      filter: Option[HostedPartiesFilter],
      error: ErrorCode,
      message: Option[String],
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- ledger
        .getHostedParties(
          GetHostedPartiesRequest(
            hostedAtOffset = Some(offset),
            filter = filter,
            streamContinuationToken = None,
          )
        )
        .mustFailWith("invalid hosted parties stream request", error, message)
      _ <- ledger
        .getHostedPartiesPage(
          GetHostedPartiesPageRequest(
            hostedAtOffset = Some(offset),
            filter = filter,
            pageToken = None,
            maxPageSize = None,
          )
        )
        .mustFailWith("invalid hosted parties page request", error, message)
    } yield ()
}
