// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesResponse,
  HostedParty,
  HostingParticipant,
  HostingState,
  ParticipantPermission,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.messages.state.HostedPartiesContinuationToken
import com.digitalasset.canton.ledger.api.util.{PageTokenUtils, TimestampConversion}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.index.IndexerHostedPartyFilter
import com.digitalasset.canton.platform.component.IndexComponentTest.ServiceParams
import com.digitalasset.canton.platform.config.{HostedPartiesStreamsConfig, IndexServiceConfig}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import io.grpc.Status
import org.apache.pekko.stream.scaladsl.Sink
import org.scalactic.source
import org.scalatest.wordspec.AnyWordSpec

trait HostedPartiesComponentTest extends AnyWordSpec with IndexComponentTest {
  import HostedPartiesComponentTest.{Hosting, Ingestion}

  private val nextRecordTime = new SingleStepIncreasingRecordTime

  private val localParticipant =
    Ref.ParticipantId.assertFromString(IndexComponentTest.TestParticipantId)

  private val remoteParticipant =
    Ref.ParticipantId.assertFromString("hosted-parties-remote-participant")

  private def party(name: String): Ref.Party = Ref.Party.assertFromString(name)

  private def hosting(
      party: Ref.Party,
      participant: Ref.ParticipantId,
      synchronizerId: SynchronizerId,
      event: TopologyTransactionEffective.AuthorizationEvent,
  ): Hosting =
    new Hosting(party, participant, synchronizerId, event)

  private def ingestScenario(hostings: Hosting*)(implicit pos: source.Position): Scenario =
    new Scenario(Map.empty).ingest(hostings*)

  private def checkHostedParties(
      hostings: Seq[Hosting],
      filter: IndexerHostedPartyFilter,
      limit: Option[Int] = None,
      expected: Seq[Hosting],
  )(implicit pos: source.Position): Unit =
    ingestScenario(hostings*).check(
      filter,
      limit = limit,
      expected = expected,
    )

  private class Scenario(ingestions: Map[Hosting, Ingestion]) {

    // Ingests each hosting in its own topology transaction.
    def ingest(hostings: Hosting*)(implicit pos: source.Position): Scenario = {
      assert(
        hostings.distinct.size == hostings.size && !hostings.exists(ingestions.contains),
        "Each hosting input must occur only once in a scenario",
      )
      val added = hostings.map { input =>
        val effectiveTime = nextRecordTime()
        val update = TopologyTransactionEffective(
          updateId = randomUpdateId,
          events = Set(
            PartyToParticipantAuthorization(input.party, input.participant, input.event)
          ),
          genericTopologyEvents = Seq.empty,
          synchronizerId = input.synchronizerId,
          effectiveTime = effectiveTime,
          traceContext = implicitly,
        )
        val offset = ingestUpdates(update -> Vector.empty)
        input -> Ingestion(offset, effectiveTime.underlying)
      }.toMap
      new Scenario(ingestions ++ added)
    }

    private def ingestion(input: Hosting)(implicit pos: source.Position): Ingestion =
      ingestions.getOrElse(input, fail("Hosting input was not ingested in this scenario"))

    def offsetOf(input: Hosting)(implicit pos: source.Position): Offset =
      ingestion(input).offset

    private def hostingParticipant(
        input: Hosting
    )(implicit pos: source.Position): HostingParticipant = {
      val position = ingestion(input)
      val (level, state): (AuthorizationLevel, HostingState) = input.event match {
        case Added(level) => level -> HostingState.HOSTING_STATE_ACTIVE
        case ChangedTo(level) => level -> HostingState.HOSTING_STATE_ACTIVE
        case Onboarding(level) => level -> HostingState.HOSTING_STATE_ONBOARDING
        case Revoked => fail("a revocation implies no hosting entry")
      }
      HostingParticipant(
        participantId = input.participant,
        synchronizerId = input.synchronizerId.toProtoPrimitive,
        recordTime = Some(TimestampConversion.fromLf(position.recordTime)),
        offset = position.offset.unwrap,
        participantPermission = level match {
          case Submission => ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
          case Confirmation => ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
          case Observation => ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
        },
        hostingState = state,
      )
    }

    def check(
        filter: IndexerHostedPartyFilter = IndexerHostedPartyFilter(),
        validAt: Offset = ledgerEnd,
        fromExcl: Option[Ref.Party] = None,
        limit: Option[Int] = None,
        expected: Seq[Hosting],
    )(implicit pos: source.Position): Unit =
      checkParties(hostedParties(filter, validAt, fromExcl, limit), expected)

    // Expected hostings are grouped by their party.
    def checkParties(actual: Seq[HostedParty], expected: Seq[Hosting])(implicit
        pos: source.Position
    ): Unit = {
      val expectedByParty = expected.groupBy(_.party)
      actual.map(_.party) should contain theSameElementsAs expectedByParty.keys
      forEvery(actual) { hostedParty =>
        clue(s"hostings of ${hostedParty.party}") {
          hostedParty.hostingParticipants should contain theSameElementsAs
            expectedByParty(party(hostedParty.party)).map(hostingParticipant)
        }
      }
    }
  }

  private def ledgerEnd: Offset = index.currentLedgerEnd().value.lastOffset

  private def hostedPartiesResponses(
      filter: IndexerHostedPartyFilter,
      validAt: Offset,
      fromExcl: Option[Ref.Party],
      limit: Option[Int],
  ): Seq[GetHostedPartiesResponse] =
    index
      .hostedParties(filter, validAt, fromExcl, limit, PageTokenUtils.emptyChecksum)
      .runWith(Sink.seq)
      .futureValue

  private def hostedParties(
      filter: IndexerHostedPartyFilter,
      validAt: Offset = ledgerEnd,
      fromExcl: Option[Ref.Party] = None,
      limit: Option[Int] = None,
  )(implicit pos: source.Position): Seq[HostedParty] = {
    val responses = hostedPartiesResponses(filter, validAt, fromExcl, limit)
    forEvery(responses) { response =>
      response.hostedAtOffset shouldBe validAt.unwrap
      response.streamContinuationToken shouldBe HostedPartiesContinuationToken.encode(
        partyLowerBoundExcl = party(response.getHostedParty.party),
        hostedAtOffset = validAt,
        checksum = PageTokenUtils.emptyChecksum,
      )
    }
    responses.flatMap(_.hostedParty)
  }

  private def hostedPartiesFailure(
      filter: IndexerHostedPartyFilter,
      validAt: Offset,
  )(implicit pos: source.Position): Throwable =
    index
      .hostedParties(filter, validAt, None, None, PageTokenUtils.emptyChecksum)
      .runWith(Sink.seq)
      .failed
      .futureValue

  private def filterFor(parties: Ref.Party*): IndexerHostedPartyFilter =
    IndexerHostedPartyFilter(parties = Some(NonEmptyUtil.fromUnsafe(parties.toSet)))

  "hostedParties" should {

    "report onboarding and active hostings with permission, state, offset and record time" in {
      val onboardingParty = party("hp-basic-onboarding")
      val activeParty = party("hp-basic-active")
      val onboarding =
        hosting(onboardingParty, localParticipant, synchronizer1, Onboarding(Observation))
      val active = hosting(activeParty, localParticipant, synchronizer1, Added(Submission))

      checkHostedParties(
        hostings = Seq(onboarding, active),
        filter = filterFor(onboardingParty, activeParty),
        expected = Seq(onboarding, active),
      )
    }

    "report the latest permission of a hosting relationship" in {
      val p = party("hp-latest")
      val original = hosting(p, localParticipant, synchronizer1, Added(Observation))
      val change = hosting(p, localParticipant, synchronizer1, ChangedTo(Confirmation))

      checkHostedParties(
        hostings = Seq(original, change),
        filter = filterFor(p),
        expected = Seq(change),
      )
    }

    "omit revoked hostings and report re-added hostings" in {
      val revokedParty = party("hp-revoked")
      val reAddedParty = party("hp-re-added")
      val reAdd = hosting(reAddedParty, localParticipant, synchronizer1, Added(Confirmation))

      checkHostedParties(
        hostings = Seq(
          hosting(revokedParty, localParticipant, synchronizer1, Added(Observation)),
          hosting(reAddedParty, localParticipant, synchronizer1, Added(Observation)),
          hosting(revokedParty, localParticipant, synchronizer1, Revoked),
          hosting(reAddedParty, localParticipant, synchronizer1, Revoked),
          reAdd,
        ),
        filter = filterFor(revokedParty, reAddedParty),
        expected = Seq(reAdd),
      )
    }

    "report the snapshot state at a historical offset" in {
      val p = party("hp-historical")
      val added = hosting(p, localParticipant, synchronizer1, Added(Observation))
      val revocation = hosting(p, localParticipant, synchronizer1, Revoked)
      val scenario = ingestScenario(added, revocation)

      scenario.check(
        filterFor(p),
        validAt = scenario.offsetOf(added),
        expected = Seq(added),
      )
      scenario.check(
        filterFor(p),
        validAt = scenario.offsetOf(revocation),
        expected = Nil,
      )
    }

    "exclude parties and hosting relationships introduced after the snapshot" in {
      val existingParty = party("hp-snapshot-existing")
      val laterParty = party("hp-snapshot-later")
      val original = hosting(existingParty, localParticipant, synchronizer1, Added(Observation))
      val later = hosting(laterParty, localParticipant, synchronizer1, Added(Submission))
      val remote = hosting(existingParty, remoteParticipant, synchronizer1, Added(Confirmation))
      val sync2 = hosting(existingParty, localParticipant, synchronizer2, Onboarding(Observation))
      val scenario =
        ingestScenario(original, later, remote, sync2)
      val filter = filterFor(existingParty, laterParty).copy(allParticipants = true)

      scenario.check(filter, validAt = scenario.offsetOf(original), expected = Seq(original))
      scenario.check(filter, expected = Seq(remote, original, sync2, later))
    }

    "revoke only the affected hosting relationship while retaining other hostings of the same party" in {
      val p = party("hp-partial-revocation")
      val local1 = hosting(p, localParticipant, synchronizer1, Added(Observation))
      val remote1 = hosting(p, remoteParticipant, synchronizer1, Added(Confirmation))
      val local2 = hosting(p, localParticipant, synchronizer2, Added(Submission))
      val remote2 = hosting(p, remoteParticipant, synchronizer2, Onboarding(Observation))
      val scenario = ingestScenario(
        local1,
        remote1,
        local2,
        remote2,
        hosting(p, localParticipant, synchronizer1, Revoked),
      )

      scenario.check(filterFor(p), expected = Seq(local2))
      scenario.check(
        filterFor(p).copy(allParticipants = true),
        expected = Seq(remote1, remote2, local2),
      )
    }

    "filter by the local participant, synchronizers and parties" in {
      val localParty = party("hp-filter-local")
      val remoteParty = party("hp-filter-remote")
      val sync1 = hosting(localParty, localParticipant, synchronizer1, Added(Observation))
      val sync2 = hosting(localParty, localParticipant, synchronizer2, Added(Confirmation))
      val remote = hosting(remoteParty, remoteParticipant, synchronizer1, Added(Observation))
      val scenario =
        ingestScenario(sync1, sync2, remote)

      scenario.check(filterFor(localParty, remoteParty), expected = Seq(sync1, sync2))
      scenario.check(
        filterFor(localParty, remoteParty).copy(allParticipants = true),
        expected = Seq(sync1, sync2, remote),
      )
      scenario.check(
        filterFor(localParty).copy(synchronizerIds = Some(NonEmpty(Set, synchronizer2))),
        expected = Seq(sync2),
      )
      scenario.check(
        filterFor(localParty).copy(
          synchronizerIds = Some(NonEmpty(Set, SynchronizerId.tryFromString("x::neverseen")))
        ),
        expected = Nil,
      )
      scenario.check(filterFor(party("hp-filter-never-seen")), expected = Nil)
    }

    "paginate the requested parties without gaps or duplicates" in {
      val all = (1 to 5).map { n =>
        hosting(party(s"hp-page-$n"), localParticipant, synchronizer1, Added(Observation))
      }
      val scenario = ingestScenario(all*)
      val filter = filterFor(all.map(_.party)*)

      val page1 = hostedParties(filter, limit = Some(2))
      page1 should have size 2
      val page2 = hostedParties(filter, fromExcl = Some(party(page1.last.party)), limit = Some(2))
      page2 should have size 2
      val page3 = hostedParties(filter, fromExcl = Some(party(page2.last.party)), limit = Some(2))
      page3 should have size 1
      scenario.checkParties(page1 ++ page2 ++ page3, expected = all)
      clue("the pages follow the order of the unpaged stream") {
        (page1 ++ page2 ++ page3).map(_.party) shouldBe hostedParties(filter).map(_.party)
      }

      scenario.check(filter, limit = Some(all.size + 1), expected = all)
      // a party that has never been hosted has no position in the snapshot order
      scenario.check(filter, fromExcl = Some(party("hp-page-never-seen")), expected = Nil)
    }

    "skip nonmatching and revoked hosting relationships before applying the party limit" in {
      val remoteOnly = party("hp-sparse-remote")
      val otherSynchronizer = party("hp-sparse-other-synchronizer")
      val revoked = party("hp-sparse-revoked")
      val matching = party("hp-sparse-matching")
      val otherSynchronizerInput =
        hosting(otherSynchronizer, localParticipant, synchronizer2, Added(Observation))
      val matchingInput = hosting(matching, localParticipant, synchronizer1, Added(Submission))
      val scenario = ingestScenario(
        hosting(remoteOnly, remoteParticipant, synchronizer1, Added(Observation)),
        otherSynchronizerInput,
        hosting(revoked, localParticipant, synchronizer1, Added(Observation)),
        hosting(revoked, localParticipant, synchronizer1, Revoked),
        matchingInput,
      )
      val filter = filterFor(remoteOnly, otherSynchronizer, revoked, matching)
        .copy(synchronizerIds = Some(NonEmpty(Set, synchronizer1)))

      scenario.check(filter, limit = Some(1), expected = Seq(matchingInput))
      scenario.check(filter, expected = Seq(matchingInput))
      scenario.check(filter, fromExcl = Some(matching), expected = Nil)
      scenario.check(
        filter.copy(synchronizerIds = Some(NonEmpty(Set, synchronizer1, synchronizer2))),
        expected = Seq(otherSynchronizerInput, matchingInput),
      )
    }

    "return all matching hostings of a party when the party limit is one" in {
      val p = party("hp-complete-across-filtered-pages")
      val synchronizer3 = SynchronizerId.tryFromString("x::hp-synchronizer3")
      val sync1 = hosting(p, localParticipant, synchronizer1, Added(Submission))
      val sync3 = hosting(p, localParticipant, synchronizer3, Onboarding(Confirmation))

      checkHostedParties(
        hostings = Seq(
          sync1,
          hosting(p, localParticipant, synchronizer2, Added(Observation)),
          sync3,
          hosting(p, remoteParticipant, synchronizer1, Added(Observation)),
        ),
        filter =
          filterFor(p).copy(synchronizerIds = Some(NonEmpty(Set, synchronizer1, synchronizer3))),
        limit = Some(1),
        expected = Seq(sync3, sync1),
      )
    }

    "paginate without gaps or duplicates when a hosting relationship has multiple historical events" in {
      val before = party("hp-churn-before")
      val churned = party("hp-churn")
      val after = party("hp-churn-after")
      val beforeInput = hosting(before, localParticipant, synchronizer1, Added(Observation))
      val revocation = hosting(churned, localParticipant, synchronizer1, Revoked)
      val reAdd = hosting(churned, localParticipant, synchronizer1, Added(Submission))
      val afterInput = hosting(after, localParticipant, synchronizer1, Added(Confirmation))
      val scenario = ingestScenario(
        beforeInput,
        hosting(churned, localParticipant, synchronizer1, Onboarding(Observation)),
        hosting(churned, localParticipant, synchronizer1, Added(Observation)),
        hosting(churned, localParticipant, synchronizer1, ChangedTo(Confirmation)),
        revocation,
        reAdd,
        afterInput,
      )
      val filter = filterFor(before, churned, after)

      scenario.check(filter, expected = Seq(beforeInput, reAdd, afterInput))
      // the earlier grants of the churned party must not resurface beneath its revocation
      scenario.check(
        filter,
        validAt = scenario.offsetOf(revocation),
        expected = Seq(beforeInput),
      )
      scenario.check(
        filter,
        validAt = scenario.offsetOf(revocation),
        fromExcl = Some(before),
        expected = Nil,
      )

      val page1 = hostedParties(filter, limit = Some(2))
      page1 should have size 2L
      val page2 = hostedParties(filter, fromExcl = Some(party(page1.last.party)), limit = Some(2))
      page2 should have size 1L
      scenario.checkParties(page1 ++ page2, expected = Seq(beforeInput, reAdd, afterInput))
    }

    "resume after a cursor party that does not match the filters" in {
      val all = (1 to 5).map { n =>
        hosting(party(s"hp-cursor-filter-$n"), localParticipant, synchronizer1, Added(Submission))
      }
      val initial = ingestScenario(all*)
      val filter = filterFor(all.map(_.party)*)
      val firstPage = hostedParties(filter, limit = Some(2)).map(_.party)
      firstPage should have size 2L
      val cursor = party(firstPage.last)
      val scenario = initial.ingest(
        hosting(cursor, remoteParticipant, synchronizer1, Added(Submission)),
        hosting(cursor, localParticipant, synchronizer1, Revoked),
      )
      val withoutCursor = all.filterNot(_.party == cursor)
      val afterFirstPage = all.filterNot(input => firstPage.contains(input.party))

      // the cursor party is now hosted only on the remote participant
      scenario.check(filter, expected = withoutCursor)
      scenario.check(filter, fromExcl = Some(cursor), expected = afterFirstPage)
      // the cursor party is not requested
      scenario.check(
        filterFor(all.map(_.party).filterNot(_ == cursor)*).copy(allParticipants = true),
        fromExcl = Some(cursor),
        expected = afterFirstPage,
      )
    }

    "resume after a revoked cursor party" in {
      val all = (1 to 5).map { n =>
        hosting(party(s"hp-cursor-revoked-$n"), localParticipant, synchronizer1, Added(Submission))
      }
      val initial = ingestScenario(all*)
      val filter = filterFor(all.map(_.party)*)
      val firstPage = hostedParties(filter, limit = Some(2)).map(_.party)
      firstPage should have size 2L
      val cursor = party(firstPage.last)
      val scenario = initial.ingest(hosting(cursor, localParticipant, synchronizer1, Revoked))
      val withoutCursor = all.filterNot(_.party == cursor)
      val afterFirstPage = all.filterNot(input => firstPage.contains(input.party))

      scenario.check(filter, expected = withoutCursor)
      scenario.check(filter, fromExcl = Some(cursor), expected = afterFirstPage)
      scenario.check(filterFor(cursor), fromExcl = Some(cursor), expected = Nil)
    }

    "resolve snapshots at offsets that carry no topology event" in {
      val earlier = party("hp-nontopology-earlier")
      val later = party("hp-nontopology-later")
      val earlierInput = hosting(earlier, localParticipant, synchronizer1, Added(Observation))
      val laterInput = hosting(later, localParticipant, synchronizer1, Added(Submission))
      val initial = ingestScenario(earlierInput)
      val betweenOffset = ingestUpdates(creates(nextRecordTime, payloadLength = 10)(size = 1))
      val scenario = initial.ingest(laterInput)
      val trailingOffset = ingestUpdates(creates(nextRecordTime, payloadLength = 10)(size = 1))
      val filter = filterFor(earlier, later)

      scenario.check(filter, validAt = betweenOffset, expected = Seq(earlierInput))
      scenario.check(
        filter,
        validAt = scenario.offsetOf(laterInput),
        expected = Seq(earlierInput, laterInput),
      )
      scenario.check(filter, validAt = trailingOffset, expected = Seq(earlierInput, laterInput))
    }

    "fail when the snapshot offset is beyond the ledger end" in {
      ingestScenario(
        hosting(party("hp-beyond"), localParticipant, synchronizer1, Added(Observation))
      ).discard
      val beyondLedgerEnd = Offset.tryFromLong(ledgerEnd.unwrap + 1)

      hostedPartiesFailure(
        IndexerHostedPartyFilter(),
        validAt = beyondLedgerEnd,
      ).getMessage should include("is beyond ledger end offset")
    }

    // Keep this test last: it prunes the shared index, hiding earlier offsets from later tests.
    "reject snapshots before the pruning offset and serve snapshots at or after it" in {
      val p = party("hp-pruning-hosted")
      val otherParty = party("hp-pruning-other")
      val added = hosting(p, localParticipant, synchronizer1, Added(Observation))
      val pruned = hosting(otherParty, localParticipant, synchronizer1, Added(Submission))
      val afterPruning = hosting(otherParty, localParticipant, synchronizer2, Added(Confirmation))
      val scenario =
        ingestScenario(added, pruned, afterPruning)
      val addedOffset = scenario.offsetOf(added)
      val prunedOffset = scenario.offsetOf(pruned)
      val filter = filterFor(p)
      scenario.check(filter, validAt = addedOffset, expected = Seq(added))

      index
        .prune(
          previousPruneUpToInclusive = index.latestPrunedOffset().futureValue,
          previousIncompleteReassignmentOffsets = Vector.empty,
          pruneUpToInclusive = prunedOffset,
          incompleteReassignmentOffsets = Vector.empty,
        )
        .futureValue

      val failure = hostedPartiesFailure(filter, validAt = addedOffset)
      Status.fromThrowable(failure).getCode shouldBe Status.Code.FAILED_PRECONDITION
      failure.getMessage should include(RequestValidationErrors.ParticipantPrunedDataAccessed.id)
      failure.getMessage should include(
        s"Hosted parties request at offset ${addedOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}"
      )
      scenario.check(filter, validAt = prunedOffset, expected = Seq(added))
      scenario.check(filter, validAt = scenario.offsetOf(afterPruning), expected = Seq(added))
    }
  }
}

object HostedPartiesComponentTest {

  // Identity distinguishes separate occurrences of the same authorization in a history.
  private final class Hosting(
      val party: Ref.Party,
      val participant: Ref.ParticipantId,
      val synchronizerId: SynchronizerId,
      val event: TopologyTransactionEffective.AuthorizationEvent,
  )

  private final case class Ingestion(
      offset: Offset,
      recordTime: Timestamp,
  )
}

final class HostedPartiesComponentTestDefaultH2 extends HostedPartiesComponentTest

final class HostedPartiesComponentTestDefaultPostgres
    extends HostedPartiesComponentTest
    with IndexComponentTest.WithPostgres

/** Forces every hosting triplet onto its own id page and payload batch, so that the pagination, the
  * parallel id filtering and the party grouping across page boundaries are exercised.
  */
trait HostedPartiesComponentTestTinyPages extends HostedPartiesComponentTest {
  override protected def serviceParams: ServiceParams =
    super.serviceParams.copy(indexServiceConfig =
      IndexServiceConfig(hostedPartiesStreams =
        HostedPartiesStreamsConfig(
          maxIdsPerIdPage = PositiveInt.one,
          maxPagesPerIdPagesBuffer = PositiveInt.one,
          maxPayloadsPerPayloadsPage = PositiveInt.one,
          maxParallelPayloadQueries = PositiveInt.two,
          idFilterQueryParallelism = PositiveInt.two,
        )
      )
    )
}

final class HostedPartiesComponentTestTinyPagesH2 extends HostedPartiesComponentTestTinyPages

final class HostedPartiesComponentTestTinyPagesPostgres
    extends HostedPartiesComponentTestTinyPages
    with IndexComponentTest.WithPostgres

/** Spreads the candidate triplets over multi-triplet id pages that do not line up with the payload
  * batches and uses non-power-of-two parallelism, so that uneven page boundaries and the input
  * buffer rounding are exercised.
  */
trait HostedPartiesComponentTestUnevenPages extends HostedPartiesComponentTest {
  override protected def serviceParams: ServiceParams =
    super.serviceParams.copy(indexServiceConfig =
      IndexServiceConfig(hostedPartiesStreams =
        HostedPartiesStreamsConfig(
          maxIdsPerIdPage = PositiveInt.three,
          maxPagesPerIdPagesBuffer = PositiveInt.two,
          maxPayloadsPerPayloadsPage = PositiveInt.two,
          maxParallelPayloadQueries = PositiveInt.three,
          idFilterQueryParallelism = PositiveInt.three,
        )
      )
    )
}

final class HostedPartiesComponentTestUnevenPagesH2 extends HostedPartiesComponentTestUnevenPages

final class HostedPartiesComponentTestUnevenPagesPostgres
    extends HostedPartiesComponentTestUnevenPages
    with IndexComponentTest.WithPostgres
