// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import cats.syntax.traverse.*
import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesResponse,
  HostedParty,
  HostingParticipant,
  HostingState as ProtoHostingState,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.state.HostedPartiesContinuationToken
import com.digitalasset.canton.ledger.api.util.{PageTokenUtils, TimestampConversion}
import com.digitalasset.canton.ledger.participant.state.index.IndexerHostedPartyFilter
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.config.HostedPartiesStreamsConfig
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.ScalaPbMessageWithPrecomputedSerializedSize
import com.digitalasset.canton.platform.store.backend.PartyStorageBackend.{
  HostedPartyPageBound,
  HostingState,
}
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, PartyStorageBackend}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.store.utils.ConcurrencyLimiter
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{Attributes, OverflowStrategy}

import scala.concurrent.{ExecutionContext, Future}

/** Streams the hosted parties snapshot at an offset in a four-step process consisting of:
  *   1. discovering page boundaries for the requested parties and participants across all
  *      synchronizers in interned (party, participant, synchronizer) order,
  *   1. filtering those ranges in parallel down to the latest event id of each hosting relationship
  *      matching the synchronizer filter, dropping revoked relationships,
  *   1. fetching the hosting relationships recorded by those events in batches,
  *   1. converting the hosting relationships in parallel to their API form with externalized ids
  *      and grouping the adjacent ones of each party into a response.
  *
  * The snapshot offset is resolved to an event sequential id once, so that all queries of a stream
  * observe the same snapshot. While pruning does not delete party-to-participant events, the
  * snapshot offset must not fall behind the pruning offset, so it is validated at that point and
  * revalidated after every payload query, as for the ACS.
  *
  * The filter strings are interned once up front as well: filter strings without an interned id
  * cannot match any hosting within the snapshot, so a filter referencing only unknown strings
  * yields an empty stream without running any paging queries.
  */
class HostedPartiesStreamReader(
    config: HostedPartiesStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    partyStorageBackend: PartyStorageBackend,
    eventStorageBackend: EventStorageBackend,
    stringInterning: StringInterning,
    participantId: Ref.ParticipantId,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val dbMetrics = metrics.index.db

  /** @param filter
    *   the parties, participants and synchronizers of interest
    * @param validAt
    *   the inclusive snapshot offset
    * @param fromExcl
    *   only parties positioned after this party in the snapshot order are streamed
    * @param limit
    *   the maximum number of parties streamed
    * @param continuationChecksum
    *   the request checksum embedded in the stream continuation token of each response
    * @return
    *   the hosted parties, in an order that is stable for a participant but not lexicographic
    */
  def streamHostedParties(
      filter: IndexerHostedPartyFilter,
      validAt: Offset,
      fromExcl: Option[Party],
      limit: Option[Int],
      continuationChecksum: PageTokenUtils.Checksum,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetHostedPartiesResponse, NotUsed] =
    Source
      .future(resolveSnapshotEventSeqId(validAt))
      .flatMapConcat(validAtEventSeqId =>
        (for {
          partyIds <- filter.parties.traverse(internAll(stringInterning.party.tryInternalize))
          synchronizerIds <- filter.synchronizerIds.traverse(
            internAll(stringInterning.synchronizerId.tryInternalize)
          )
          participantIdFilter <- Option
            .unless(filter.allParticipants)(
              stringInterning.participantId.tryInternalize(participantId)
            )
            .sequence
          fromInclusive <- startBound(fromExcl)
        } yield doStreamHostedParties(
          partyIds = partyIds,
          participantIdFilter = participantIdFilter,
          synchronizerIds = synchronizerIds,
          validAt = validAt,
          validAtEventSeqId = validAtEventSeqId,
          fromInclusive = fromInclusive,
          limit = limit,
          continuationChecksum = continuationChecksum,
        )).getOrElse(Source.empty)
      )

  /** Interns the requested values, dropping those never interned: they cannot match any hosting.
    * Returns None when none of them was ever interned.
    */
  private def internAll[T](intern: T => Option[Int])(
      requested: NonEmpty[Set[T]]
  ): Option[NonEmpty[Set[Int]]] =
    NonEmpty.from(requested.forgetNE.flatMap(intern(_)))

  /** Resolves the snapshot offset to the event sequential id all queries of the stream observe,
    * failing if the offset precedes the pruning offset or exceeds the ledger end.
    */
  private def resolveSnapshotEventSeqId(validAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Long] =
    withValidatedHostedAt(validAt)(
      dbDispatcher.executeSql(dbMetrics.getHostedPartiesEventSeqId)(
        eventStorageBackend.maxEventSequentialId(Some(validAt))
      )
    )

  private def withValidatedHostedAt[T](validAt: Offset)(query: => Future[T])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[T] =
    queryValidRange.withOffsetNotBeforePruning(
      offset = validAt,
      errorPruning = prunedOffset =>
        s"Hosted parties request at offset ${validAt.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
      errorLedgerEnd = ledgerEndOffset =>
        s"Hosted parties request at offset ${validAt.unwrap} is beyond ledger end offset ${ledgerEndOffset
            .fold(0L)(_.unwrap)}",
    )(query)

  /** The bound to resume the snapshot from. Returns None when the party to continue after was never
    * interned: no party can follow it then.
    */
  private def startBound(fromExcl: Option[Party]): Option[HostedPartyPageBound] =
    fromExcl.fold(Option(HostedPartyPageBound.Min))(party =>
      stringInterning.party
        .tryInternalize(party)
        .map(partyId => HostedPartyPageBound(partyId + 1, 0, 0))
    )

  private def doStreamHostedParties(
      partyIds: Option[NonEmpty[Set[Int]]],
      participantIdFilter: Option[Int],
      synchronizerIds: Option[NonEmpty[Set[Int]]],
      validAt: Offset,
      validAtEventSeqId: Long,
      fromInclusive: HostedPartyPageBound,
      limit: Option[Int],
      continuationChecksum: PageTokenUtils.Checksum,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetHostedPartiesResponse, NotUsed] = {
    def fetchBoundary(fromInclusive: HostedPartyPageBound): Future[Option[HostedPartyPageBound]] =
      globalIdQueriesLimiter.execute(
        dbDispatcher.executeSql(dbMetrics.getHostedPartyIdRanges)(
          partyStorageBackend.hostedPartyPageBoundary(
            partyIds = partyIds,
            participantId = participantIdFilter,
            fromInclusive = fromInclusive,
            limit = config.maxIdsPerIdPage.value,
            validAtEventSeqId = validAtEventSeqId,
          )
        )
      )

    def fetchIds(
        fromInclusive: HostedPartyPageBound,
        toInclusive: HostedPartyPageBound,
    ): Future[Vector[Long]] =
      globalIdQueriesLimiter.execute(
        dbDispatcher.executeSql(dbMetrics.getHostedPartyIds)(
          partyStorageBackend.hostedPartyIds(
            partyIds = partyIds,
            participantId = participantIdFilter,
            synchronizerIds = synchronizerIds,
            fromInclusive = fromInclusive,
            toInclusive = toInclusive,
            validAtEventSeqId = validAtEventSeqId,
          )
        )
      )

    def fetchHostings(ids: Iterable[Long]): Future[Vector[PartyStorageBackend.PartyHosting]] =
      globalPayloadQueriesLimiter
        .execute(
          withValidatedHostedAt(validAt)(
            dbDispatcher.executeSql(dbMetrics.getHostedPartyBatch)(
              partyStorageBackend.partyHostings(ids)
            )
          )
        )
        // The backend does not order the hostings, but groupedAdjacentBy needs them grouped by
        // party. Each batch is a contiguous slice of the triplet-ordered ids, and mapAsync keeps
        // the batch order, so sorting each batch orders the whole stream.
        .map(_.sortBy(hosting => (hosting.partyId, hosting.participantId, hosting.synchronizerId)))

    // Pagination advances even when a whole range is revoked or does not match the filters.
    val ids: Source[Long, NotUsed] =
      Source
        .unfoldAsync(fromInclusive) { pageFromInclusive =>
          fetchBoundary(pageFromInclusive).map(_.map { toInclusive =>
            toInclusive.successor -> (pageFromInclusive -> toInclusive)
          })
        }
        .mapAsync(config.idFilterQueryParallelism.value) { case (rangeFromInclusive, toInclusive) =>
          fetchIds(rangeFromInclusive, toInclusive)
        }
        .buffer(config.maxPagesPerIdPagesBuffer.value, OverflowStrategy.backpressure)
        .mapConcat(identity)

    // Pekko requires for this buffer's size to be a power of two.
    val inputBufferSize =
      Utils.largestSmallerOrEqualPowerOfTwo(config.maxParallelPayloadQueries.value)
    ids
      .batchNForMaxConcurrency(
        maxBatchSize = config.maxPayloadsPerPayloadsPage.value,
        maxBatchCount = config.maxParallelPayloadQueries.value + 1,
      )
      .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
      .mapAsync(config.maxParallelPayloadQueries.value)(fetchHostings(_).map(toHostingParticipants))
      .mapConcat(identity)
      .groupedAdjacentBy(_._1)
      .optionalTake(limit.map(_.toLong))
      .mapAsync(config.hostedPartiesProcessingParallelism.value)(
        toResponse(validAt, continuationChecksum)
      )
      .mapConcat(_.toList)
  }

  private def toHostingParticipants(
      hostings: Vector[PartyStorageBackend.PartyHosting]
  ): Vector[(Int, HostingParticipant)] =
    hostings.map(hosting =>
      hosting.partyId -> HostingParticipant(
        participantId = stringInterning.participantId.externalize(hosting.participantId),
        synchronizerId =
          stringInterning.synchronizerId.externalize(hosting.synchronizerId).toProtoPrimitive,
        recordTime = Some(TimestampConversion.fromLf(hosting.recordTime)),
        offset = hosting.offset.unwrap,
        participantPermission =
          EventsTable.TransactionConversions.toParticipantPermission(hosting.authorizationLevel),
        hostingState = hosting.hostingState match {
          case HostingState.Onboarding => ProtoHostingState.HOSTING_STATE_ONBOARDING
          case HostingState.Active => ProtoHostingState.HOSTING_STATE_ACTIVE
        },
      )
    )

  private def toResponse(
      validAt: Offset,
      continuationChecksum: PageTokenUtils.Checksum,
  )(hostings: Seq[(Int, HostingParticipant)]): Future[Option[GetHostedPartiesResponse]] =
    Future {
      hostings.headOption.map { case (partyId, _) =>
        val party = stringInterning.party.externalize(partyId)
        GetHostedPartiesResponse(
          hostedParty = Some(HostedParty(party = party, hostingParticipants = hostings.map(_._2))),
          hostedAtOffset = validAt.unwrap,
          streamContinuationToken =
            HostedPartiesContinuationToken.encode(party, validAt, continuationChecksum),
        ).withPrecomputedSerializedSize()
      }
    }
}
