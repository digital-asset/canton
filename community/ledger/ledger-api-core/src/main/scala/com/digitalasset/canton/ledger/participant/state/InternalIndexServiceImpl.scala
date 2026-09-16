// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import cats.implicits.catsSyntaxOptionId
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.logging.entries.LoggingEntries
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.state.AcsRangeInfo
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  ParticipantAuthorizationFormat,
  TopologyFormat,
  UpdateFormat,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdatesResponse
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfigOverrides
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.RecoveryStrategy
import com.digitalasset.daml.lf.data.Ref.Party
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class InternalIndexServiceImpl(
    indexService: IndexService,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends InternalIndexService
    with NamedLogging {

  override def activeContracts(
      partyIds: Set[LfPartyId],
      validAt: Option[Offset],
      recoveryStrategy: RecoveryStrategy,
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
    loggedStream(
      logMarker = "Internal Active Contracts Stream",
      inputParamsForLogging = s"partyIds: $partyIds, validAt: $validAt",
    ) { implicit traceContext =>
      PekkoUtil
        .recoveringSource(Option.empty[ByteString], recoveryStrategy)(continuationToken =>
          indexService
            .getActiveContracts(
              eventFormat = EventFormat(
                filtersByParty =
                  partyIds.view.map(_ -> CumulativeFilter.templateWildcardFilter(true)).toMap,
                filtersForAnyParty =
                  Option.when(partyIds.isEmpty)(CumulativeFilter.templateWildcardFilter(true)),
                verbose = false,
              ),
              activeAt = validAt,
              rangeInfo = AcsRangeInfo.assertFromContinuationTokenBytes(continuationToken),
              configOverrides = None,
            )(loggingContext)
        )(_.streamContinuationToken.some)
    }

  override def topologyTransactions(
      partyId: LfPartyId,
      fromExclusive: Offset,
      recoveryStrategy: RecoveryStrategy,
  )(implicit traceContext: TraceContext): Source[TopologyTransaction, NotUsed] =
    loggedStream(
      logMarker = "Internal Topology Transaction Update Stream",
      inputParamsForLogging = s"partyId: $partyId, fromExclusive: $fromExclusive",
    ) { implicit traceContext =>
      PekkoUtil
        .recoveringSource(fromExclusive.unwrap, recoveryStrategy)(fromExclusive =>
          indexService
            .updates(
              begin = Some(Offset.tryFromLong(fromExclusive)),
              endAt = None,
              updateFormat = UpdateFormat(
                includeTransactions = None,
                includeReassignments = None,
                includeTopologyEvents = Some(
                  TopologyFormat(
                    participantAuthorizationFormat = Some(
                      ParticipantAuthorizationFormat(
                        parties = Some(Set(partyId))
                      )
                    ),
                    synchronizerParametersFormat = false,
                    synchronizerId = None,
                  )
                ),
                includeAcsCommitments = None,
                includeAcsChanges = None,
              ),
              descendingOrder = false,
              skipPruningChecks = false,
            )(loggingContext)
            .collect { case UpdatesResponse.ProtoUpdates(Some(update), _) =>
              update
            }
            .mapConcat(_.update.topologyTransaction)
        )(_.offset)
    }

  override def acsUpdates(
      synchronizerId: SynchronizerId,
      fromExclusive: Option[Offset],
      recoveryStrategy: RecoveryStrategy,
  )(implicit
      traceContext: TraceContext
  ): Source[InternalIndexService.AcsUpdateContainer, NotUsed] =
    loggedStream(
      logMarker = "Internal ACS Updates Stream",
      inputParamsForLogging = s"synchronizerId: $synchronizerId, fromExclusive: $fromExclusive",
    ) { implicit traceContext =>
      val updateFormat = UpdateFormat(
        includeTransactions = None,
        includeReassignments = None,
        includeTopologyEvents = Some(
          TopologyFormat(
            participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(parties = None)),
            synchronizerParametersFormat = true,
            synchronizerId = Some(synchronizerId),
          )
        ),
        includeAcsCommitments = Some(synchronizerId),
        includeAcsChanges = Some(synchronizerId),
      )

      PekkoUtil.recoveringSource(fromExclusive, recoveryStrategy)(begin =>
        indexService
          .updates(
            begin = begin,
            endAt = None,
            updateFormat = updateFormat,
            descendingOrder = false,
            skipPruningChecks = false,
          )(loggingContext)
          .mapConcat(
            InternalIndexService.AcsUpdateContainer.fromUpdatesResponse(synchronizerId)
          )
      )(_.offset.some)
    }

  override def acs(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      stakeholders1: Set[Party],
      stakeholders2: Set[Party],
      configOverrides: ActiveContractsServiceStreamsConfigOverrides,
      recoveryStrategy: RecoveryStrategy,
  )(implicit traceContext: TraceContext): Source[InternalIndexService.ActiveContract, NotUsed] =
    if (stakeholders1.isEmpty)
      Source.failed(
        new IllegalArgumentException("acs requires a non-empty stakeholders1 set")
      )
    else {
      def onlyFirstTwenty(parties: Set[Party]) = parties.toVector.sorted.take(10).toString()
      val first20Stakeholders1 = onlyFirstTwenty(stakeholders1)
      val first20Stakeholders2 = onlyFirstTwenty(stakeholders2)
      loggedStream(
        logMarker = "Internal ACS Stream",
        inputParamsForLogging =
          s"synchronizerId: $synchronizerId, activeAt: $activeAt, stakeholders1 (first 20 shown): $first20Stakeholders1, stakeholders2 (first 20 shown): $first20Stakeholders2",
      )(implicit traceContext =>
        PekkoUtil.recoveringSource(Option.empty[ByteString], recoveryStrategy)(continuationToken =>
          indexService.acs(
            synchronizerId = synchronizerId,
            activeAt = activeAt,
            stakeholders1 = stakeholders1,
            stakeholders2 = stakeholders2,
            configOverrides = configOverrides,
            continuationToken = continuationToken,
          )(loggingContext)
        )(_.continuationToken.some)
      )
    }

  override def counterParties(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      party: Option[Party],
      configOverrides: ActiveContractsServiceStreamsConfigOverrides,
      recoveryStrategy: RecoveryStrategy,
  )(implicit traceContext: TraceContext): Source[LfPartyId, NotUsed] =
    loggedStream(
      logMarker = "Internal ACS Stream For counter-parties",
      inputParamsForLogging = s"synchronizerId: $synchronizerId, activeAt: $activeAt, party: $party",
    )(implicit traceContext =>
      PekkoUtil
        .recoveringSource(Option.empty[ByteString], recoveryStrategy)(continuationToken =>
          indexService.acs(
            synchronizerId = synchronizerId,
            activeAt = activeAt,
            // an empty stakeholders1 means "any party"
            stakeholders1 = party.toList.toSet,
            stakeholders2 = Set.empty,
            configOverrides = configOverrides,
            continuationToken = continuationToken,
          )(loggingContext)
        )(_.continuationToken.some)
        .mapConcat(_.stakeholders)
        // Emits each distinct element of the stream only once.
        // The full set of already-seen elements is kept in memory for the lifetime of the stream.
        .statefulMapConcat { () =>
          val seen = mutable.Set.empty[Party]
          elem => Option.when(seen.add(elem))(elem).toList
        }
    )

  override def indexerPrunedUpTo(implicit traceContext: TraceContext): Future[Option[Offset]] =
    indexService.indexDbPrunedUpto(loggingContext)

  private def loggingContext(implicit traceContext: TraceContext): LoggingContextWithTrace =
    new LoggingContextWithTrace(LoggingEntries.empty, traceContext)

  private def loggedStream[T, Mat](
      logMarker: String,
      inputParamsForLogging: String,
  )(
      streamFactory: TraceContext => Source[T, Mat]
  )(implicit traceContext: TraceContext): Source[T, Mat] = {
    val originalTraceContext = traceContext
    TraceContext.withNewTraceContext(logMarker) { implicit traceContext: TraceContext =>
      logger.info(
        s"Starting internal stream [$logMarker] with new tid: ${traceContext.showTraceId}"
      )(originalTraceContext)
      logger.info(
        s"Starting internal stream [$logMarker] with original tid: ${originalTraceContext.showTraceId} $inputParamsForLogging"
      )
      streamFactory(traceContext)
        .watchTermination() { (mat, done) =>
          done.onComplete {
            case Success(_) =>
              logger.info(s"Internal stream [$logMarker] successfully completed")
            case Failure(throwable) =>
              logger.warn(s"Internal stream [$logMarker] completed with a failure", throwable)
          }
          mat
        }
    }
  }
}
