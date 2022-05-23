// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.OptionT
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.sync.{
  SyncDomainPersistentStateManager,
  TimestampedEvent,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.participant.{LedgerSyncEvent, RequestCounter}
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfCommittedTransaction, LfContractId, SerializableContract}
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{
  CursorPrehead,
  SequencedEventNotFoundError,
  SequencedEventRangeOverlapsWithPruning,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LedgerTransactionId}

import java.time.Instant
import java.util.NoSuchElementException
import scala.concurrent.{ExecutionContext, Future}

/** Implements inspection functions for the sync state of a participant node */
class SyncStateInspection(
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    participantNodePersistentState: ParticipantNodePersistentState,
    pruningProcessor: PruningProcessor,
    timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import SyncStateInspection.getOrFail

  /** For a set of contracts lookup which domain they are currently in.
    * If a contract is not found in a available ACS it will be omitted from the response.
    */
  def lookupContractDomain(
      contractIds: Set[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, DomainAlias]] = {
    def lookupAlias(domainId: DomainId): DomainAlias =
      // am assuming that an alias can't be missing once registered
      syncDomainPersistentStateManager
        .aliasForDomainId(domainId)
        .getOrElse(sys.error(s"missing alias for domain [$domainId]"))

    syncDomainPersistentStateManager.getAll.toList
      .map { case (id, state) => lookupAlias(id) -> state }
      .traverse { case (alias, state) =>
        OptionT(state.requestJournalStore.preheadClean)
          .semiflatMap(cleanRequest =>
            state.activeContractStore
              .contractSnapshot(contractIds, cleanRequest.timestamp)
              .fold(
                err => sys.error(s"acs snapshot failed for $alias: $err"),
                _.keySet.map(_ -> alias),
              )
          )
          .getOrElse(List.empty[(LfContractId, DomainAlias)])
      }
      .map(_.flatten.toMap)
  }

  def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): Future[Option[DomainId]] =
    participantNodePersistentState.multiDomainEventLog.lookupTransactionDomain(transactionId).value

  /** searches the pcs and returns the contract and activeness flag */
  def findContracts(
      domain: DomainAlias,
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): List[(Boolean, SerializableContract)] = {
    val persistentState = syncDomainPersistentStateManager.getByAlias(domain)
    val acs =
      timeouts.inspection.await()(
        // not failing on unknown domain to allow inspection of unconnected domains
        persistentState
          .map(currentAcsSnapshot)
          .getOrElse(Future.successful(Right(Map.empty[LfContractId, CantonTimestamp])))
      ) match {
        case Left(err) =>
          throw new IllegalArgumentException(s"failed to load ACS for ${domain} due to error $err")
        case Right(map) => map
      }

    timeouts.inspection.await()(
      getOrFail(persistentState, domain).contractStore
        .find(filterId, filterPackage, filterTemplate, limit)
        .map(_.map(sc => (acs.contains(sc.contractId), sc)))
    )
  }

  def contractCount(domain: DomainAlias)(implicit traceContext: TraceContext): Future[Int] = {
    val state = syncDomainPersistentStateManager
      .getByAlias(domain)
      .getOrElse(throw new IllegalArgumentException(s"Unable to find contract store for $domain."))
    state.contractStore.contractCount()
  }

  def contractCountInAcs(domain: DomainAlias, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[Int]] = {
    getPersistentState(domain) match {
      case None => Future.successful(None)
      case Some(state) => state.activeContractStore.contractCount(timestamp).map(Some(_))
    }
  }

  def requestJournalSize(
      domain: DomainAlias,
      start: CantonTimestamp = CantonTimestamp.Epoch,
      end: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Option[Int] = {
    getPersistentState(domain).map { state =>
      timeouts.inspection.await()(state.requestJournalStore.size(start, end))
    }
  }

  private def currentAcsSnapshot(persistentState: SyncDomainPersistentState)(implicit
      traceContext: TraceContext
  ): Future[Either[AcsError, Map[LfContractId, CantonTimestamp]]] =
    for {
      cursorHeadO <- persistentState.requestJournalStore.preheadClean
      snapshot <- cursorHeadO.fold(
        Future.successful(Either.right[AcsError, Map[LfContractId, CantonTimestamp]](Map.empty))
      )(cursorHead => persistentState.activeContractStore.snapshot(cursorHead.timestamp))
    } yield snapshot

  def findAcceptedTransactions(
      domain: DomainAlias,
      from: Option[CantonTimestamp] = None,
      to: Option[CantonTimestamp] = None,
      limit: Option[Int] = None,
  )(implicit
      traceContext: TraceContext
  ): Seq[(SyncStateInspection.DisplayOffset, LfCommittedTransaction)] = {
    // Need to apply limit after filtering for TransactionAccepted-Events (else might miss transactions due to
    // limit also counting towards non-TransactionAccepted-Events
    val found = findEvents(domain, from, to).collect {
      case (offset, TimestampedEvent(accepted: LedgerSyncEvent.TransactionAccepted, _, _, _)) =>
        offset -> accepted.transaction
    }
    limit.fold(found)(n => found.take(n))
  }

  /** Returns the events from the given domain; if the specified domain is empty, returns the events from the combined,
    * multi-domain event log. `from` and `to` only have an effect if the domain isn't empty.
    * @throws scala.RuntimeException (by Await.result and if lookup fails)
    */
  def findEvents(
      domain: DomainAlias,
      from: Option[CantonTimestamp] = None,
      to: Option[CantonTimestamp] = None,
      limit: Option[Int] = None,
  )(implicit
      traceContext: TraceContext
  ): Seq[(SyncStateInspection.DisplayOffset, TimestampedEvent)] =
    if (domain.unwrap == "")
      timeouts.inspection.await()(
        participantNodePersistentState.multiDomainEventLog
          .lookupEventRange(None, limit)
          .map(_.map { case (offset, eventAndCausalChange) =>
            (offset.toString, eventAndCausalChange.tse)
          })
      )
    else {
      timeouts.inspection
        .await()(
          getOrFail(getPersistentState(domain), domain).eventLog
            .lookupEventRange(None, None, from, to, limit)
        )
        .toSeq
        .map { case (offset, eventAndCausalChange) =>
          (offset.toString, eventAndCausalChange.tse)
        }
    }

  private def tryGetProtocolVersion(
      state: SyncDomainPersistentState,
      domain: DomainAlias,
  )(implicit traceContext: TraceContext): ProtocolVersion =
    timeouts.inspection
      .await()(state.parameterStore.lastParameters)
      .getOrElse(throw new IllegalStateException(s"No static domain parameters found for $domain"))
      .protocolVersion

  def findMessages(
      domain: DomainAlias,
      from: Option[Instant],
      to: Option[Instant],
      limit: Option[Int],
  )(implicit traceContext: TraceContext): Seq[PossiblyIgnoredProtocolEvent] = {
    val state = getPersistentState(domain).getOrElse(
      throw new NoSuchElementException(s"Unknown domain $domain")
    )
    val messagesF =
      if (from.isEmpty && to.isEmpty) state.sequencedEventStore.sequencedEvents(limit)
      else { // if a timestamp is set, need to use less efficient findRange method (it sorts results first)
        val cantonFrom =
          from.map(t => CantonTimestamp.assertFromInstant(t)).getOrElse(CantonTimestamp.MinValue)
        val cantonTo =
          to.map(t => CantonTimestamp.assertFromInstant(t)).getOrElse(CantonTimestamp.MaxValue)
        state.sequencedEventStore.findRange(ByTimestampRange(cantonFrom, cantonTo), limit).valueOr {
          // this is an inspection command, so no need to worry about pruned events
          case SequencedEventRangeOverlapsWithPruning(_criterion, _pruningStatus, foundEvents) =>
            foundEvents
        }
      }
    val closed = timeouts.inspection.await()(messagesF)
    val opener =
      new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
        tryGetProtocolVersion(state, domain),
        state.pureCryptoApi,
      )
    closed.map(opener.open)
  }

  def findMessage(domain: DomainAlias, criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): Either[SequencedEventNotFoundError, PossiblyIgnoredProtocolEvent] = {
    val state = getPersistentState(domain).getOrElse(
      throw new NoSuchElementException(s"Unknown domain $domain")
    )
    val messageF = state.sequencedEventStore.find(criterion).value
    val closed = timeouts.inspection.await()(messageF)
    val opener = new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
      tryGetProtocolVersion(state, domain),
      state.pureCryptoApi,
    )
    closed.map(opener.open)
  }

  def safeToPrune(beforeOrAt: CantonTimestamp, ledgerEnd: LedgerOffset)(implicit
      traceContext: TraceContext
  ): Either[String, Option[LedgerOffset]] = {
    for {
      ledgerEndOffset <- UpstreamOffsetConvert
        .toLedgerSyncOffset(ledgerEnd)
        .flatMap(UpstreamOffsetConvert.toGlobalOffset)
      pruningOffsetO <- timeouts.inspection
        .await()(pruningProcessor.safeToPrune(beforeOrAt, ledgerEndOffset).value)
        .leftMap(_.message)
    } yield pruningOffsetO.map(UpstreamOffsetConvert.toLedgerOffset)
  }

  def findComputedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await()(
      getOrFail(persistentState, domain).acsCommitmentStore
        .searchComputedBetween(start, end, counterParticipant)
    )
  }

  def findReceivedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit traceContext: TraceContext): Iterable[SignedProtocolMessage[AcsCommitment]] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await()(
      getOrFail(persistentState, domain).acsCommitmentStore
        .searchReceivedBetween(start, end, counterParticipant)
    )
  }

  def outstandingCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Iterable[(CommitmentPeriod, ParticipantId)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await()(
      getOrFail(persistentState, domain).acsCommitmentStore
        .outstanding(start, end, counterParticipant)
    )
  }

  def noOutstandingCommitmentsTs(domain: DomainAlias, beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[CantonTimestamp] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await()(
      getOrFail(persistentState, domain).acsCommitmentStore.noOutstandingCommitments(beforeOrAt)
    )
  }

  /** Update the prehead for clean requests to the given value, bypassing all checks. Only used for testing. */
  def forceCleanPrehead(newHead: Option[CursorPrehead[RequestCounter]], domain: DomainAlias)(
      implicit traceContext: TraceContext
  ): Either[String, Future[Unit]] = {
    getPersistentState(domain)
      .map(state => state.requestJournalStore.overridePreheadCleanForTesting(newHead))
      .toRight(s"Not connected to $domain")
  }

  def lookupCleanPrehead(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[CursorPrehead[RequestCounter]]]] =
    getPersistentState(domain)
      .map(state => state.requestJournalStore.preheadClean)
      .toRight(s"Not connected to $domain")

  def requestStateInJournal(rc: RequestCounter, domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestJournal.RequestData]]] =
    getPersistentState(domain)
      .toRight(s"Not connected to $domain")
      .map(state => state.requestJournalStore.query(rc).value)

  private[this] def getPersistentState(domain: DomainAlias): Option[SyncDomainPersistentState] =
    syncDomainPersistentStateManager.getByAlias(domain)

  def locateOffset(
      numTransactions: Long
  )(implicit traceContext: TraceContext): Future[Either[String, LedgerOffset]] = {

    if (numTransactions <= 0L)
      throw new IllegalArgumentException(
        s"Number of transactions needs to be positive and not ${numTransactions}"
      )

    participantNodePersistentState.multiDomainEventLog
      .locateOffset(numTransactions - 1L)
      .fold(
        Left(s"Participant does not contain ${numTransactions} transactions."): Either[
          String,
          LedgerOffset,
        ]
      )(o => Right(UpstreamOffsetConvert.toLedgerOffset(o)))
  }

  def getOffsetByTime(
      pruneUpTo: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[LedgerOffset]] =
    participantNodePersistentState.multiDomainEventLog
      .getOffsetByTimeUpTo(pruneUpTo)
      .map(UpstreamOffsetConvert.toLedgerOffset)
      .value

}

object SyncStateInspection {
  type DisplayOffset = String

  private def getOrFail[T](opt: Option[T], domain: DomainAlias): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such domain [${domain}]"))
}
