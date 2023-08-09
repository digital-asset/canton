// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.{Eval, Foldable}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.SyncStateInspection.{
  SerializableContractWithDomainId,
  TimestampValidation,
}
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateManager,
  TimestampedEvent,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.participant.{GlobalOffset, Pruning}
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfCommittedTransaction, LfContractId, SerializableContract}
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.store.CursorPrehead.{
  RequestCounterCursorPrehead,
  SequencerCounterCursorPrehead,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{
  SequencedEventNotFoundError,
  SequencedEventRangeOverlapsWithPruning,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LedgerTransactionId, LfPartyId, RequestCounter}

import java.io.OutputStream
import java.time.Instant
import java.util.Base64
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Implements inspection functions for the sync state of a participant node */
final class SyncStateInspection(
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
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
      .parTraverse { case (alias, state) =>
        OptionT(state.requestJournalStore.preheadClean)
          .semiflatMap(cleanRequest =>
            state.activeContractStore
              .contractSnapshot(contractIds, cleanRequest.timestamp)
              .map(_.keySet.map(_ -> alias))
          )
          .getOrElse(List.empty[(LfContractId, DomainAlias)])
      }
      .map(_.flatten.toMap)
  }

  def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): Future[Option[DomainId]] =
    participantNodePersistentState.value.multiDomainEventLog
      .lookupTransactionDomain(transactionId)
      .value

  /** returns the potentially big ACS of a given domain */
  def findAcs(
      domain: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, CantonTimestamp]] = {
    syncDomainPersistentStateManager.getByAlias(domain) match {
      case Some(persistentState) =>
        EitherT.liftF(currentAcsSnapshot(persistentState))
      case None =>
        EitherT.leftT(SyncStateInspection.NoSuchDomain(domain))
    }
  }

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
      timeouts.inspection.await("creating an ACS snapshot")(
        // not failing on unknown domain to allow inspection of unconnected domains
        persistentState
          .map(currentAcsSnapshot)
          .getOrElse(Future.successful(Map.empty[LfContractId, CantonTimestamp]))
      )

    timeouts.inspection.await("finding contracts in the persistent state")(
      getOrFail(persistentState, domain).contractStore
        .find(filterId, filterPackage, filterTemplate, limit)
        .map(_.map(sc => (acs.contains(sc.contractId), sc)))
    )
  }

  def writeActiveContracts(
      outputStream: OutputStream,
      filterDomain: DomainId => Boolean,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      protocolVersion: Option[ProtocolVersion],
      contractDomainRenames: Map[DomainId, DomainId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, (DomainId, SyncStateInspection.Error), Unit] =
    MonadUtil.sequentialTraverse_(syncDomainPersistentStateManager.getAll) {
      case (domainId, state) if filterDomain(domainId) =>
        val domainIdForExport = contractDomainRenames.getOrElse(domainId, domainId)
        for {
          useProtocolVersion <- protocolVersion.fold(getProtocolVersion(state))(EitherT.rightT(_))
          _ <- writeActiveContracts(
            outputStream,
            domainIdForExport,
            parties,
            state,
            timestamp,
            useProtocolVersion,
          ).leftMap(domainId -> _)
        } yield ()
      case _ =>
        EitherTUtil.unit
    }

  private def getProtocolVersion[A](
      state: SyncDomainPersistentState
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, A, ProtocolVersion] =
    EitherT.right[A](
      state.parameterStore.lastParameters.map(_.fold(ProtocolVersion.latest)(_.protocolVersion))
    )

  // fetch acs, checking that the requested timestamp is clean
  private def acsSnapshotAt(state: SyncDomainPersistentState)(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncStateInspection.Error, SortedMap[LfContractId, CantonTimestamp]] =
    for {
      _ <- TimestampValidation.beforePrehead(state.requestJournalStore.preheadClean, timestamp)
      snapshot <- EitherT.right(state.activeContractStore.snapshot(timestamp))
      // check after getting the snapshot in case a pruning was happening concurrently
      _ <- TimestampValidation.afterPruning(state.activeContractStore.pruningStatus, timestamp)
    } yield snapshot.view.mapValues { case (ts, _transferCounter) => ts }.to(SortedMap)

  // sort acs for easier comparison
  private def getAcsSnapshot(
      state: SyncDomainPersistentState,
      timestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncStateInspection.Error, Iterator[
    SortedMap[LfContractId, CantonTimestamp]
  ]] =
    timestamp
      .map(acsSnapshotAt(state))
      .getOrElse(EitherT.right(currentAcsSnapshot(state).map(SortedMap.from(_))))
      .map(_.grouped(SyncStateInspection.BatchSize.value))

  private def writeActiveContracts(
      outputStream: OutputStream,
      domainIdForExport: DomainId,
      parties: Set[LfPartyId],
      state: SyncDomainPersistentState,
      timestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncStateInspection.Error, Unit] =
    for {
      acs <- getAcsSnapshot(state, timestamp)
      unit <- MonadUtil.sequentialTraverse_(acs)(
        writeBatch(outputStream, domainIdForExport, parties, protocolVersion, state)
      )
    } yield unit

  private def writeBatch(
      outputStream: OutputStream,
      domainIdForExport: DomainId,
      parties: Set[LfPartyId],
      protocolVersion: ProtocolVersion,
      state: SyncDomainPersistentState,
  )(
      batch: SortedMap[LfContractId, CantonTimestamp]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncStateInspection.Error, Unit] = EitherT {
    state.contractStore
      .lookupManyUncached(batch.keysIterator.toSeq)
      .map(_.partitionMap {
        case (contractId, None) => Left(contractId)
        case (_, Some(stored)) => Right(stored.contract)
      })
      .map {
        case (Nil, contracts) =>
          Try {
            for (contract <- contracts if parties.exists(contract.metadata.stakeholders)) {
              val domainToContract = SerializableContractWithDomainId(domainIdForExport, contract)
              val encodedContract = domainToContract.encode(protocolVersion)
              outputStream.write(encodedContract.getBytes)
              outputStream.flush()
            }
          }.toEither.leftMap(SyncStateInspection.Error.UnexpectedError)
        case (missingContractIds, _) =>
          Left(SyncStateInspection.Error.InconsistentSnapshot(missingContractIds))
      }
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
      timeouts.inspection.await(
        s"$functionFullName from $start to $end from the journal of domain $domain"
      )(
        state.requestJournalStore.size(start, end)
      )
    }
  }

  def currentAcsSnapshot(persistentState: SyncDomainPersistentState)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] =
    for {
      cursorHeadO <- persistentState.requestJournalStore.preheadClean
      snapshot <- cursorHeadO.fold(
        Future.successful(Map.empty[LfContractId, CantonTimestamp])
      )(cursorHead =>
        persistentState.activeContractStore
          .snapshot(cursorHead.timestamp)
          .map(_.unsorted.map { case (cid, (ts, _transferCounter)) => cid -> ts })
      )
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
      timeouts.inspection.await("finding events in the multi-domain event log")(
        participantNodePersistentState.value.multiDomainEventLog
          .lookupEventRange(None, limit)
          .map(_.map { case (offset, event) => (offset.toString, event) })
      )
    else {
      timeouts.inspection
        .await(s"$functionFullName from $from to $to in the event log")(
          getOrFail(getPersistentState(domain), domain).eventLog
            .lookupEventRange(None, None, from, to, limit)
        )
        .toSeq
        .map { case (offset, event) => (offset.toString, event) }
    }

  private def tryGetProtocolVersion(
      state: SyncDomainPersistentState,
      domain: DomainAlias,
  )(implicit traceContext: TraceContext): ProtocolVersion =
    timeouts.inspection
      .await(functionFullName)(state.parameterStore.lastParameters)
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
    val closed =
      timeouts.inspection.await(s"finding messages from $from to $to on $domain")(messagesF)
    val opener =
      new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
        tryGetProtocolVersion(state, domain),
        state.pureCryptoApi,
      )
    closed.map(opener.tryOpen)
  }

  def findMessage(domain: DomainAlias, criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): Either[SequencedEventNotFoundError, PossiblyIgnoredProtocolEvent] = {
    val state = getPersistentState(domain).getOrElse(
      throw new NoSuchElementException(s"Unknown domain $domain")
    )
    val messageF = state.sequencedEventStore.find(criterion).value
    val closed =
      timeouts.inspection.await(s"$functionFullName on $domain matching $criterion")(messageF)
    val opener = new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
      tryGetProtocolVersion(state, domain),
      state.pureCryptoApi,
    )
    closed.map(opener.tryOpen)
  }

  def safeToPrune(beforeOrAt: CantonTimestamp, ledgerEndOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Pruning.LedgerPruningError, Option[GlobalOffset]] = pruningProcessor
    .safeToPrune(beforeOrAt, ledgerEndOffset)

  def findComputedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
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
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
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
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore
        .outstanding(start, end, counterParticipant)
    )
  }

  def noOutstandingCommitmentsTs(domain: DomainAlias, beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[CantonTimestamp] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName on $domain for ts $beforeOrAt")(
      getOrFail(persistentState, domain).acsCommitmentStore.noOutstandingCommitments(beforeOrAt)
    )
  }

  /** Update the prehead for clean requests to the given value, bypassing all checks. Only used for testing. */
  def forceCleanPrehead(
      newHead: Option[RequestCounterCursorPrehead],
      domain: DomainAlias,
  )(implicit
      traceContext: TraceContext
  ): Either[String, Future[Unit]] = {
    getPersistentState(domain)
      .map(state => state.requestJournalStore.overridePreheadCleanForTesting(newHead))
      .toRight(s"Unknown domain $domain")
  }

  def forceCleanSequencerCounterPrehead(
      newHead: Option[SequencerCounterCursorPrehead],
      domain: DomainAlias,
  )(implicit traceContext: TraceContext): Either[String, Future[Unit]] = {
    getPersistentState(domain)
      .map(state => state.sequencerCounterTrackerStore.rewindPreheadSequencerCounter(newHead))
      .toRight(s"Unknown domain $domain")
  }

  def lookupCleanPrehead(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestCounterCursorPrehead]]] =
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
        s"Number of transactions needs to be positive and not $numTransactions"
      )

    participantNodePersistentState.value.multiDomainEventLog
      .locateOffset(numTransactions - 1L)
      .toRight(s"Participant does not contain $numTransactions transactions.")
      .map(UpstreamOffsetConvert.toLedgerOffset)
      .value
  }

  def getOffsetByTime(
      pruneUpTo: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[LedgerOffset]] =
    participantNodePersistentState.value.multiDomainEventLog
      .getOffsetByTimeUpTo(pruneUpTo)
      .map(UpstreamOffsetConvert.toLedgerOffset)
      .value

  def lookupPublicationTime(
      ledgerOffset: LedgerOffset
  )(implicit traceContext: TraceContext): EitherT[Future, String, CantonTimestamp] = for {
    globalOffset <- EitherT.fromEither[Future](
      UpstreamOffsetConvert.ledgerOffsetToGlobalOffset(ledgerOffset)
    )
    res <- participantNodePersistentState.value.multiDomainEventLog
      .lookupOffset(globalOffset)
      .toRight(s"offset $ledgerOffset not found")
    (_eventLogId, _localOffset, publicationTimestamp) = res
  } yield publicationTimestamp

}

object SyncStateInspection {

  private type DisplayOffset = String

  private val BatchSize = PositiveInt.tryCreate(1000)

  private def getOrFail[T](opt: Option[T], domain: DomainAlias): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such domain [$domain]"))

  private final case class NoSuchDomain(alias: DomainAlias) extends AcsError

  final case class SerializableContractWithDomainId(
      domainId: DomainId,
      contract: SerializableContract,
  ) {

    import SerializableContractWithDomainId.{Delimiter, encoder}

    def encode(protocolVersion: ProtocolVersion): String = {
      val byteStr = contract.toByteString(protocolVersion)
      val encoded = encoder.encodeToString(byteStr.toByteArray)
      val domain = domainId.filterString
      s"$domain$Delimiter$encoded\n"
    }
  }

  object SerializableContractWithDomainId {
    private val Delimiter = ":::"
    private val decoder = java.util.Base64.getDecoder
    private val encoder: Base64.Encoder = java.util.Base64.getEncoder
    def decode(line: String, lineNumber: Int): Either[String, SerializableContractWithDomainId] =
      line.split(Delimiter).toList match {
        case domainId :: contractByteString :: Nil =>
          for {
            domainId <- DomainId.fromString(domainId)
            contract <- SerializableContract
              .fromByteArray(decoder.decode(contractByteString))
              .leftMap(err => s"Failed parsing disclosed contract: $err")
          } yield SerializableContractWithDomainId(domainId, contract)
        case line => Either.left(s"Failed parsing line $lineNumber: $line ")
      }
  }

  sealed abstract class Error extends Product with Serializable

  object Error {

    final case class TimestampAfterPrehead private[SyncStateInspection] (
        requestedTimestamp: CantonTimestamp,
        cleanTimestamp: CantonTimestamp,
    ) extends Error

    final case class TimestampBeforePruning private[SyncStateInspection] (
        requestedTimestamp: CantonTimestamp,
        prunedTimestamp: CantonTimestamp,
    ) extends Error

    final case class InconsistentSnapshot(missingContracts: List[LfContractId]) extends Error

    final case class UnexpectedError(cause: Throwable) extends Error

  }

  private object TimestampValidation {

    private def validate[A, F[_]: Foldable](ffa: Future[F[A]])(p: A => Boolean)(fail: A => Error)(
        implicit ec: ExecutionContext
    ): EitherT[Future, Error, Unit] =
      EitherT(ffa.map(_.traverse_(a => EitherUtil.condUnitE(p(a), fail(a)))))

    def beforePrehead(
        cursorPrehead: Future[Option[RequestCounterCursorPrehead]],
        timestamp: CantonTimestamp,
    )(implicit ec: ExecutionContext): EitherT[Future, Error, Unit] =
      validate(cursorPrehead)(timestamp < _.timestamp)(cp =>
        Error.TimestampAfterPrehead(timestamp, cp.timestamp)
      )

    def afterPruning(pruningStatus: Future[Option[PruningStatus]], timestamp: CantonTimestamp)(
        implicit ec: ExecutionContext
    ): EitherT[Future, Error, Unit] =
      validate(pruningStatus)(timestamp >= _.timestamp)(ps =>
        Error.TimestampBeforePruning(timestamp, ps.timestamp)
      )
  }

}
