// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LedgerSyncEvent
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  SyncDomainPersistentStateManager,
  TimestampedEvent,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfCommittedTransaction, LfContractId, SerializableContract}
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{
  SequencedEventNotFoundError,
  SequencedEventRangeOverlapsWithPruning,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LedgerTransactionId, RequestCounter}
import io.functionmeta.functionFullName
import org.slf4j.event.Level

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStreamWriter}
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream
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
      .parTraverse { case (alias, state) =>
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

  /** returns the potentially big ACS of a given domain */
  def findAcs(
      domain: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, CantonTimestamp]] = {
    val persistentState = syncDomainPersistentStateManager.getByAlias(domain)
    EitherT(
      persistentState
        .map(currentAcsSnapshot)
        .getOrElse(Future.successful(Left(SyncStateInspection.NoSuchDomain(domain))))
    )
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
          .getOrElse(Future.successful(Right(Map.empty[LfContractId, CantonTimestamp])))
      ) match {
        case Left(err) =>
          throw new IllegalArgumentException(s"failed to load ACS for ${domain} due to error $err")
        case Right(map) => map
      }

    timeouts.inspection.await("finding contracts in the persistent state")(
      getOrFail(persistentState, domain).contractStore
        .find(filterId, filterPackage, filterTemplate, limit)
        .map(_.map(sc => (acs.contains(sc.contractId), sc)))
    )
  }

  def storeActiveContractsToFile(
      parties: Set[PartyId],
      target: File,
      batchSize: PositiveInt,
      filterDomain: DomainId => Boolean = _ => true,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Either[String, Unit] = {
    import scala.util.Using
    val lfParties = parties.map(_.toLf)
    val encoder = java.util.Base64.getEncoder
    def openStream(): OutputStreamWriter = {
      val fileOut = new FileOutputStream(target)
      val bufferOut = if (target.getName.endsWith(".gz")) {
        // default gzip output buffer is 512, so setting it to the same as the
        // buffered output stream defaults
        new GZIPOutputStream(fileOut, 8192)
      } else {
        new BufferedOutputStream(fileOut)
      }
      new OutputStreamWriter(bufferOut)
    }
    logger.info(s"Downloading active contract set to ${target} for parties ${parties}")
    val first = new AtomicBoolean(true)
    Using(openStream()) { writer =>
      syncDomainPersistentStateManager.getAll.toList.sortBy(_._1.uid.id.unwrap).traverse {
        case (domainId, state) if filterDomain(domainId) =>
          val domainIdStr = domainId.uid.toProtoPrimitive
          // add an empty line between the contracts
          if (!first.getAndSet(false)) {
            writer.write("\n")
          }
          writer.write(SyncStateInspection.DomainIdPrefix)
          writer.write(domainIdStr)
          writer.write("\n")
          val storeF = for {
            // fetch acs
            acs <- EitherT(currentAcsSnapshot(state))
            // sort acs by coid (for easier comparison ...)
            grouped = acs.toList.sortBy(_._1.coid).grouped(batchSize.value)
            // fetch contracts
            _ <- EitherT.right[AcsError](MonadUtil.sequentialTraverse_(grouped) { batch =>
              logger.debug(
                s"Loading next batch of ${batch.size} contracts, looking for matching stakeholders."
              )
              val loadedF = state.contractStore.lookupManyUncached(batch.map(_._1))
              val storedF = loadedF.map { contracts =>
                contracts.foreach {
                  case Some(stored)
                      // filter for parties
                      if lfParties.exists(stored.contract.metadata.stakeholders.contains) =>
                    // write contract as base64 text to file
                    val byteStr = stored.contract.toByteString(protocolVersion)
                    writer.write(encoder.encodeToString(byteStr.toByteArray))
                    writer.write("\n")
                  case _ =>
                }
              }
              storedF
            })
          } yield ()
          timeouts.unbounded
            .await(s"Downloading ACS of ${domainId}", logFailing = Some(Level.WARN))(storeF.value)
        case _ => Right(())
      }
    }.fold(x => Left(x.getMessage), _.leftMap(_.toString).map(_ => ()))
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
      timeouts.inspection.await("finding events in the multi-domain event log")(
        participantNodePersistentState.multiDomainEventLog
          .lookupEventRange(None, limit)
          .map(_.map { case (offset, eventAndCausalChange) =>
            (offset.toString, eventAndCausalChange.tse)
          })
      )
    else {
      timeouts.inspection
        .await(s"$functionFullName from $from to $to in the event log")(
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
    closed.map(opener.open)
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
        .await(
          s"checking whether timestamp $beforeOrAt with ledgerEnd $ledgerEnd is safe to prune"
        )(
          pruningProcessor.safeToPrune(beforeOrAt, ledgerEndOffset).value
        )
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
      .toRight(s"Not connected to $domain")
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

  case class NoSuchDomain(alias: DomainAlias) extends AcsError

  lazy val DomainIdPrefix = "domain-id:"

}
