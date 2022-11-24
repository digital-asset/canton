// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import akka.stream.Materializer
import cats.Monad
import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.{CantonError, HasDegradationState}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.config.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.{
  DomainHandle,
  DomainRegistryError,
  SequencerBasedRegisterTopologyTransactionHandle,
}
import com.digitalasset.canton.participant.event.{AcsChange, RecordTime}
import com.digitalasset.canton.participant.metrics.{PruningMetrics, SyncDomainMetrics}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.submission.{
  ConfirmationRequestFactory,
  InFlightSubmissionTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  DomainNotReady,
  TransferProcessorError,
}
import com.digitalasset.canton.participant.protocol.transfer.*
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  PruneObserver,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.{
  ParticipantNodePersistentState,
  StoredContract,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
}
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.PeriodicAcknowledgements
import com.digitalasset.canton.sequencing.handlers.{CleanSequencerCounterTracker, EnvelopeOpener}
import com.digitalasset.canton.sequencing.protocol.{Batch, Envelope}
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  DelayLogger,
  HandlerResult,
  PossiblyIgnoredApplicationHandler,
  PossiblyIgnoredProtocolEvent,
  SubscriptionStart,
}
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, MonadUtil}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import io.functionmeta.functionFullName
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** A connected domain from the synchronization service.
  *
  * @param domainId                  The identifier of the connected domain.
  * @param domainHandle              A domain handle providing sequencer clients.
  * @param participantId             The participant node id hosting this sync service.
  * @param persistent                The persistent state of the sync domain.
  * @param ephemeral                 The ephemeral state of the sync domain.
  * @param packageService            Underlying package management service.
  * @param domainCrypto              Synchronisation crypto utility combining IPS and Crypto operations for a single domain.
  * @param topologyProcessor         Processor of topology messages from the sequencer.
  */
class SyncDomain(
    val domainId: DomainId,
    domainHandle: DomainHandle,
    participantId: ParticipantId,
    damle: DAMLe,
    parameters: ParticipantNodeParameters,
    participantNodePersistentState: ParticipantNodePersistentState,
    private[sync] val persistent: SyncDomainPersistentState,
    val ephemeral: SyncDomainEphemeralState,
    val packageService: PackageService,
    domainCrypto: DomainSyncCryptoClient,
    partyNotifier: LedgerServerPartyNotifier,
    val topologyClient: DomainTopologyClientWithInit,
    identityPusher: ParticipantTopologyDispatcher,
    transferCoordination: TransferCoordination,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    messageDispatcherFactory: MessageDispatcher.Factory[MessageDispatcher],
    clock: Clock,
    selfKillSwitch: => Unit,
    pruningMetrics: PruningMetrics,
    metrics: SyncDomainMetrics,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with StartAndCloseable[Either[SyncDomainInitializationError, Unit]]
    with TransferSubmissionHandle
    with HasDegradationState[CantonError] {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private[canton] val sequencerClient = domainHandle.sequencerClient
  val timeTracker: DomainTimeTracker = ephemeral.timeTracker
  val staticDomainParameters: StaticDomainParameters = domainHandle.staticParameters

  private val seedGenerator =
    new SeedGenerator(domainCrypto.crypto.pureCrypto)

  private[canton] val requestGenerator =
    ConfirmationRequestFactory(participantId, domainId, staticDomainParameters.protocolVersion)(
      domainCrypto.crypto.pureCrypto,
      seedGenerator,
      packageService,
      parameters.loggingConfig,
      staticDomainParameters.uniqueContractKeys,
      loggerFactory,
    )

  private val transactionProcessor: TransactionProcessor = new TransactionProcessor(
    participantId,
    requestGenerator,
    domainId,
    damle,
    staticDomainParameters,
    domainCrypto,
    sequencerClient,
    inFlightSubmissionTracker,
    ephemeral,
    metrics.transactionProcessing,
    timeouts,
    loggerFactory,
  )

  private val transferOutProcessor: TransferOutProcessor = new TransferOutProcessor(
    domainId,
    participantId,
    damle,
    transferCoordination,
    inFlightSubmissionTracker,
    ephemeral,
    domainCrypto,
    seedGenerator,
    sequencerClient,
    timeouts,
    SourceProtocolVersion(staticDomainParameters.protocolVersion),
    loggerFactory,
  )

  private val transferInProcessor: TransferInProcessor = new TransferInProcessor(
    domainId,
    participantId,
    damle,
    transferCoordination,
    inFlightSubmissionTracker,
    ephemeral,
    domainCrypto,
    seedGenerator,
    sequencerClient,
    parameters.enableCausalityTracking,
    timeouts,
    TargetProtocolVersion(staticDomainParameters.protocolVersion),
    loggerFactory,
  )

  private val sortedReconciliationIntervalsProvider = SortedReconciliationIntervalsProvider(
    staticDomainParameters,
    topologyClient,
    futureSupervisor,
    loggerFactory,
  )

  private val pruneObserver = new PruneObserver(
    persistent.requestJournalStore,
    persistent.sequencerCounterTrackerStore,
    sortedReconciliationIntervalsProvider,
    persistent.acsCommitmentStore,
    persistent.activeContractStore,
    persistent.contractKeyJournal,
    participantNodePersistentState.inFlightSubmissionStore,
    domainId,
    parameters.stores.acsPruningInterval,
    clock,
    timeouts,
    loggerFactory,
  )

  private val acsCommitmentProcessor = {
    val listener = new AcsCommitmentProcessor(
      domainId,
      participantId,
      sequencerClient,
      domainCrypto,
      sortedReconciliationIntervalsProvider,
      persistent.acsCommitmentStore,
      pruneObserver.observer(_, _),
      killSwitch = selfKillSwitch,
      pruningMetrics,
      staticDomainParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )
    ephemeral.recordOrderPublisher.setAcsChangeListener(listener)
    listener
  }
  val topologyProcessor = new TopologyTransactionProcessor(
    domainId,
    domainCrypto.pureCrypto,
    domainHandle.topologyStore,
    clock,
    acsCommitmentProcessor.scheduleTopologyTick,
    futureSupervisor,
    parameters.processingTimeouts,
    loggerFactory,
  )
  // connect domain client to processor
  topologyProcessor.subscribe(domainHandle.topologyClient)
  // subscribe party notifier to topology processor
  topologyProcessor.subscribe(partyNotifier.attachToTopologyProcessor())
  // turn on missing key alerter such that we get notified if a key is used that we do not have
  private val missingKeysAlerter = new MissingKeysAlerter(
    participantId,
    domainId,
    topologyClient,
    topologyProcessor,
    domainCrypto.crypto.cryptoPrivateStore,
    loggerFactory,
  )

  private val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor =
    new BadRootHashMessagesRequestProcessor(
      ephemeral,
      domainCrypto,
      sequencerClient,
      participantId,
      staticDomainParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )

  private val repairProcessor: RepairProcessor =
    new RepairProcessor(
      ephemeral.requestCounterAllocator,
      ephemeral.phase37Synchronizer,
      loggerFactory,
    )

  private val registerIdentityTransactionHandle =
    new SequencerBasedRegisterTopologyTransactionHandle(
      (traceContext, env) =>
        domainHandle.sequencerClient.sendAsync(
          Batch(List(env), staticDomainParameters.protocolVersion)
        )(traceContext),
      domainId,
      participantId,
      participantId,
      staticDomainParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )

  private val messageDispatcher: MessageDispatcher =
    messageDispatcherFactory.create(
      domainId,
      participantId,
      ephemeral.requestTracker,
      transactionProcessor,
      transferOutProcessor,
      transferInProcessor,
      registerIdentityTransactionHandle.processor,
      ephemeral.causalityLookup,
      topologyProcessor,
      acsCommitmentProcessor.processBatch,
      ephemeral.requestCounterAllocator,
      ephemeral.recordOrderPublisher,
      badRootHashMessagesRequestProcessor,
      repairProcessor,
      inFlightSubmissionTracker,
      loggerFactory,
    )

  private def initialize(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainInitializationError, Unit] = {
    def liftF[A](f: Future[A]): EitherT[Future, SyncDomainInitializationError, A] = EitherT.liftF(f)

    def withMetadataSeq(cids: Seq[LfContractId]): Future[Seq[StoredContract]] =
      persistent.contractStore
        .lookupManyUncached(cids)
        .map(_.zip(cids).map {
          case (None, cid) =>
            val errMsg = s"Contract $cid is in active contract store but not in the contract store"
            ErrorUtil.internalError(new IllegalStateException(errMsg))
          case (Some(sc), _) => sc
        })

    def lookupChangeMetadata(change: ActiveContractIdsChange): Future[AcsChange] = {
      for {
        // TODO(i9270) extract magic numbers
        storedActivatedContracts <- MonadUtil.batchedSequentialTraverse(
          parallelism = 20,
          chunkSize = 500,
        )(change.activations.toSeq)(withMetadataSeq)
        storedDeactivatedContracts <- MonadUtil
          .batchedSequentialTraverse(parallelism = 20, chunkSize = 500)(change.deactivations.toSeq)(
            withMetadataSeq
          )
      } yield {
        AcsChange(
          activations = storedActivatedContracts
            .map(c =>
              c.contractId -> WithContractHash.fromContract(c.contract, c.contract.metadata)
            )
            .toMap,
          deactivations = storedDeactivatedContracts
            .map(c =>
              c.contractId -> WithContractHash
                .fromContract(c.contract, c.contract.metadata.stakeholders)
            )
            .toMap,
        )
      }
    }

    // pre-inform the ACS commitment processor about upcoming topology changes.
    // as we future date the topology changes, they happen at an effective time
    // and not necessarily at a timestamp triggered by a sequencer message
    // therefore, we need to pre-register them with the acs commitment processor
    // who will then consume them once a tick with a higher timestamp is observed.
    // on a restart, we can just load the relevant effective times from the database
    def loadPendingEffectiveTimesFromTopologyStore(
        timestamp: CantonTimestamp
    ): EitherT[Future, SyncDomainInitializationError, Unit] = {
      val store = domainHandle.topologyStore
      EitherT.right(store.findUpcomingEffectiveChanges(timestamp).map { changes =>
        changes.headOption.foreach { head =>
          logger.debug(
            s"Initialising the acs commitment processor with ${changes.length} effective times starting from: ${head.effective}"
          )
          acsCommitmentProcessor.initializeTicksOnStartup(changes.map(_.effective.value).toList)
        }
      })
    }

    def replayAcsChanges(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SyncDomainInitializationError, LazyList[(RecordTime, AcsChange)]] = {
      liftF(for {
        contractIdChanges <- persistent.activeContractStore
          .changesBetween(fromExclusive, toInclusive)
        changes <- contractIdChanges.parTraverse { case (toc, change) =>
          lookupChangeMetadata(change).map(ch => (RecordTime.fromTimeOfChange(toc), ch))
        }
      } yield {
        logger.info(
          s"Replaying ${changes.size} ACS changes between $fromExclusive (exclusive) and $toInclusive to the commitment processor"
        )
        changes
      })
    }

    def initializeClientAtCleanHead(): Future[Unit] = {
      // generally, the topology client will be initialised by the topology processor. however,
      // if there is nothing to be replayed, then the topology processor will only be initialised
      // once the first event is dispatched.
      // however, this is bad for transfer processing as we need to be able to access the topology state
      // across domains and this requires that the clients are separately initialised on the participants
      val resubscriptionTs =
        ephemeral.startingPoints.rewoundSequencerCounterPrehead.fold(CantonTimestamp.MinValue)(
          _.timestamp
        )
      logger.debug(s"Initialising topology client at clean head=$resubscriptionTs")
      // startup with the resubscription-ts
      topologyClient.updateHead(
        EffectiveTime(resubscriptionTs),
        ApproximateTime(resubscriptionTs),
        potentialTopologyChange = true,
      )
      // now, compute epsilon at resubscriptionTs
      topologyClient
        .awaitSnapshot(resubscriptionTs)
        .flatMap(
          _.findDynamicDomainParametersOrDefault(
            staticDomainParameters.protocolVersion,
            warnOnUsingDefault = false,
          )
        )
        .map(_.topologyChangeDelay)
        .map { topologyChangeDelay =>
          // update client
          topologyClient.updateHead(
            EffectiveTime(resubscriptionTs.plus(topologyChangeDelay.duration)),
            ApproximateTime(resubscriptionTs),
            potentialTopologyChange = true,
          )
        }
    }

    val startingPoints = ephemeral.startingPoints
    val cleanHeadRc = startingPoints.processing.nextRequestCounter
    val cleanHeadPrets = startingPoints.processing.prenextTimestamp

    for {
      // Prepare missing key alerter
      _ <- EitherT.right(missingKeysAlerter.init())

      // Phase 0: Initialise topology client at current clean head
      _ <- EitherT.right(initializeClientAtCleanHead())

      // Phase 1: publish pending events of the event log up to the prehead clean request
      // Later events may have already been published before the crash
      // if someone rewound the clean request prehead (e.g., for testing), so this is only a lower bound!
      _ = logger.info(s"Publishing pending events up to ${cleanHeadRc - 1L}")
      lastLocalOffset <- EitherT.right(
        participantNodePersistentState.multiDomainEventLog.lastLocalOffset(persistent.eventLog.id)
      )

      _unit <- EitherT.right(
        persistent.causalDependencyStore.initialize(
          lastLocalOffset.map(lo =>
            RequestCounter(lo.min(cleanHeadRc.asLocalOffset - 1))
          ) // TODO(#10497) is this conversion fine?
        )
      )

      // Phase 1: remove in-flight submissions that have been sequenced and published,
      // but not yet removed from the in-flight submission store
      //
      // Remove and complete all in-flight submissions that have been published at the multi-domain event log.
      _ <- EitherT.right(
        inFlightSubmissionTracker.recoverDomain(
          domainId,
          startingPoints.processing.prenextTimestamp,
        )
      )

      // Phase 2: recover events that have been published to the single-dimension event log, but were not published at
      // the multi-domain event log before the crash
      pending <- EitherT.right(
        participantNodePersistentState.multiDomainEventLog
          .fetchUnpublished(persistent.eventLog.id, Some(cleanHeadRc.asLocalOffset - 1L))
      )

      _unit = ephemeral.recordOrderPublisher.scheduleRecoveries(pending)

      // Phase 3: Initialize the repair processor
      repairs <- EitherT.right[SyncDomainInitializationError](
        persistent.requestJournalStore.repairRequests(
          ephemeral.startingPoints.cleanReplay.nextRequestCounter
        )
      )
      _ = logger.info(
        show"Found ${repairs.size} repair requests at request counters ${repairs.map(_.rc)}"
      )
      _ = repairProcessor.setRemainingRepairRequests(repairs)

      // Phase 4: publish ACS changes from some suitable point up to clean head timestamp to the commitment processor.
      // The "suitable point" must ensure that the [[com.digitalasset.canton.participant.store.AcsSnapshotStore]]
      // receives any partially-applied changes; choosing the timestamp returned by the store is sufficient and optimal
      // in terms of performance, but any earlier timestamp is also correct
      acsChangesReplayStartRt <- liftF(persistent.acsCommitmentStore.runningCommitments.watermark)
      _ <- loadPendingEffectiveTimesFromTopologyStore(acsChangesReplayStartRt.timestamp)
      acsChangesToReplay <-
        if (
          cleanHeadPrets >= acsChangesReplayStartRt.timestamp && cleanHeadRc > RequestCounter.Genesis
        ) {
          logger.info(
            s"Looking for ACS changes to replay between ${acsChangesReplayStartRt.timestamp} and $cleanHeadPrets"
          )
          replayAcsChanges(
            acsChangesReplayStartRt.toTimeOfChange,
            TimeOfChange(cleanHeadRc, cleanHeadPrets),
          )
        } else EitherT.pure[Future, SyncDomainInitializationError](Seq.empty)
      _ = acsChangesToReplay.foreach { case (toc, change) =>
        acsCommitmentProcessor.publish(toc, change)
      }
    } yield ()
  }

  protected def startAsync(): Future[Either[SyncDomainInitializationError, Unit]] = {
    implicit val initializationTraceContext: TraceContext = TraceContext.empty

    val delayLogger =
      new DelayLogger(
        clock,
        logger,
        parameters.delayLoggingThreshold,
        metrics.sequencerClient.delay,
      )

    def firstUnpersistedEventScF: Future[SequencerCounter] =
      persistent.sequencedEventStore
        .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))(initializationTraceContext)
        .fold(_ => SequencerCounter.Genesis, _.counter + 1)

    val sequencerCounterPreheadTsO =
      ephemeral.startingPoints.rewoundSequencerCounterPrehead.map(_.timestamp)
    val subscriptionPriorTs = {
      val cleanReplayTs = ephemeral.startingPoints.cleanReplay.prenextTimestamp
      val sequencerCounterPreheadTs = sequencerCounterPreheadTsO.getOrElse(CantonTimestamp.MinValue)
      Ordering[CantonTimestamp].min(cleanReplayTs, sequencerCounterPreheadTs)
    }

    def waitForParticipantToBeInTopology(
        initializationTraceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SyncDomainInitializationError, Unit] =
      EitherT(
        domainHandle.topologyClient
          .await(_.isParticipantActive(participantId), timeouts.verifyActive.duration)(
            initializationTraceContext
          )
          .map(isActive =>
            if (isActive) Right(())
            else
              Left(
                ParticipantDidNotBecomeActive(
                  s"Participant did not become active after ${timeouts.verifyActive.duration}"
                )
              )
          )
      )

    // Initialize, replay and process stored events, then subscribe to new events
    (for {
      _ <- initialize(initializationTraceContext)
      firstUnpersistedEventSc <- EitherT.liftF(firstUnpersistedEventScF)
      monitor = new SyncDomain.EventProcessingMonitor(
        ephemeral.startingPoints,
        firstUnpersistedEventSc,
        delayLogger,
        loggerFactory,
      )
      messageHandler =
        new ApplicationHandler[
          Lambda[`+X <: Envelope[_]` => Traced[Seq[PossiblyIgnoredSequencedEvent[X]]]],
          DefaultOpenEnvelope,
        ] {
          override def name: String = s"sync-domain-$domainId"

          override def subscriptionStartsAt(
              start: SubscriptionStart
          )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
            topologyProcessor.subscriptionStartsAt(start)(traceContext)

          override def apply(events: Traced[Seq[PossiblyIgnoredProtocolEvent]]): HandlerResult =
            messageDispatcher.handleAll(events)
        }
      eventHandler = monitor(
        EnvelopeOpener(staticDomainParameters.protocolVersion, domainCrypto.crypto.pureCrypto)(
          messageHandler
        )
      )

      cleanSequencerCounterTracker = new CleanSequencerCounterTracker(
        persistent.sequencerCounterTrackerStore,
        notifyInFlightSubmissionTracker,
        loggerFactory,
      )
      trackingHandler = cleanSequencerCounterTracker(eventHandler)
      _ <- EitherT.right[SyncDomainInitializationError](
        sequencerClient.subscribeAfter(
          subscriptionPriorTs,
          sequencerCounterPreheadTsO,
          trackingHandler,
          ephemeral.timeTracker,
          PeriodicAcknowledgements.fetchCleanCounterFromStore(
            persistent.sequencerCounterTrackerStore
          ),
        )(initializationTraceContext)
      )

      // wait for initial topology transactions to be sequenced and received before we start computing pending
      // topology transactions to push for IDM approval
      _ <- waitForParticipantToBeInTopology(initializationTraceContext).onShutdown(Right(()))
      _ <-
        identityPusher
          .domainConnected(
            domainHandle.domainAlias,
            domainId,
            staticDomainParameters.protocolVersion,
            registerIdentityTransactionHandle,
            domainHandle.topologyClient,
            domainHandle.topologyStore,
            domainCrypto.crypto,
          )(initializationTraceContext)
          .onShutdown(Right(()))
          .leftMap[SyncDomainInitializationError](ParticipantTopologyHandshakeError)

    } yield {
      logger.debug(s"Started sync domain for $domainId")(initializationTraceContext)
      ephemeral.markAsRecovered()
      logger.debug("Sync domain is ready.")(initializationTraceContext)
      FutureUtil.doNotAwait(
        completeTxIn,
        "Failed to complete outstanding transfer-ins on startup. " +
          "You may have to complete the transfer-ins manually.",
      )
      ()
    }).value
  }

  def completeTxIn(implicit tc: TraceContext): Future[Unit] = {

    val fetchLimit = 1000

    def completeTransfers(
        previous: Option[(CantonTimestamp, DomainId)]
    ): Future[Either[Option[(CantonTimestamp, DomainId)], Unit]] = {
      logger.debug(s"Fetch $fetchLimit pending transfers")
      val resF = for {
        pendingTransfers <- persistent.transferStore.findAfter(
          requestAfter = previous,
          limit = fetchLimit,
        )
        // TODO(i9500): Here, transfer-ins are completed sequentially. Consider running several in parallel to speed
        // this up. It may be helpful to use the `RateLimiter`
        eithers <- MonadUtil
          .sequentialTraverse(pendingTransfers) { data =>
            logger.debug(s"Complete ${data.transferId} after startup")
            val eitherF = TransferOutProcessingSteps.autoTransferIn(
              data.transferId,
              domainId,
              transferCoordination,
              data.contract.metadata.stakeholders,
              participantId,
              data.transferOutRequest.targetTimeProof.timestamp,
            )
            eitherF.value.map(_.left.map(err => data.transferId -> err))
          }

      } yield {
        // Log any errors, then discard the errors and continue to complete pending transfers
        eithers.foreach({
          case Left((transferId, error)) =>
            logger.debug(s"Failed to complete pending transfer $transferId. The error was $error.")
          case Right(()) => ()
        })

        pendingTransfers.lastOption.map(t => t.transferId.requestTimestamp -> t.sourceDomain)
      }

      resF.map {
        // Continue completing transfers that are after the last completed transfer
        case Some(value) => Left(Some(value))
        // We didn't find any uncompleted transfers, so stop
        case None => Right(())
      }
    }

    logger.debug(s"Wait for replay to complete")
    for {
      // Wait to see a timestamp >= now from the domain -- when we see such a timestamp, it means that the participant
      // has "caught up" on messages from the domain (and so should have seen all the transfer-ins)
      // TODO(i9009): This assumes the participant and domain clocks are synchronized, which may not be the case
      waitForReplay <- timeTracker
        .awaitTick(clock.now)
        .map(_.void)
        .getOrElse(Future.unit)

      params <- topologyClient.currentSnapshotApproximation.findDynamicDomainParametersOrDefault(
        staticDomainParameters.protocolVersion
      )

      _bool <- Monad[Future].tailRecM(None: Option[(CantonTimestamp, DomainId)])(ts =>
        completeTransfers(ts)
      )
    } yield {
      logger.debug(s"Transfer in completion has finished")
    }

  }

  /** A [[SyncDomain]] is ready when it has resubscribed to the sequencer client. */
  def ready: Boolean = ephemeral.recovered

  def readyForSubmission: Boolean = ready && !isDegraded && sequencerClient.subscriptionIsHealthy

  /** @return The outer future completes after the submission has been registered as in-flight.
    *         The inner future completes after the submission has been sequenced or if it will never be sequenced.
    */
  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionSubmissionError, Future[TransactionSubmitted]] =
    performUnlessClosingEitherTF[TransactionSubmissionError, TransactionSubmitted](
      functionFullName,
      SubmissionDuringShutdown.Rejection(),
    ) {
      ErrorUtil.requireState(ready, "Cannot submit transaction before recovery")
      transactionProcessor
        .submit(submitterInfo, transactionMeta, keyResolver, transaction)
        .onShutdown(Left(SubmissionDuringShutdown.Rejection()))
    }

  def submitTransferOut(
      submittingParty: LfPartyId,
      contractId: LfContractId,
      targetDomain: DomainId,
      targetProtocolVersion: TargetProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, TransferOutProcessingSteps.SubmissionResult] =
    performUnlessClosingEitherT[
      TransferProcessorError,
      TransferOutProcessingSteps.SubmissionResult,
    ](functionFullName, DomainNotReady(domainId, "The domain is shutting down.")) {
      if (!ready)
        DomainNotReady(domainId, "Cannot submit transfer-out before recovery").discard
      transferOutProcessor
        .submit(
          TransferOutProcessingSteps
            .SubmissionParam(submittingParty, contractId, targetDomain, targetProtocolVersion)
        )
        .onShutdown(Left(DomainNotReady(domainId, "The domain is shutting down")))
        .semiflatMap(Predef.identity)
    }

  def submitTransferIn(
      submittingParty: LfPartyId,
      transferId: TransferId,
      sourceProtocolVersion: SourceProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, TransferInProcessingSteps.SubmissionResult] =
    performUnlessClosingEitherT[TransferProcessorError, TransferInProcessingSteps.SubmissionResult](
      functionFullName,
      DomainNotReady(domainId, "The domain is shutting down."),
    ) {

      if (!ready)
        DomainNotReady(domainId, "Cannot submit transfer-out before recovery").discard
      transferInProcessor
        .submit(
          TransferInProcessingSteps
            .SubmissionParam(submittingParty, transferId, sourceProtocolVersion)
        )
        .onShutdown(Left(DomainNotReady(domainId, "The domain is shutting down")))
        .semiflatMap(Predef.identity)
    }

  def numberOfDirtyRequests(): Int = ephemeral.requestJournal.numberOfDirtyRequests

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    // As the commitment and protocol processors use the sequencer client to send messages, close
    // them before closing the domainHandle. Both of them will ignore the requests from the message dispatcher
    // after they get closed.
    Seq(
      SyncCloseable(
        "sync-domain",
        Lifecycle.close(
          // Close the domain crypto client first to stop waiting for snapshots that may block the sequencer subscription
          domainCrypto,
          // Close the sequencer subscription so that the processors won't receive or handle events when
          // their shutdown is initiated.
          () => domainHandle.sequencerClient.closeSubscription(),
          pruneObserver,
          acsCommitmentProcessor,
          transactionProcessor,
          transferOutProcessor,
          transferInProcessor,
          badRootHashMessagesRequestProcessor,
          topologyProcessor,
          ephemeral.timeTracker, // need to close time tracker before domain handle, as it might otherwise send messages
          domainHandle,
          ephemeral,
        )(logger),
      )
    )

  override def toString: String = s"SyncDomain(domain=$domainId, participant=$participantId)"

  private def notifyInFlightSubmissionTracker(
      tracedCleanSequencerCounterPrehead: Traced[SequencerCounterCursorPrehead]
  ): Future[Unit] =
    tracedCleanSequencerCounterPrehead.withTraceContext {
      implicit traceContext => cleanSequencerCounterPrehead =>
        val observedTime = cleanSequencerCounterPrehead.timestamp
        inFlightSubmissionTracker.timelyReject(domainId, observedTime).valueOr {
          case InFlightSubmissionTracker.UnknownDomain(domainId) =>
            // The CantonSyncService removes the SyncDomain from the connected domains map
            // before the SyncDomain is closed. So guarding the timely rejections against the SyncDomain being closed
            // cannot eliminate this possibility.
            //
            // It is safe to skip the timely rejects because crash recovery and replay will take care
            // upon the next reconnection.
            logger.info(
              s"Skipping timely rejects for domain $domainId upto $observedTime because domain is being disconnected."
            )
        }
    }
}

object SyncDomain {

  private class EventProcessingMonitor(
      startingPoints: ProcessingStartingPoints,
      firstUnpersistedSc: SequencerCounter,
      delayLogger: DelayLogger,
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    def apply[Env <: Envelope[_]](
        handler: PossiblyIgnoredApplicationHandler[Env]
    ): PossiblyIgnoredApplicationHandler[Env] = handler.replace { tracedBatch =>
      tracedBatch.withTraceContext { implicit batchTraceContext => tracedEvents =>
        tracedEvents.lastOption.fold(HandlerResult.done) { lastEvent =>
          if (lastEvent.counter >= firstUnpersistedSc) {
            delayLogger.checkForDelay(lastEvent)
          }
          val firstEvent = tracedEvents.headOption.getOrElse(
            throw new RuntimeException("A sequence with a last element must also have a head")
          )

          def batchIncludesCounter(counter: SequencerCounter): Boolean =
            firstEvent.counter <= counter && lastEvent.counter >= counter

          if (batchIncludesCounter(startingPoints.cleanReplay.nextSequencerCounter)) {
            logger.info(
              s"Replaying requests ${startingPoints.cleanReplay.nextRequestCounter} up to clean prehead ${startingPoints.processing.nextRequestCounter - 1L}"
            )
          }
          if (batchIncludesCounter(startingPoints.processing.nextSequencerCounter)) {
            logger.info(
              s"Replaying or processing locally stored events with sequencer counters ${startingPoints.processing.nextSequencerCounter} to ${firstUnpersistedSc - 1L}"
            )
          }
          handler(tracedBatch)
        }
      }
    }
  }

  trait Factory[+T <: SyncDomain] {

    def create(
        domainId: DomainId,
        domainHandle: DomainHandle,
        participantId: ParticipantId,
        damle: DAMLe,
        parameters: ParticipantNodeParameters,
        participantNodePersistentState: ParticipantNodePersistentState,
        persistentState: SyncDomainPersistentState,
        ephemeralState: SyncDomainEphemeralState,
        packageService: PackageService,
        domainCrypto: DomainSyncCryptoClient,
        partyNotifier: LedgerServerPartyNotifier,
        topologyClient: DomainTopologyClientWithInit,
        identityPusher: ParticipantTopologyDispatcher,
        transferCoordination: TransferCoordination,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        clock: Clock,
        selfKillSwitch: => Unit,
        pruningMetrics: PruningMetrics,
        syncDomainMetrics: SyncDomainMetrics,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): T
  }

  object DefaultFactory extends Factory[SyncDomain] {
    override def create(
        domainId: DomainId,
        domainHandle: DomainHandle,
        participantId: ParticipantId,
        damle: DAMLe,
        parameters: ParticipantNodeParameters,
        participantNodePersistentState: ParticipantNodePersistentState,
        persistentState: SyncDomainPersistentState,
        ephemeralState: SyncDomainEphemeralState,
        packageService: PackageService,
        domainCrypto: DomainSyncCryptoClient,
        partyNotifier: LedgerServerPartyNotifier,
        topologyClient: DomainTopologyClientWithInit,
        identityPusher: ParticipantTopologyDispatcher,
        transferCoordination: TransferCoordination,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        clock: Clock,
        selfKillSwitch: => Unit,
        pruningMetrics: PruningMetrics,
        syncDomainMetrics: SyncDomainMetrics,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): SyncDomain =
      new SyncDomain(
        domainId,
        domainHandle,
        participantId,
        damle,
        parameters,
        participantNodePersistentState,
        persistentState,
        ephemeralState,
        packageService,
        domainCrypto,
        partyNotifier,
        topologyClient,
        identityPusher,
        transferCoordination,
        inFlightSubmissionTracker,
        MessageDispatcher.DefaultFactory,
        clock,
        selfKillSwitch,
        pruningMetrics,
        syncDomainMetrics,
        futureSupervisor,
        loggerFactory,
      )
  }
}

sealed trait SyncDomainInitializationError
case class SequencedEventStoreError(err: store.SequencedEventStoreError)
    extends SyncDomainInitializationError
case class ParticipantTopologyHandshakeError(err: DomainRegistryError)
    extends SyncDomainInitializationError
case class ParticipantDidNotBecomeActive(msg: String) extends SyncDomainInitializationError
