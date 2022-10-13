// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cats.data.EitherT
import cats.implicits.*
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.*
import com.daml.error.definitions.PackageServiceError
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.*
import com.daml.ledger.participant.state
import com.daml.ledger.participant.state.v2.*
import com.daml.lf.command.DisclosedContract
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.Versioned
import com.daml.logging.LoggingContext
import com.daml.nonempty.NonEmpty
import com.daml.telemetry.TelemetryContext
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, String256M}
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.SyncServiceErrorGroup
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.InjectionErrorGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError
import com.digitalasset.canton.participant.admin.{workflows, *}
import com.digitalasset.canton.participant.config.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.*
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.GlobalCausalOrderer
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerDomainState
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter
import com.digitalasset.canton.participant.protocol.submission.{
  CommandDeduplicatorImpl,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferCoordination
import com.digitalasset.canton.participant.pruning.{NoOpPruningProcessor, PruningProcessor}
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.MissingConfigForAlias
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceDomainBecamePassive,
  SyncServiceDomainDisabledUs,
  SyncServiceDomainDisconnect,
  SyncServiceFailedDomainConnection,
}
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreFactory
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import io.functionmeta.functionFullName
import io.opentelemetry.api.trace.Tracer
import org.slf4j.event.Level

import java.time.{Duration as JDuration}
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.{Failure, Right, Success}

/** The Canton-based synchronization service.
  *
  * A single Canton sync service can connect to multiple domains.
  *
  * @param participantId               The participant node id hosting this sync service.
  * @param domainRegistry              Domain registry for connecting to domains.
  * @param domainConnectionConfigStore Storage for domain connection configs
  * @param packageService              Underlying package management service.
  * @param syncCrypto                  Synchronisation crypto utility combining IPS and Crypto operations.
  * @param isActive                    Returns true of the node is the active replica
  */
class CantonSyncService(
    val participantId: ParticipantId,
    private[participant] val domainRegistry: DomainRegistry,
    private[canton] val domainConnectionConfigStore: DomainConnectionConfigStore,
    private[participant] val aliasManager: DomainAliasManager,
    participantNodePersistentState: ParticipantNodePersistentState,
    participantNodeEphemeralState: ParticipantNodeEphemeralState,
    private[canton] val syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    syncDomainPersistentStateFactory: SyncDomainPersistentStateFactory,
    private[canton] val packageService: PackageService,
    topologyStoreFactory: TopologyStoreFactory,
    domainCausalityStore: MultiDomainCausalityStore,
    topologyManager: ParticipantTopologyManager,
    identityPusher: ParticipantTopologyDispatcher,
    partyNotifier: LedgerServerPartyNotifier,
    val syncCrypto: SyncCryptoApiProvider,
    pruningProcessor: PruningProcessor,
    ledgerId: LedgerId,
    engine: Engine,
    syncDomainStateFactory: SyncDomainEphemeralStateFactory,
    clock: Clock,
    resourceManagementService: ResourceManagementService,
    parameters: ParticipantNodeParameters,
    syncDomainFactory: SyncDomain.Factory[SyncDomain],
    indexedStringStore: IndexedStringStore,
    metrics: ParticipantMetrics,
    val isActive: () => Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer, val tracer: Tracer)
    extends state.v2.WriteService
    with WriteParticipantPruningService
    with state.v2.ReadService
    with FlagCloseable
    with Spanning
    with NamedLogging {

  import ShowUtil.*

  val maxDeduplicationDuration: NonNegativeFiniteDuration =
    participantNodePersistentState.settingsStore.settings.maxDeduplicationDuration
      .getOrElse(throw new RuntimeException("Max deduplication duration is not available"))

  private val ledgerInitialConditionsInternal = LedgerInitialConditions(
    ledgerId = ledgerId,
    config = Configuration(
      generation = 1L,
      timeModel = { // To start out, defining the most "permissive" time model possible
        val min = JDuration.ofNanos(0L)
        val max = JDuration.ofDays(365L)
        LedgerTimeModel(
          avgTransactionLatency = min,
          minSkew = min,
          maxSkew = max,
        ).getOrElse(throw new RuntimeException("Static config should not fail"))
      },
      maxDeduplicationDuration = maxDeduplicationDuration.unwrap,
    ),
    initialRecordTime = LedgerSyncRecordTime.Epoch,
  )

  private val excludedPackageIds = if (parameters.excludeInfrastructureTransactions) {
    Set(workflows.PackageID.PingPong, workflows.PackageID.DarDistribution)
      .map(Ref.PackageId.assertFromString)
  } else {
    Set.empty[Ref.PackageId]
  }

  protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(ledgerInitialConditionsInternal)

  // The domains this sync service is connected to. Can change due to connect/disconnect operations.
  // This may contain domains for which recovery is still running.
  // Invariant: All domain IDs in this map have a corresponding domain alias in the alias manager
  private val connectedDomainsMap: TrieMap[DomainId, SyncDomain] =
    TrieMap.empty[DomainId, SyncDomain]

  private val partyAllocation = new PartyAllocation(
    participantId,
    participantNodeEphemeralState,
    topologyManager,
    partyNotifier,
    parameters,
    isActive,
    connectedDomainsMap,
    loggerFactory,
  )

  private case class AttemptReconnect(
      alias: DomainAlias,
      last: CantonTimestamp,
      retryDelay: Duration,
      trace: TraceContext,
  ) {
    val earliest: CantonTimestamp = last.plusMillis(retryDelay.toMillis)
  }

  // Track domains we would like to "keep on reconnecting until available"
  private val attemptReconnect: TrieMap[DomainAlias, AttemptReconnect] = TrieMap.empty
  private def resolveReconnectAttempts(alias: DomainAlias): Unit = {
    val _ = attemptReconnect.remove(alias)
  }

  // A connected domain is ready if recovery has succeeded
  private def readySyncDomainById(domainId: DomainId): Option[SyncDomain] =
    connectedDomainsMap.get(domainId).filter(_.ready)

  private def existsReadyDomain: Boolean = connectedDomainsMap.exists { case (_, sync) =>
    sync.ready
  }

  private def syncDomainForAlias(alias: DomainAlias): Option[SyncDomain] =
    aliasManager.domainIdForAlias(alias).flatMap(connectedDomainsMap.get)

  private val globalTracker =
    new GlobalCausalOrderer(
      participantId,
      connectedDomainsMap.contains,
      parameters.processingTimeouts,
      domainCausalityStore,
      loggerFactory,
    )

  private val domainRouter =
    DomainRouter(
      packageService,
      connectedDomainsMap,
      domainConnectionConfigStore,
      aliasManager,
      participantId,
      autoTransferTransaction = parameters.enablePreviewFeatures,
      parameters.processingTimeouts,
      loggerFactory,
    )(ec, TraceContext.empty)

  private val transferCoordination: TransferCoordination =
    TransferCoordination(
      parameters.transferTimeProofFreshnessProportion,
      syncDomainPersistentStateManager,
      connectedDomainsMap.get,
      syncCrypto,
      loggerFactory,
    )(ec, TraceContext.empty)

  val protocolVersionGetter: Traced[DomainId] => Future[Option[ProtocolVersion]] =
    (tracedDomainId: Traced[DomainId]) =>
      tracedDomainId.withTraceContext { implicit traceContext => domainId =>
        syncDomainPersistentStateManager.protocolVersionFor(domainId)
      }

  val transferService: TransferService = new TransferService(
    domainIdOfAlias = aliasManager.domainIdForAlias,
    submissionHandles = readySyncDomainById,
    transferLookups = domainId =>
      syncDomainPersistentStateManager.get(domainId).map(_.transferStore),
    protocolVersionFor = protocolVersionGetter,
  )

  private val commandDeduplicator = new CommandDeduplicatorImpl(
    participantNodePersistentState.commandDeduplicationStore,
    clock,
    participantNodePersistentState.multiDomainEventLog.publicationTimeLowerBound,
    loggerFactory,
  )

  private val inFlightSubmissionTracker = {
    def domainStateFor(domainId: DomainId): Option[InFlightSubmissionTrackerDomainState] = {
      connectedDomainsMap.get(domainId).map { syncDomain =>
        InFlightSubmissionTrackerDomainState
          .fromSyncDomainState(syncDomain.persistent, syncDomain.ephemeral)
      }
    }

    new InFlightSubmissionTracker(
      participantNodePersistentState.inFlightSubmissionStore,
      participantNodeEphemeralState.participantEventPublisher,
      commandDeduplicator,
      participantNodePersistentState.multiDomainEventLog,
      domainStateFor,
      timeouts,
      loggerFactory,
    )
  }

  // Setup the propagation from the MultiDomainEventLog to the InFlightSubmissionTracker
  // before we run crash recovery
  participantNodePersistentState.multiDomainEventLog.setOnPublish(
    inFlightSubmissionTracker.onPublishListener
  )

  if (isActive()) {
    TraceContext.withNewTraceContext { implicit traceContext =>
      initializeState()
    }
  }

  private val damle =
    new DAMLe(
      pkgId => traceContext => packageService.getPackage(pkgId)(traceContext),
      engine,
      loggerFactory,
    )

  private val repairService: RepairService = new RepairService(
    participantId,
    topologyStoreFactory,
    syncCrypto,
    packageService,
    damle,
    participantNodePersistentState.multiDomainEventLog,
    syncDomainPersistentStateManager,
    aliasManager,
    parameters,
    indexedStringStore,
    loggerFactory,
  )

  private val migrationService =
    new SyncDomainMigration(
      aliasManager,
      domainConnectionConfigStore,
      stateInspection,
      repairService,
      prepareDomainConnectionForMigration,
      parameters.processingTimeouts,
      loggerFactory,
    )

  // Submit a transaction (write service implementation)
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: LfSubmittedTransaction,
      _estimatedInterpretationCost: Long,
      keyResolver: LfKeyResolver,
      explicitlyDisclosedContracts: ImmArray[Versioned[DisclosedContract]],
  )(implicit
      _loggingContext: LoggingContext, // not used - contains same properties as canton named logger
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    import scala.jdk.FutureConverters.*
    implicit val traceContext: TraceContext =
      TraceContext.fromDamlTelemetryContext(telemetryContext)
    withSpan("CantonSyncService.submitTransaction") { implicit traceContext => span =>
      span.setAttribute("command_id", submitterInfo.commandId)
      logger.debug(s"Received submit-transaction ${submitterInfo.commandId} from ledger-api server")
      submitTransactionF(submitterInfo, transactionMeta, transaction, keyResolver)
    }.asJava
  }

  lazy val stateInspection = new SyncStateInspection(
    syncDomainPersistentStateManager,
    participantNodePersistentState,
    pruningProcessor,
    parameters.processingTimeouts,
    loggerFactory,
  )

  override def prune(
      pruneUpToInclusive: LedgerSyncOffset,
      submissionId: LedgerSubmissionId,
      _pruneAllDivulgedContracts: Boolean, // Canton always prunes divulged contracts ignoring this flag
  ): CompletionStage[PruningResult] =
    (withNewTrace("CantonSyncService.prune") { implicit traceContext => span =>
      span.setAttribute("submission_id", submissionId)

      pruneInternally(pruneUpToInclusive).fold(
        err => PruningResult.NotPruned(err.code.asGrpcStatus(err)),
        _ => PruningResult.ParticipantPruned,
      )
    }).asJava

  def pruneInternally(
      pruneUpToInclusive: LedgerSyncOffset
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, Unit] =
    (for {
      pruneUpToMultiDomainGlobalOffset <- EitherT
        .fromEither[Future](UpstreamOffsetConvert.toGlobalOffset(pruneUpToInclusive))
        .leftMap { message =>
          LedgerPruningOffsetNonCantonFormat(
            s"Specified offset does not convert to a canton multi domain event log global offset: ${message}"
          )
        }
      _pruned <- pruningProcessor.pruneLedgerEvents(pruneUpToMultiDomainGlobalOffset)
    } yield ()).transform {
      case Left(LedgerPruningNothingPruned(message)) =>
        logger.info(
          s"Could not locate pruning point: ${message}. Considering success for idempotency"
        )
        Right(())
      case Left(LedgerPruningOnlySupportedInEnterpriseEdition(message)) =>
        logger.warn(
          s"Canton participant pruning not supported in canton-open-source edition: ${message}"
        )
        Left(PruningServiceError.PruningNotSupportedInCommunityEdition.Error())
      case Left(err: LedgerPruningOffsetNonCantonFormat) =>
        logger.info(err.message)
        Left(PruningServiceError.NonCantonOffset.Error(err.message))
      case Left(err: LedgerPruningOffsetUnsafeToPrune) =>
        logger.info(s"Unsafe to prune: ${err.message}")
        Left(
          PruningServiceError.UnsafeToPrune.Error(
            err.cause,
            err.message,
            err.lastSafeOffset.fold("")(UpstreamOffsetConvert.fromGlobalOffset(_).toHexString),
          )
        )
      case Left(err: LedgerPruningOffsetUnsafeDomain) =>
        logger.info(s"Unsafe to prune ${err.domain}: ${err.message}")
        Left(
          PruningServiceError.UnsafeToPrune.Error(
            s"no suitable offset for domain ${err.domain}",
            err.message,
            "none",
          )
        )
      case Left(err) =>
        logger.warn(s"Internal error while pruning: $err")
        Left(PruningServiceError.InternalServerError.Error(err.message))
      case Right(_unit) => Right(())
    }

  private def submitTransactionF(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: LfSubmittedTransaction,
      keyResolver: LfKeyResolver,
  )(implicit traceContext: TraceContext): Future[SubmissionResult] = {

    val ack = SubmissionResult.Acknowledged

    def processSubmissionError(error: TransactionError): Future[SubmissionResult] = {
      error.logWithContext(
        Map("commandId" -> submitterInfo.commandId, "applicationId" -> submitterInfo.applicationId)
      )
      Future.successful(SubmissionResult.SynchronousError(error.rpcStatus()))
    }

    if (isClosing) {
      processSubmissionError(SubmissionDuringShutdown.Rejection())
    } else if (!isActive()) {
      // this is the only error we can not really return with a rejection, as this is the passive replica ...
      val err = SyncServiceInjectionError.PassiveReplica.Error(
        submitterInfo.applicationId,
        submitterInfo.commandId,
      )
      err.logWithContext(
        Map("commandId" -> submitterInfo.commandId, "applicationId" -> submitterInfo.applicationId)
      )
      Future.successful(SubmissionResult.SynchronousError(err.rpcStatus()))
    } else if (!existsReadyDomain) {
      processSubmissionError(SyncServiceInjectionError.NotConnectedToAnyDomain.Error())
    } else {
      val submittedFF = domainRouter.submitTransaction(
        submitterInfo,
        transactionMeta,
        keyResolver,
        transaction,
      )
      // TODO(i2794) retry command if token expired
      submittedFF.value.transformWith {
        case Success(Right(sequencedF)) =>
          // Reply with ACK as soon as the submission has been registered as in-flight,
          // and asynchronously send it to the sequencer.
          logger.debug(s"Command ${submitterInfo.commandId} is now in flight.")
          sequencedF.onComplete {
            case Success(_) =>
              logger.debug(s"Successfully submitted transaction ${submitterInfo.commandId}.")
            case Failure(ex) =>
              logger.error(s"Command submissision for ${submitterInfo.commandId} failed", ex)
          }
          Future.successful(ack)
        case Success(Left(submissionError)) =>
          processSubmissionError(submissionError)
        case Failure(PassiveInstanceException(_reason)) =>
          val err = SyncServiceInjectionError.PassiveReplica.Error(
            submitterInfo.applicationId,
            submitterInfo.commandId,
          )
          Future.successful(SubmissionResult.SynchronousError(err.rpcStatus()))
        case Failure(exception) =>
          val err = SyncServiceInjectionError.InjectionFailure.Failure(exception)
          err.logWithContext()
          Future.successful(SubmissionResult.SynchronousError(err.rpcStatus()))
      }
    }
  }

  override def submitConfiguration(
      _maxRecordTimeToBeRemovedUpstream: participant.LedgerSyncRecordTime,
      submissionId: LedgerSubmissionId,
      config: Configuration,
  )(implicit
      _loggingContext: LoggingContext, // not used - contains same properties as canton named logger
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    implicit val traceContext: TraceContext =
      TraceContext.fromDamlTelemetryContext(telemetryContext)
    logger.info("Canton does not support dynamic reconfiguration of time model")
    CompletableFuture.completedFuture(TransactionError.NotSupported)
  }

  /** Build source for subscription (for ledger api server indexer).
    * @param beginAfterOffset offset after which to emit events
    */
  override def stateUpdates(
      beginAfterOffset: Option[LedgerSyncOffset]
  )(implicit loggingContext: LoggingContext): Source[(LedgerSyncOffset, LedgerSyncEvent), NotUsed] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      logger.debug(s"Subscribing to stateUpdates from $beginAfterOffset")
      // Plus one since dispatchers use inclusive offsets.
      beginAfterOffset
        .traverse(after => UpstreamOffsetConvert.toGlobalOffset(after).map(_ + 1L))
        .fold(
          e => Source.failed(new IllegalArgumentException(e)),
          beginStartingAt =>
            participantNodePersistentState.multiDomainEventLog
              .subscribe(beginStartingAt)
              .map[LedgerSyncEventWithOffset] { case (offset, tracedEvent) =>
                implicit val traceContext = tracedEvent.traceContext
                val event = augmentTransactionStatistics(tracedEvent.value)
                logger.debug(show"Emitting event at offset $offset. Event: $event")
                (UpstreamOffsetConvert.fromGlobalOffset(offset), event)
              },
        )
    }

  // Augment event with transaction statistics "as late as possible" as stats are redundant data and so that
  // we don't need to persist stats and deal with versioning stats changes. Also every event is usually consumed
  // only once.
  private[sync] def augmentTransactionStatistics(event: LedgerSyncEvent): LedgerSyncEvent =
    event match {
      case ta @ LedgerSyncEvent.TransactionAccepted(
            Some(completionInfo),
            _transactionMeta,
            transaction,
            _transactionId,
            _recordTime,
            _divulgedContracts,
            _blindingInfo,
            _contractMetadata,
          ) =>
        ta.copy(optCompletionInfo =
          Some(
            completionInfo.copy(statistics =
              Some(LedgerTransactionNodeStatistics(transaction, excludedPackageIds))
            )
          )
        )
      case event => event
    }

  override def allocateParty(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit
      _loggingContext: LoggingContext, // not used - contains same properties as canton named logger
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    partyAllocation.allocate(hint, displayName, rawSubmissionId)

  override def uploadPackages(
      submissionId: LedgerSubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit
      _loggingContext: LoggingContext, // not used - contains same properties as canton named logger
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    implicit val traceContext: TraceContext =
      TraceContext.fromDamlTelemetryContext(telemetryContext)
    withSpan("CantonSyncService.uploadPackages") { implicit traceContext => span =>
      if (!isActive()) {
        logger.debug(s"Rejecting package upload on passive replica.")
        Future.successful(TransactionError.NotSupported)
      } else {
        span.setAttribute("submission_id", submissionId)
        logger.debug(
          s"Processing ledger-api package upload of ${archives.length} packages from source ${sourceDescription}"
        )
        // The API Package service has already decoded and validated the archives,
        // so we can simply store them here without revalidating them.
        val ret = for {
          sourceDescriptionLenLimit <- EitherT.fromEither[Future](
            String256M
              .create(sourceDescription.getOrElse(""), Some("package source description"))
              .leftMap(PackageServiceError.InternalError.Generic.apply)
          )
          _ <- packageService.storeValidatedPackagesAndSyncEvent(
            archives,
            sourceDescriptionLenLimit,
            submissionId,
            dar = None,
            vetAllPackages = true,
            synchronizeVetting = false,
          )
        } yield SubmissionResult.Acknowledged
        ret.valueOr(err => TransactionError.internalError(CantonError.stringFromContext(err)))
      }
    }
  }.asJava

  /** Executes ordered sequence of steps to recover any state that might have been lost if the participant previously
    * crashed. Needs to be invoked after the input stores have been created, but before they are made available to
    * dependent components.
    */
  private def recoverParticipantNodeState()(implicit traceContext: TraceContext): Unit = {

    val participantEventLogId = participantNodePersistentState.participantEventLog.id

    // Note that state from domain event logs is recovered when the participant reconnects to domains.

    val recoveryF = {
      logger.info("Recovering published timely rejections")
      // Recover the published in-flight submissions for all domain ids we know
      val domains =
        configuredDomains
          .collect { case domain if domain.status.isActive => domain.config.domain }
          .mapFilter(aliasManager.domainIdForAlias)
      for {
        _ <- inFlightSubmissionTracker.recoverPublishedTimelyRejections(domains)
        _ = logger.info("Publishing the unpublished events from the ParticipantEventLog")
        // These publications will propagate to the CommandDeduplicator and InFlightSubmissionTracker like during normal processing
        unpublished <- participantNodePersistentState.multiDomainEventLog.fetchUnpublished(
          participantEventLogId,
          None,
        )
        unpublishedEvents = unpublished.mapFilter {
          case RecordOrderPublisher.PendingTransferPublish(rc, updateS, ts, eventLogId) =>
            logger.error(
              s"Pending transfer event with rc $rc timestamp $ts found in participant event log " +
                s"$participantEventLogId. Participant event log should not contain transfers."
            )
            None
          case RecordOrderPublisher.PendingEventPublish(update, tse, ts, eventLogId) => Some(tse)
        }

        _units <- MonadUtil.sequentialTraverse(unpublishedEvents) { tse =>
          participantNodePersistentState.multiDomainEventLog.publish(
            PublicationData(participantEventLogId, tse, None)
          )
        }
      } yield {
        logger.debug(s"Participant event log recovery completed")
      }
    }

    // also resume pending party notifications
    val resumePendingF = recoveryF.flatMap { _ =>
      partyNotifier.resumePending()
    }

    parameters.processingTimeouts.unbounded.await(
      "Wait for participant event log recovery to finish"
    )(resumePendingF)
  }

  def initializeState()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Invoke crash recovery or initialize active participant")

    // Important to invoke recovery before we do anything else with persisted stores.
    recoverParticipantNodeState()

    // Starting with Daml 1.1.0, the ledger api server requires the ledgers to publish their time model as the
    // first event. Only do so on brand new ledgers without preexisting state.
    logger.debug("Publishing time model configuration event if ledger is brand new")
    parameters.processingTimeouts.default
      .await("Publish time model configuration event if ledger is brand new")(
        participantNodeEphemeralState.participantEventPublisher.publishTimeModelConfigNeededUpstreamOnlyIfFirst
      )
  }

  /** Returns the ready domains this sync service is connected to. */
  def readyDomains: Map[DomainAlias, (DomainId, Boolean)] =
    connectedDomainsMap
      .to(LazyList)
      .mapFilter {
        case (id, sync) if sync.ready =>
          aliasManager.aliasForDomainId(id).map(_ -> ((id, sync.readyForSubmission)))
        case _ => None
      }
      .toMap

  /** Returns the recovering domains this sync service is connected to.
    * "Recovering" means connected, but not yet ready to use.
    */
  def recoveringDomains: Map[DomainAlias, DomainId] =
    connectedDomainsMap
      .to(LazyList)
      .mapFilter {
        case (id, sync) if !sync.ready => aliasManager.aliasForDomainId(id).map(_ -> id)
        case _ => None
      }
      .toMap

  /** Returns the domains this sync service is configured with. */
  def configuredDomains: Seq[StoredDomainConnectionConfig] = domainConnectionConfigStore.getAll()

  /** Returns the pure crypto operations used for the sync protocol */
  def pureCryptoApi: CryptoPureApi = syncCrypto.pureCrypto

  /** Lookup a time tracker for the given `domainId`.
    * A time tracker will only be returned if the domain is registered and connected.
    */
  def lookupDomainTimeTracker(domainId: DomainId): Option[DomainTimeTracker] =
    connectedDomainsMap.get(domainId).map(_.timeTracker)

  /** Adds a new domain to the sync service's configuration.
    *
    * NOTE: Does not automatically connect the sync service to the new domain.
    *
    * @param config   The domain configuration.
    * @return Error or unit.
    */
  def addDomain(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Unit] = {
    domainConnectionConfigStore
      .put(config, DomainConnectionConfigStore.Active)
      .leftMap(e => SyncServiceError.SyncServiceAlreadyAdded.Error(e.alias))
  }

  /** Modifies the settings of the sync-service's configuration
    *
    * NOTE: This does not automatically reconnect the sync service.
    */
  def modifyDomain(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Unit] =
    domainConnectionConfigStore
      .replace(config)
      .leftMap(e => SyncServiceError.SyncServiceInternalError.MissingDomainConfig(e.alias))

  def migrateDomain(
      source: DomainAlias,
      target: DomainConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def mustBeOffline(alias: DomainAlias, domainId: DomainId) = EitherT.cond[FutureUnlessShutdown](
      !connectedDomainsMap.contains(domainId),
      (),
      SyncServiceError.SyncServiceDomainMustBeOffline.Error(alias): SyncServiceError,
    )
    for {
      targetDomainInfo <- performUnlessClosingEitherU(functionFullName)(
        DomainConnectionInfo
          .fromConfig(domainRegistry.sequencerConnectClientBuilder)(target)
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(
              target.domain,
              DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(err.message),
            )
          )
      )
      sourceDomainId <- EitherT.fromEither[FutureUnlessShutdown](
        aliasManager
          .domainIdForAlias(source)
          .toRight(
            SyncServiceError.SyncServiceUnknownDomain.Error(source): SyncServiceError
          )
      )
      _ <- mustBeOffline(source, sourceDomainId)
      _ <- mustBeOffline(target.domain, targetDomainInfo.domainId)
      _ <- EitherT(
        FutureUnlessShutdown(
          connectQueue.execute(
            migrationService
              .migrateDomain(
                source,
                target,
                targetDomainInfo.domainId,
                targetDomainInfo.parameters,
              )
              .leftMap[SyncServiceError](
                SyncServiceError.SyncServiceMigrationError(source, target.domain, _)
              )
              .value
              .unwrap,
            "migrate domain",
          )
        )
      )
    } yield ()
  }

  /** Removes a configured and disconnected domain.
    *
    * This is an unsafe operation as it changes the ledger offsets.
    */
  def purgeDomain(domain: DomainAlias): Either[SyncServiceError, Unit] =
    throw new UnsupportedOperationException("This unsafe operation has not been implemented yet")

  /** Reconnect to all configured domains that have autoStart = true */
  def reconnectDomains(
      ignoreFailures: Boolean
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Seq[DomainAlias]] =
    EitherT(
      connectQueue.execute(
        // TODO(#6175) propagate the shutdown into the queue instead of discarding it
        performReconnectDomains(ignoreFailures).value.onShutdown(Right(Seq())),
        "reconnect domains",
      )
    )

  private def performReconnectDomains(ignoreFailures: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Seq[DomainAlias]] = {

    // TODO(i2833): do this in parallel to speed up start-up once this is stable enough
    //  This will need additional synchronization in performDomainConnection
    def go(
        connected: List[DomainAlias],
        open: List[DomainAlias],
    ): EitherT[FutureUnlessShutdown, SyncServiceError, List[DomainAlias]] =
      open match {
        case Nil => EitherT.rightT(connected)
        case con :: rest =>
          for {
            succeeded <- performDomainConnection(con, startSyncDomain = false).transform {
              case Left(SyncServiceFailedDomainConnection(_, parent)) if ignoreFailures =>
                // if the error is retryable, we'll reschedule an automatic retry so this domain gets connected eventually
                if (parent.retryable.nonEmpty) {
                  logger.warn(
                    s"Skipping failing domain $con after ${parent.code.toMsg(parent.cause, traceContext.traceId)}. Will schedule subsequent retry."
                  )
                  attemptReconnect
                    .put(
                      con,
                      AttemptReconnect(
                        con,
                        clock.now,
                        parameters.sequencerClient.startupConnectionRetryDelay.toScala,
                        traceContext,
                      ),
                    )
                    .discard
                  scheduleReconnectAttempt(
                    clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.duration)
                  )
                } else {
                  logger.warn(
                    s"Skipping failing domain $con after ${parent.code.toMsg(parent.cause, traceContext.traceId)}. Will not schedule retry. Please connect it manually."
                  )
                }
                Right(false)
              case Left(err) =>
                // disconnect from pending connections on failure
                val failures = connected.mapFilter(performDomainDisconnect(_).left.toOption)
                if (failures.nonEmpty) {
                  logger.error(s"Failed to disconnect from domains: ${failures}")
                }
                Left(err)
              case Right(_) => Right(true)
            }
            res <- go(if (succeeded) connected :+ con else connected, rest)
          } yield res
      }
    def startDomains(domains: Seq[DomainAlias]): EitherT[Future, SyncServiceError, Unit] = {
      // we need to start all domains concurrently in order to avoid the transfer processing
      // to hang
      val futE = Future.traverse(domains)(domain =>
        (for {
          syncDomain <- EitherT.fromOption[Future](
            syncDomainForAlias(domain),
            SyncServiceError.SyncServiceUnknownDomain.Error(domain),
          )
          _ <- startDomain(domain, syncDomain)
        } yield ()).value.map(v => (domain, v))
      )
      EitherT(futE.map { res =>
        val failed = res.collect { case (_, Left(err)) => err }
        NonEmpty.from(failed) match {
          case None => Right(())
          case Some(lst) =>
            domains.foreach(performDomainDisconnect)
            Left(SyncServiceError.SyncServiceStartupError(lst))
        }
      })
    }
    val connectedDomains =
      connectedDomainsMap.keys.to(LazyList).mapFilter(aliasManager.aliasForDomainId).toSet
    def shouldConnectTo(config: StoredDomainConnectionConfig): Boolean = {
      config.status.isActive && !config.config.manualConnect && !connectedDomains.contains(
        config.config.domain
      )
    }
    for {
      configs <- EitherT.pure[FutureUnlessShutdown, SyncServiceError](
        domainConnectionConfigStore
          .getAll()
          .collect {
            case storedConfig if shouldConnectTo(storedConfig) => storedConfig.config.domain
          }
      )

      _ = logger.info(
        s"Reconnecting to domains ${configs.map(_.unwrap)}. Already connected: $connectedDomains"
      )
      // step connect
      connected <- go(List(), configs.toList)
      _ = if (configs.nonEmpty) {
        if (connected.nonEmpty)
          logger.info("Starting sync-domains for global reconnect of domains")
        else
          logger.info("Not starting any sync-domain as none can be contacted")
      }
      // step subscribe
      _ <- startDomains(connected).mapK(FutureUnlessShutdown.outcomeK)
    } yield {
      if (connected != configs)
        logger.info(
          s"Successfully re-connected to a subset of domains ${connected}, failed to connect to ${configs.toSet -- connected.toSet}"
        )
      else
        logger.info(s"Successfully re-connected to domains ${connected}")
      connected
    }
  }

  private def startDomain(alias: DomainAlias, syncDomain: SyncDomain)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncServiceError, Unit] =
    EitherTUtil
      .fromFuture(
        syncDomain.start(),
        t => SyncServiceError.SyncServiceInternalError.Failure(alias, t),
      )
      .subflatMap[SyncServiceError, Unit](
        _.leftMap(error => SyncServiceError.SyncServiceInternalError.InitError(alias, error))
      )

  /** Connect the sync service to the given domain.
    * This method makes sure there can only be one connection in progress at a time.
    */
  def connectDomain(domainAlias: DomainAlias, keepRetrying: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncServiceError, Boolean] =
    domainConnectionConfigByAlias(domainAlias)
      .leftMap(_ => SyncServiceError.SyncServiceUnknownDomain.Error(domainAlias))
      .flatMap { _ =>
        val initial = if (keepRetrying) {
          // we're remembering that we have been trying to reconnect here
          attemptReconnect
            .put(
              domainAlias,
              AttemptReconnect(
                domainAlias,
                clock.now,
                parameters.sequencerClient.startupConnectionRetryDelay.toScala,
                traceContext,
              ),
            )
            .isEmpty
        } else true
        attemptDomainConnection(domainAlias, keepRetrying = keepRetrying, initial = initial)
      }

  private def attemptDomainConnection(
      domainAlias: DomainAlias,
      keepRetrying: Boolean,
      initial: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Boolean] = {
    EitherT(
      // TODO(#6175) propagate the shutdown into the queue
      connectQueue.execute(
        (if (keepRetrying && !attemptReconnect.exists(_._1 == domainAlias)) {
           EitherT.rightT[FutureUnlessShutdown, SyncServiceError](false)
         } else {
           performDomainConnection(domainAlias, startSyncDomain = true).transform {
             case Left(SyncServiceError.SyncServiceFailedDomainConnection(_, err))
                 if keepRetrying && err.retryable.nonEmpty =>
               if (initial)
                 logger.warn(s"Initial connection attempt to ${domainAlias} failed with ${err.code
                     .toMsg(err.cause, traceContext.traceId)}. Will keep on trying.")
               else
                 logger.info(
                   s"Initial connection attempt to ${domainAlias} failed. Will keep on trying."
                 )
               scheduleReconnectAttempt(
                 clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.duration)
               )
               Right(false)
             case Right(()) =>
               resolveReconnectAttempts(domainAlias)
               Right(true)
             case Left(x) =>
               resolveReconnectAttempts(domainAlias)
               Left(x)
           }
         }).value.onShutdown(Right(false)),
        s"connect to $domainAlias",
      )
    )
  }

  private def scheduleReconnectAttempt(timestamp: CantonTimestamp): Unit = {
    def mergeLarger(cur: Option[CantonTimestamp], ts: CantonTimestamp): Option[CantonTimestamp] =
      cur match {
        case None => Some(ts)
        case Some(old) => Some(CantonTimestamp.max(ts, old))
      }
    val _ = clock.scheduleAt(
      ts => {
        val (reconnect, nextO) = {
          attemptReconnect.toList.foldLeft(
            (Seq.empty[AttemptReconnect], None: Option[CantonTimestamp])
          ) { case ((reconnect, next), (alias, item)) =>
            // if we can't retry now, remember to retry again
            if (item.earliest > ts)
              (reconnect, mergeLarger(next, item.earliest))
            else {
              // update when we retried
              val nextRetry = item.retryDelay.*(2.0)
              val maxRetry = parameters.sequencerClient.maxConnectionRetryDelay.toScala
              val nextRetryCapped = if (nextRetry > maxRetry) maxRetry else nextRetry
              attemptReconnect
                .put(alias, item.copy(last = ts, retryDelay = nextRetryCapped))
                .discard
              (reconnect :+ item, mergeLarger(next, ts.plusMillis(nextRetryCapped.toMillis)))
            }
          }
        }
        reconnect.foreach { item =>
          implicit val traceContext: TraceContext = item.trace
          logger.debug(s"Starting background reconnect attempt for ${item.alias}")
          EitherTUtil.doNotAwait(
            attemptDomainConnection(item.alias, keepRetrying = true, initial = false),
            s"Background reconnect to ${item.alias} failed",
          )
        }
        nextO.foreach(scheduleReconnectAttempt)
      },
      timestamp,
    )
  }

  def domainConnectionConfigByAlias(
      domainAlias: DomainAlias
  ): EitherT[Future, MissingConfigForAlias, StoredDomainConnectionConfig] =
    EitherT.fromEither[Future](domainConnectionConfigStore.get(domainAlias))

  private val connectQueue = new SimpleExecutionQueue()

  /** Connect the sync service to the given domain. */
  private def performDomainConnection(
      domainAlias: DomainAlias,
      startSyncDomain: Boolean,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def createDomainHandle(
        config: DomainConnectionConfig
    ): EitherT[FutureUnlessShutdown, SyncServiceError, DomainHandle] =
      EitherT(domainRegistry.connect(config, syncDomainPersistentStateFactory)).leftMap(err =>
        SyncServiceError.SyncServiceFailedDomainConnection(domainAlias, err)
      )

    def handleCloseDegradation(syncDomain: SyncDomain)(err: CantonError) = {
      syncDomain.degradationOccurred(err)
      disconnectDomain(domainAlias)
    }

    if (aliasManager.domainIdForAlias(domainAlias).exists(connectedDomainsMap.contains)) {
      logger.debug(s"Already connected to domain: ${domainAlias.unwrap}")
      resolveReconnectAttempts(domainAlias)
      EitherT.rightT(())
    } else {

      // Lazy val to ensure we disconnect only once, even if several parts of the system trigger the switch
      lazy val killSwitch: Unit = {
        parameters.processingTimeouts.unbounded
          .await_("disconnecting domain")(checked(tryDisconnectDomain(domainAlias)))
      }

      logger.debug(s"Connecting to domain: ${domainAlias.unwrap}")
      val domainMetrics = metrics.domainMetrics(domainAlias)

      val ret: EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = for {

        domainConnectionConfig <- domainConnectionConfigByAlias(domainAlias)
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[SyncServiceError] { case MissingConfigForAlias(alias) =>
            SyncServiceError.SyncServiceInternalError.MissingDomainConfig(alias)
          }
        // do not connect to a domain that is not active
        _ <- EitherT.cond[FutureUnlessShutdown](
          domainConnectionConfig.status.isActive || skipStatusCheck,
          (),
          SyncServiceError.SyncServiceDomainIsNotActive
            .Error(domainAlias, domainConnectionConfig.status): SyncServiceError,
        )
        domainHandle <- createDomainHandle(domainConnectionConfig.config)

        persistent = domainHandle.domainPersistentState
        domainId = domainHandle.domainId
        ephemeral <- EitherT.right[SyncServiceError](
          FutureUnlessShutdown.outcomeF(
            syncDomainStateFactory
              .createFromPersistent(
                persistent,
                participantNodePersistentState.multiDomainEventLog,
                globalTracker,
                inFlightSubmissionTracker,
                (loggerFactory: NamedLoggerFactory) =>
                  DomainTimeTracker(
                    domainConnectionConfig.config.timeTracker,
                    clock,
                    domainHandle.sequencerClient,
                    loggerFactory,
                  ),
                domainMetrics,
                participantId,
              )
          )
        )
        domainLoggerFactory = loggerFactory.append("domain", domainAlias.unwrap)

        syncDomain = syncDomainFactory.create(
          domainId,
          domainHandle,
          participantId,
          damle,
          parameters,
          participantNodePersistentState,
          persistent,
          ephemeral,
          packageService,
          syncCrypto.tryForDomain(domainId, Some(domainAlias)),
          partyNotifier,
          domainHandle.topologyClient,
          identityPusher,
          transferCoordination,
          inFlightSubmissionTracker,
          clock,
          killSwitch,
          metrics.pruning,
          domainMetrics,
          futureSupervisor,
          domainLoggerFactory,
        )

        // update list of connected domains
        _ = connectedDomainsMap += (domainId -> syncDomain)

        _ = syncDomain.resolveDegradationIfExists(_ => s"reconnected to domain $domainAlias")

        // Start sequencer client subscription only after sync domain has been added to connectedDomainsMap, e.g. to
        // prevent sending PartyAddedToParticipantEvents before the domain is available for command submission. (#2279)
        _ <-
          if (startSyncDomain) {
            logger.info(s"Connected to domain and starting synchronisation: $domainAlias")
            startDomain(domainAlias, syncDomain).mapK(FutureUnlessShutdown.outcomeK)
          } else {
            logger.info(s"Connected to domain: $domainAlias, without starting synchronisation")
            EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())
          }
        _ = domainHandle.sequencerClient.completion.onComplete {
          case Success(denied: CloseReason.PermissionDenied) =>
            handleCloseDegradation(syncDomain)(
              SyncServiceDomainDisabledUs.Error(domainAlias, denied.cause)
            )
          case Success(CloseReason.BecamePassive) =>
            handleCloseDegradation(syncDomain)(
              SyncServiceDomainBecamePassive.Error(domainAlias)
            )
          case Success(error: CloseReason.UnrecoverableError) =>
            if (isClosing)
              disconnectDomain(domainAlias)
            else
              handleCloseDegradation(syncDomain)(
                SyncServiceDomainDisconnect.UnrecoverableError(domainAlias, error.cause)
              )
          case Success(error: CloseReason.UnrecoverableException) =>
            handleCloseDegradation(syncDomain)(
              SyncServiceDomainDisconnect.UnrecoverableException(domainAlias, error.throwable)
            )
          case Success(CloseReason.ClientShutdown) =>
            logger.info(s"$domainAlias disconnected because sequencer client was closed")
            disconnectDomain(domainAlias)
          case Failure(exception) =>
            handleCloseDegradation(syncDomain)(
              SyncServiceDomainDisconnect.UnrecoverableException(domainAlias, exception)
            )
        }
      } yield {
        // remove this one from the reconnect attempt list, as we are successfully connected now
        this.resolveReconnectAttempts(domainAlias)
      }

      def disconnectOn(cause: String): Unit = {
        // only invoke domain disconnect if we actually got so far that the domain-id has been read from the remote node
        if (aliasManager.domainIdForAlias(domainAlias).nonEmpty)
          performDomainDisconnect(
            domainAlias
          ).discard // Ignore Lefts because we don't know to what extent the connection succeeded.
      }

      def handleOutcome(
          outcome: UnlessShutdown[Either[SyncServiceError, Unit]]
      ): UnlessShutdown[Either[SyncServiceError, Unit]] =
        outcome match {
          case x @ UnlessShutdown.Outcome(Right(())) => x
          case UnlessShutdown.AbortedDueToShutdown =>
            disconnectOn("shutdown")
            UnlessShutdown.AbortedDueToShutdown
          case x @ UnlessShutdown.Outcome(
                Left(SyncServiceError.SyncServiceAlreadyAdded.Error(_))
              ) =>
            x
          case x @ UnlessShutdown.Outcome(Left(_)) =>
            disconnectOn("failed connect")
            x
        }

      EitherT(
        ret.value.transform(
          handleOutcome,
          err => {
            logger.error(
              s"performing domain connection for ${domainAlias.unwrap} failed with an unhandled error",
              err,
            )
            err
          },
        )
      )
    }
  }

  /** Disconnect the given domain from the sync service. */
  def disconnectDomain(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Unit] = {
    resolveReconnectAttempts(domain)
    connectQueue.executeE(
      EitherT.fromEither(performDomainDisconnect(domain)),
      s"disconnect from $domain",
    )
  }

  private def performDomainDisconnect(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Either[SyncServiceError, Unit] = {
    logger.info(show"Disconnecting from $domain")
    (for {
      domainId <- aliasManager.domainIdForAlias(domain)
    } yield {
      connectedDomainsMap.remove(domainId) match {
        case Some(syncDomain) =>
          syncDomain.close()
          logger.info(show"Disconnected from $domain")
        case None =>
          logger.info(show"Nothing to do, as we are not connected to $domain")
      }
    }).toRight(SyncServiceError.SyncServiceUnknownDomain.Error(domain))
  }

  /** Disconnect knowing that the alias exists */
  private def tryDisconnectDomain(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Future[Unit] = {
    disconnectDomain(domain).value.map(
      _.valueOr(err => throw new RuntimeException(s"Error while disconnecting domain: $err"))
    )
  }

  /** Disconnect from all connected domains. */
  def disconnectDomains()(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncServiceError, Unit] = {
    val connectedDomains =
      connectedDomainsMap.keys.toList.mapFilter(aliasManager.aliasForDomainId).distinct
    connectedDomains.traverse_(disconnectDomain)
  }

  /** Checks if a given party has any active contracts. */
  def partyHasActiveContracts(
      partyId: PartyId
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    val stateInspection = new SyncStateInspection(
      syncDomainPersistentStateManager,
      participantNodePersistentState,
      pruningProcessor,
      parameters.processingTimeouts,
      loggerFactory,
    )

    // checks active contracts for all stores of connected domains
    syncDomainPersistentStateManager.getAll.toList
      .findM { case (_, store) =>
        partyHasActiveContractsInDomain(store, partyId, stateInspection)
      }
      .map(_.nonEmpty)
  }

  /** Checks if a given party has any active contracts for a given domain. */
  private def partyHasActiveContractsInDomain(
      domainStore: SyncDomainPersistentState,
      partyId: PartyId,
      stateInspection: SyncStateInspection,
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    for {
      acs <- stateInspection.currentAcsSnapshot(domainStore)
      res <- acs match {
        case Right(x) =>
          domainStore.contractStore
            .hasActiveContracts(
              partyId,
              x.keys.toVector,
            )
        case Left(err) =>
          logger.error(
            s"Error fetching current acs snapshot: $err."
          )
          Future.successful(false)
      }
    } yield res
  }

  /** Participant repair utility for manually adding contracts to a domain in an offline fashion.
    *
    * @param domain             alias of domain to add contracts to. The domain needs to be configured, but disconnected
    *                           to prevent race conditions.
    * @param contractsToAdd     contracts to add. Relevant pieces of each contract: create-arguments (LfContractInst),
    *                           template-id (LfContractInst), contractId, ledgerCreateTime, salt (to be added to
    *                           SerializableContract), and witnesses, SerializableContract.metadata is only validated,
    *                           but otherwise ignored as stakeholder and signatories can be recomputed from contracts.
    * @param ignoreAlreadyAdded whether to ignore and skip over contracts already added/present in the domain. Setting
    *                           this to true (at least on retries) enables writing idempotent repair scripts.
    */
  def addContractsRepair(
      domain: DomainAlias,
      contractsToAdd: Seq[SerializableContractWithWitnesses],
      ignoreAlreadyAdded: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    for {
      // Ensure domain is configured but not connected to avoid race conditions.
      domainId <- aliasManager.domainIdForAlias(domain).toRight(s"Could not find $domain")
      _ <- Either.cond(
        !connectedDomainsMap.contains(domainId),
        (),
        s"Participant is still connected to $domain",
      )
      _ <- repairService.addContracts(domainId, contractsToAdd, ignoreAlreadyAdded)
    } yield ()
  }

  /** Participant repair utility for manually purging (archiving) contracts in an offline fashion.
    *
    * @param domain              alias of domain to purge contracts from. The domain needs to be configured, but
    *                            disconnected to prevent race conditions.
    * @param contractIds         lf contract ids of contracts to purge
    * @param ignoreAlreadyPurged whether to ignore already purged contracts.
    */
  def purgeContractsRepair(
      domain: DomainAlias,
      contractIds: Seq[LfContractId],
      ignoreAlreadyPurged: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    for {
      // Ensure domain is configured but not connected to avoid race conditions.
      domainId <- aliasManager
        .domainIdForAlias(domain)
        .toRight(s"Could not find domain ${domain.unwrap}")
      _ <- Either.cond(
        !connectedDomainsMap.contains(domainId),
        (),
        s"Participant is still connected to $domain",
      )
      _ <- repairService.purgeContracts(domainId, contractIds, ignoreAlreadyPurged)
    } yield ()

  /** Participant repair utility for manually moving contracts from a source domain to a target domain in an offline
    * fashion.
    *
    * @param contractIds  ids of contracts to move that reside in the sourceDomain (or for idempotency already in the
    *                     targetDomain)
    * @param sourceDomain alias of source domain from which to move contracts
    * @param targetDomain alias of target domain to which to move contracts
    * @param skipInactive whether to only move contracts that are active in the source domain
    * @param batchSize    how many contracts to write at once
    */
  def changeDomainRepair(
      contractIds: Seq[LfContractId],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit tranceContext: TraceContext): Either[String, Unit] = {
    for {
      // Ensure both domains are configured but not connected to avoid race conditions.
      sourceDomainId <- aliasManager
        .domainIdForAlias(sourceDomain)
        .toRight(s"Could not find source domain $sourceDomain")
      targetDomainId <- aliasManager
        .domainIdForAlias(targetDomain)
        .toRight(s"Could not find target domain $targetDomain")
      _ <- Either.cond(
        !connectedDomainsMap.contains(sourceDomainId),
        (),
        s"Participant is still connected to source domain $sourceDomain",
      )
      _ <- Either.cond(
        !connectedDomainsMap.contains(targetDomainId),
        (),
        s"Participant is still connected to target domain $targetDomain",
      )
      _ <- repairService.changeDomainAwait(
        contractIds,
        sourceDomainId,
        targetDomainId,
        skipInactive,
        batchSize,
      )
    } yield ()
  }

  def ignoreEventsRepair(
      domain: DomainId,
      from: SequencerCounter,
      to: SequencerCounter,
      force: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(s"Ignoring sequenced events from $from to $to (force = $force).")
    for {
      _ <- Either.cond(
        !connectedDomainsMap.contains(domain),
        (),
        s"Participant is still connected to domain $domain",
      )
      _ <- repairService.ignoreEvents(domain, from, to, force)
    } yield ()
  }

  def unignoreEventsRepair(
      domain: DomainId,
      from: SequencerCounter,
      to: SequencerCounter,
      force: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(s"Unignoring sequenced events from $from to $to (force = $force).")
    for {
      _ <- Either.cond(
        !connectedDomainsMap.contains(domain),
        (),
        s"Participant is still connected to domain $domain",
      )
      _ <- repairService.unignoreEvents(domain, from, to, force)
    } yield ()
  }

  /** prepares a domain connection for migration: connect and wait until the topology state has been pushed
    * so we don't deploy against an empty domain
    */
  private def prepareDomainConnectionForMigration(
      aliasT: Traced[DomainAlias]
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = aliasT.withTraceContext {
    implicit tx => alias =>
      (for {
        _ <- performDomainConnection(alias, startSyncDomain = true, skipStatusCheck = true)
        success <- identityPusher
          .awaitIdle(alias, timeouts.unbounded.unwrap)
          .leftMap(reg => SyncServiceError.SyncServiceFailedDomainConnection(alias, reg))
        // now, tick the domain so we can be sure to have a tick that includes the topology changes
        syncService <- EitherT.fromEither[FutureUnlessShutdown](
          syncDomainForAlias(alias).toRight(SyncServiceError.SyncServiceUnknownDomain.Error(alias))
        )
        _ <- EitherT.right(
          FutureUnlessShutdown.outcomeF(
            syncService.timeTracker
              .awaitTick(syncService.topologyClient.approximateTimestamp)
              .map(_.map(_ => ()))
              .getOrElse(Future.unit)
          )
        )
        _ <- EitherT.fromEither[FutureUnlessShutdown](performDomainDisconnect(alias))
      } yield success)
        .leftMap[SyncDomainMigrationError](err =>
          SyncDomainMigrationError.MigrationParentError(alias, err)
        )
        .flatMap { success =>
          EitherT.cond[FutureUnlessShutdown](
            success,
            (),
            SyncDomainMigrationError.InternalError.Generic(
              "Failed to successfully dispatch topology state to target domain"
            ): SyncDomainMigrationError,
          )
        }
  }

  // Canton assumes that as long as the CantonSyncService is up we are "read"-healthy. We could consider lack
  // of storage readability as a way to be read-unhealthy, but as participants share the database backend with
  // the ledger-api-server and indexer, database-non-availability is already flagged upstream.
  override def currentHealth(): HealthStatus = HealthStatus.healthy

  // Write health requires the ability to transact, i.e. connectivity to at least one domain and HA-activeness.
  def currentWriteHealth(): HealthStatus =
    if (existsReadyDomain && isActive()) HealthStatus.healthy else HealthStatus.unhealthy

  def computeTotalLoad: Int = connectedDomainsMap.foldLeft(0) { case (acc, (_, syncDomain)) =>
    acc + syncDomain.numberOfDirtyRequests()
  }

  def checkOverloaded(telemetryContext: TelemetryContext): Option[state.v2.SubmissionResult] = {
    implicit val traceContext: TraceContext =
      TraceContext.fromDamlTelemetryContext(telemetryContext)
    val load = computeTotalLoad
    resourceManagementService.checkOverloaded(load)
  }

  def refreshCaches()(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- domainConnectionConfigStore.refreshCache()
      _ <- resourceManagementService.refreshCache()
    } yield ()

  override def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*
    val connectQueueFlush =
      connectQueue.asCloseable("connectQueue", parameters.processingTimeouts.network.unwrap)

    val instances = Seq(
      connectQueueFlush,
      migrationService,
      repairService,
      pruningProcessor,
    ) ++ syncCrypto.ips.allDomains.toSeq ++ connectedDomainsMap.values.toSeq ++ Seq(
      packageService,
      domainRouter,
      domainRegistry,
      inFlightSubmissionTracker,
      domainConnectionConfigStore,
      domainCausalityStore,
      syncDomainPersistentStateManager,
      participantNodePersistentState,
    )

    Lifecycle.close(instances: _*)(logger)
  }

  override def toString: String = s"CantonSyncService($participantId)"
}

object CantonSyncService {
  trait Factory[+T <: CantonSyncService] {
    def create(
        participantId: ParticipantId,
        domainRegistry: DomainRegistry,
        domainConnectionConfigStore: DomainConnectionConfigStore,
        domainAliasManager: DomainAliasManager,
        participantNodePersistentState: ParticipantNodePersistentState,
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
        syncDomainPersistentStateFactory: SyncDomainPersistentStateFactory,
        packageService: PackageService,
        topologyStoreFactory: TopologyStoreFactory,
        multiDomainCausalityStore: MultiDomainCausalityStore,
        topologyManager: ParticipantTopologyManager,
        identityPusher: ParticipantTopologyDispatcher,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiProvider,
        ledgerId: LedgerId,
        engine: Engine,
        syncDomainStateFactory: SyncDomainEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        indexedStringStore: IndexedStringStore,
        metrics: ParticipantMetrics,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): T
  }

  object DefaultFactory extends Factory[CantonSyncService] {
    override def create(
        participantId: ParticipantId,
        domainRegistry: DomainRegistry,
        domainConnectionConfigStore: DomainConnectionConfigStore,
        domainAliasManager: DomainAliasManager,
        participantNodePersistentState: ParticipantNodePersistentState,
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
        syncDomainPersistentStateFactory: SyncDomainPersistentStateFactory,
        packageService: PackageService,
        topologyStoreFactory: TopologyStoreFactory,
        multiDomainCausalityStore: MultiDomainCausalityStore,
        topologyManager: ParticipantTopologyManager,
        identityPusher: ParticipantTopologyDispatcher,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiProvider,
        ledgerId: LedgerId,
        engine: Engine,
        syncDomainStateFactory: SyncDomainEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        indexedStringStore: IndexedStringStore,
        metrics: ParticipantMetrics,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): CantonSyncService =
      new CantonSyncService(
        participantId,
        domainRegistry,
        domainConnectionConfigStore,
        domainAliasManager,
        participantNodePersistentState,
        participantNodeEphemeralState,
        syncDomainPersistentStateManager,
        syncDomainPersistentStateFactory,
        packageService,
        topologyStoreFactory,
        multiDomainCausalityStore,
        topologyManager,
        identityPusher,
        partyNotifier,
        syncCrypto,
        NoOpPruningProcessor,
        ledgerId,
        engine,
        syncDomainStateFactory,
        clock,
        resourceManagementService,
        cantonParameterConfig,
        SyncDomain.DefaultFactory,
        indexedStringStore,
        metrics,
        () => storage.isActive,
        futureSupervisor,
        loggerFactory,
      )
  }
}

trait SyncServiceError extends Serializable with Product with CantonError

object SyncServiceInjectionError extends InjectionErrorGroup {

  import com.daml.lf.data.Ref.{ApplicationId, CommandId}

  @Explanation("This error results if a command is submitted to the passive replica.")
  @Resolution("Send the command to the active replica.")
  object PassiveReplica
      extends ErrorCode(
        id = "NODE_IS_PASSIVE_REPLICA",
        ErrorCategory.TransientServerFailure,
      ) {
    case class Error(applicationId: ApplicationId, commandId: CommandId)
        extends TransactionErrorImpl(
          cause = "Cannot process submitted command. This participant is the passive replica."
        )
  }

  @Explanation(
    "This errors results if a command is submitted to a participant that is not connected to any domain."
  )
  @Resolution(
    "Connect your participant to the domain where the given parties are hosted."
  )
  object NotConnectedToAnyDomain
      extends ErrorCode(
        id = "NOT_CONNECTED_TO_ANY_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Error()
        extends TransactionErrorImpl(cause = "This participant is not connected to any domain.")
  }

  @Explanation("This errors occurs if an internal error results in an exception.")
  @Resolution("Contact support.")
  object InjectionFailure
      extends ErrorCode(
        id = "COMMAND_INJECTION_FAILURE",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class Failure(throwable: Throwable)
        extends TransactionErrorImpl(
          cause = "Command failed with an exception",
          throwableO = Some(throwable),
        )
  }

}

object SyncServiceError extends SyncServiceErrorGroup {

  @Explanation(
    "This error results if a domain connectivity command is referring to a domain alias that has not been registered."
  )
  object SyncServiceUnknownDomain
      extends ErrorCode(
        "SYNC_SERVICE_UNKNOWN_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = s"The domain with alias ${domain.unwrap} is unknown.")
        with SyncServiceError
  }

  @Explanation(
    "This error results on an attempt to register a new domain under an alias already in use."
  )
  object SyncServiceAlreadyAdded
      extends ErrorCode(
        "SYNC_SERVICE_ALREADY_ADDED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = "The domain with the given alias has already been added.")
        with SyncServiceError
  }

  abstract class MigrationErrors extends ErrorGroup()

  abstract class DomainRegistryErrorGroup extends ErrorGroup()

  case class SyncServiceFailedDomainConnection(domain: DomainAlias, parent: DomainRegistryError)(
      implicit val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[DomainRegistryError] {

    override def logOnCreation: Boolean = false
    override def mixinContext: Map[String, String] = Map("domain" -> domain.unwrap)

  }

  case class SyncServiceMigrationError(
      from: DomainAlias,
      to: DomainAlias,
      parent: SyncDomainMigrationError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[SyncDomainMigrationError] {

    override def logOnCreation: Boolean = false
    override def mixinContext: Map[String, String] = Map("from" -> from.unwrap, "to" -> to.unwrap)

  }

  @Explanation(
    "This error is logged when the synchronization service shuts down because the remote domain has disabled this participant."
  )
  @Resolution("Contact the domain operator and inquire why you have been booted out.")
  object SyncServiceDomainDisabledUs
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_DISABLED_US",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override def logLevel: Level = Level.WARN

    case class Error(domain: DomainAlias, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$domain rejected our subscription attempt with permission denied."
        )
  }

  @Explanation(
    "This error is logged when a sync domain has a non-active status."
  )
  @Resolution(
    """If you attempt to connect to a domain that has either been migrated off or has a pending migration, 
       |this error will be emitted. Please complete the migration before attempting to connect to it."""
  )
  object SyncServiceDomainIsNotActive
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_STATUS_NOT_ACTIVE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    case class Error(domain: DomainAlias, status: DomainConnectionConfigStore.Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"$domain has status $status and can therefore not be connected to."
        )
        with SyncServiceError
  }

  @Explanation(
    "This error is logged when a sync domain is disconnected because the participant became passive."
  )
  @Resolution("Fail over to the active participant replica.")
  object SyncServiceDomainBecamePassive
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_BECAME_PASSIVE",
        ErrorCategory.TransientServerFailure,
      ) {

    override def logLevel: Level = Level.WARN

    case class Error(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$domain disconnected because participant became passive."
        )
  }

  @Explanation(
    "This error is emitted when an operation is attempted such as repair that requires the domain connection to be disconnected and clean."
  )
  @Resolution("Disconnect the domain before attempting the command.")
  object SyncServiceDomainMustBeOffline
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_MUST_BE_OFFLINE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = show"$domain must be disconnected for the given operation")
        with SyncServiceError

  }

  @Explanation(
    "This error is logged when a sync domain is unexpectedly disconnected from the Canton " +
      "sync service (after having previously been connected)"
  )
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceDomainDisconnect
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_DISCONNECTED",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    case class UnrecoverableError(domain: DomainAlias, _reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = show"$domain fatally disconnected because of ${_reason}")

    case class UnrecoverableException(domain: DomainAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            show"Domain $domain fatally disconnected because of an exception ${throwable.getMessage}",
          throwableO = Some(throwable),
        )

  }

  @Explanation("This error indicates an internal issue.")
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceInternalError
      extends ErrorCode(
        "SYNC_SERVICE_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class MissingDomainConfig(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "A domain config is missing for the given alias.")
        with SyncServiceError

    case class UnknownDomainParameters(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The domain parameters for the given domain are missing in the store"
        )
        with SyncServiceError

    case class Failure(domain: DomainAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The domain failed to startup due to an internal error",
          throwableO = Some(throwable),
        )
        with SyncServiceError

    case class InitError(domain: DomainAlias, error: SyncDomainInitializationError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "The domain failed to initialize due to an internal error")
        with SyncServiceError

    case class DomainIsMissingInternally(domain: DomainAlias, where: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Failed to await for participant becoming active due to missing domain objects"
        )
        with SyncServiceError

  }

  @Explanation("The participant has detected that another node is behaving maliciously.")
  @Resolution("Contact support.")
  object SyncServiceAlarm extends AlarmErrorCode("SYNC_SERVICE_ALARM") {
    case class Warn(override val cause: String) extends Alarm(cause)
  }

  case class SyncServiceStartupError(override val errors: NonEmpty[Seq[SyncServiceError]])(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with CombinedError[SyncServiceError]

  @Explanation(
    """The participant is not connected to a domain and can therefore not allocate a party 
    because the party notification is configured as ``party-notification.type = via-domain``."""
  )
  @Resolution(
    "Connect the participant to a domain first or change the participant's party notification config to ``eager``."
  )
  object PartyAllocationNoDomainError
      extends ErrorCode(
        "PARTY_ALLOCATION_WITHOUT_CONNECTED_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Error(submission_id: LedgerSubmissionId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Cannot allocate a party without being connected to a domain"
        )
  }
}
