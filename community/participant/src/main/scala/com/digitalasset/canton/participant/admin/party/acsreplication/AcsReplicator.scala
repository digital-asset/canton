// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AgreementStatus,
  Disconnected,
  EphemeralFileImporterProgress,
  EphemeralSequencerChannelProgress,
  PartyReplicationAuthorization,
  PartyReplicationFailed,
  PersistentProgress,
  ReplicationParams,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicator.{
  AddPartyRequestId,
  PartyReplicationArguments,
}
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStage.*
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicator.AcsReplicationRequestId
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationStatus,
  PartyReplicationTestInterceptor,
}
import com.digitalasset.canton.participant.config.AlphaOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.protocol.party.TargetParticipantAcsPersistence
import com.digitalasset.canton.participant.protocol.party.acsreplication.{
  AcsReplicationProcessor,
  AcsReplicationSourceParticipantProcessor,
  AcsReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.store.{
  AcsReplicationStateManager,
  PartyReplicationStateManager,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

/** Acts on behalf of the participant's ACS replication requests handling asynchronous requests and
  * driving progress in its execution queue and based on state from the AcsReplicationStateManager.
  */
// TODO(#35267) use AcsReplicationStatus and parameters instead of PartyReplicationStatus
// TODO(#35267) some auxiliary methods are duplicated here and in PartyReplicator
final class AcsReplicator(
    participantId: ParticipantId,
    syncService: CantonSyncService,
    internalIndexService: InternalIndexService,
    clock: Clock,
    // TODO(#35267) replace with an AcsReplicator-specific config class (ACS replication could be allowed but not OnPR)
    config: AlphaOnlinePartyReplicationConfig,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    parallelism: PositiveInt = AcsReplicator.defaultParallelism,
    progressSchedulingInterval: PositiveFiniteDuration =
      AcsReplicator.defaultProgressSchedulingInterval,
)(implicit
    executionContext: ExecutionContext,
    mat: Materializer,
) extends AcsReplicationAdminWorkflow.AcsReplicationTransactionHandler
    with FlagCloseable
    with HasCloseContext
    with NamedLogging {

  // ACS replications state must be modified only within the simple executionQueue.
  // When read outside executeAsync*, readers must be aware that the map concurrently
  // changes and read state may be immediately stale.
  private[party] val acsReplicationStateManager =
    new AcsReplicationStateManager(
      participantId,
      storage,
      futureSupervisor,
      exitOnFatalFailures,
      loggerFactory,
      timeouts,
    )

  private val executionQueue = new SimpleExecutionQueue(
    "acs-replicator-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private val progressSchedulingActive = new AtomicBoolean(false)

  private val topologyWorkflow =
    new AcsReplicationTopologyWorkflow(timeouts, loggerFactory)

  private val damlAdminWorkflowO = new SingleUseCell[AcsReplicationAdminWorkflow]

  private[party] val testInterceptorO: Option[PartyReplicationTestInterceptor] =
    config.testInterceptor.map(_())

  /** Validates ACS replication arguments and propose party ACS replication via the provided admin
    * workflow service.
    */
  private[admin] def replicateAcsAsync(
      args: PartyReplicationArguments,
      requestId: AcsReplicationRequestId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyReplicationStatus] =
    executionQueue.executeEUS(
      {
        val PartyReplicationArguments(
          partyId,
          synchronizerId,
          sourceParticipantId,
          serial,
          participantPermission,
        ) = args
        for {
          _ <- EitherT.cond[FutureUnlessShutdown](
            syncService.isActive(),
            logger.info(
              s"Initiating ACS replication of party $partyId from participant $sourceParticipantId on synchronizer $synchronizerId"
            ),
            s"Participant $participantId is inactive",
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            damlAdminWorkflowO.get.toRight(
              "The ACS replication requires the `unsafe_sequencer_channel_support` configuration flag to be true"
            )
          )
          connectedSynchronizer <-
            EitherT.fromEither[FutureUnlessShutdown](
              syncService
                .readyConnectedSynchronizerById(synchronizerId)
                .toRight(s"Unknown synchronizer $synchronizerId")
            )
          syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
          _ <- ensurePartyHostedBySourceAndTargetParticipant(
            partyId,
            sourceParticipantId,
            participantId,
            serial,
            syncPersistentState.topologyStore,
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](ensureCanReplicateAcs())
          newStatus = PartyReplicationStatus(
            PartyReplicationStatus.ReplicationParams(
              requestId,
              partyId,
              synchronizerId,
              sourceParticipantId,
              participantId,
              serial,
              participantPermission,
            ),
            syncPersistentState.staticSynchronizerParameters.protocolVersion,
          )
          _ <- acsReplicationStateManager.add(newStatus)
        } yield {
          logger.info(s"New ACS replication $requestId created and is about to start")
          activateProgressMonitoring(requestId)
          newStatus
        }
      },
      s"replicate ACS of party ${args.partyId} on ${args.synchronizerId}",
    )

  /** Activates progress scheduling once a new ACS replication request is received unless already
    * active.
    */
  private def activateProgressMonitoring(
      requestId: AcsReplicationRequestId
  )(implicit traceContext: TraceContext): Unit = {
    val previouslyActive = progressSchedulingActive.getAndSet(true)
    if (previouslyActive) {
      logger.info(s"Progress scheduling already active, so no need to activate for $requestId.")
    } else {
      logger.info(s"Activating progress scheduling for party replication $requestId.")
      scheduleExecuteAsync(progressSchedulingInterval)(progressAcsReplications())
    }
  }

  /** Single point of entry for progress monitoring and advancing of ACS replication states for
    * those states that are driven by the ACS replicator.
    */
  private def progressAcsReplications()(implicit traceContext: TraceContext): Unit = {
    val activeAcsReplications = acsReplicationStateManager.collect {
      case (requestId, status) if status.isProgressExpected => requestId
    }

    if (activeAcsReplications.isEmpty) {
      logger.info("No ACS replication progress to monitor, deactivating progress scheduling.")
      progressSchedulingActive.set(false)
    }
    // Check if any ACS replication work is currently running and back off if it is to avoid eagerly queuing
    // obsolete state transitions.
    else {
      if (!executionQueue.isEmpty) {
        logger.debug(
          s"Skipping advancing ACS replication progress because still busy with ${executionQueue.queued
              .mkString(", ")}."
        )
      } else {
        // In case the set of requestIds has changed (particularly if grown) since "activeAcsReplications"
        // has been read above, we will pick it up on the next invocation scheduled below.
        activeAcsReplications.foreach(progressAcsReplication)
      }
      // Schedule the next time to progress-check party replications asynchronously, i.e. not recursively.
      scheduleExecuteAsync(progressSchedulingInterval)(progressAcsReplications())
    }
  }

  private def progressAcsReplication(
      requestId: AcsReplicationRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(requestId, s"progress party replication $requestId")(
      acsReplicationStateManager
        .get(requestId)
        .flatMap(AcsReplicationStage.fromPartyReplicationStatus)
        .fold(EitherTUtil.unitUS[String]) {
          // Stages listed in order of occurrence
          // TODO(#35267) remove this step completely
          /*
            - onboarding flag is verified by PartyReplicator
            - the fact that TP hosts the party is verified in replicateAcsAsync and
              processAcsReplicationProposalAtSourceParticipant methods
            - the stage is needed for now, because the topology transaction timestamp is obtained here
              (needed for the sequencer channel processor). Later, the timestamp will be provided by the client.
           */
          case VerifyingOnboardingTopologyAuthorization(params) =>
            logger.debug(s"Verifying party replication $requestId topology authorization")
            verifyOnboardingTopology(params.requestId)
          case NeedsToProposeAcsReplicationSequencerChannel(params, errorMessage) =>
            logger.debug(
              s"Proposing to create sequencer channel for ACS replication $requestId of party ${params.partyId}." +
                errorMessage.fold("")(msg => s" The channel was previously disconnected: $msg")
            )
            proposeAcsReplicationSequencerChannel(params)
          // TODO(#35267) rename to AcsReplicationSequencerChannelAgreementProposed
          case AcsReplicationSequencerChannelAgreementProposed(params) =>
            logger.debug(
              s"Sequencer channel proposed for ACS replication $requestId of party ${params.partyId}. Waiting for agreement."
            )
            EitherTUtil.unitUS

          case NeedToConnectToSequencerChannel =>
            logger.debug(s"Connecting to sequencer channel for ACS replication $requestId")
            connectToSequencerChannel(requestId)

          case NeedToReconnectToDisconnectedSequencerChannel(message) =>
            logger.info(s"ACS replication $requestId attempting to reconnect after: $message")
            attemptToReconnectToSequencerChannel(requestId)

          // Stages shared between File-based and SequencerChannel-based OnPR:
          case ReplicatingPartyAcs(p, progress) =>
            if (progress.fullyProcessedAcs) {
              logger.debug(
                s"ACS replication $requestId has finished replicating all ${progress.processedContractCount} contracts for ${p.partyId}."
              )
              finishAcsReplication(requestId)
            } else {
              progress match {
                case EphemeralSequencerChannelProgress(_, _, _, _, processor) =>
                  logger.debug(
                    s"ACS replication $requestId has replicated ${progress.processedContractCount} contracts for ${p.partyId}. Progress driven by processor."
                  )
                  // ensure that the processor is making progress
                  EitherT.rightT[FutureUnlessShutdown, String](
                    processor.foreach(_.progressAcsReplication())
                  )
                case replicationNonRuntime: PersistentProgress =>
                  // TODO(#29498): As part of TP-resilience to restart and crash recovery, rebuild target participant
                  //  processor once reconnected to synchronizer.
                  EitherT.leftT[FutureUnlessShutdown, Unit](
                    s"ACS replication ${p.requestId} AcsReplicationProgress not in runtime state: $replicationNonRuntime"
                  )
                // TODO(#35267) no mention of file import should be here
                case _: EphemeralFileImporterProgress =>
                  EitherT.leftT[FutureUnlessShutdown, Unit](
                    s"Sequencer channel based ACS replication ${p.requestId} shouldn't have EphemeralFileImporterProgress"
                  )
              }
            }
          case IsInInvalidState(error) =>
            EitherT.leftT[FutureUnlessShutdown, Unit](error.message)
          case other =>
            EitherT.leftT[FutureUnlessShutdown, Unit](s"Invalid state: $other")
        }
    )

  private def connectToSequencerChannel(
      requestId: AcsReplicationRequestId
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val ownsSessionKey = true
    val noSessionKey = false
    ensureParticipantStateAndSynchronizerConnectedWithChannelSupport(requestId) {
      case (
            PartyReplicationStatus(
              params,
              PartyReplicationStatus.AgreementStatus.Exists(_, agreedAt, sequencerId),
              Some(PartyReplicationAuthorization(onboardingAt, _)),
              _,
              _,
              _,
              _,
              _,
            ),
            connectedSynchronizer,
            channelClient,
          ) =>
        for {
          partiesAlreadyHostedByTargetParticipant <- EitherT.right[String](
            partiesHostedByParticipant(
              params.targetParticipantId,
              params.partyId,
              connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
              onboardingAt,
            )
          )
          processorInfo <-
            if (participantId == params.sourceParticipantId) {
              AcsReplicationSourceParticipantProcessor
                .initialize(
                  connectedSynchronizer.psid,
                  params.partyId,
                  requestId,
                  onboardingAt.value,
                  participantId,
                  partiesAlreadyHostedByTargetParticipant,
                  agreedAt,
                  internalIndexService,
                  acsReplicationStateManager,
                  recordSequencerChannelError(requestId, traceContext),
                  markDisconnected(requestId)(_, _),
                  syncService.participantNodePersistentState.value.ledgerApiStore,
                  futureSupervisor,
                  exitOnFatalFailures,
                  timeouts,
                  loggerFactory,
                  testInterceptorO.getOrElse(PartyReplicationTestInterceptor.AlwaysProceed),
                )
                .map((_: AcsReplicationProcessor, params.targetParticipantId, noSessionKey))
            } else if (participantId == params.targetParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                {
                  val contextualLoggerFactory = loggerFactory
                    .append("psid", connectedSynchronizer.psid.toProtoPrimitive)
                    .append("partyId", params.partyId.toProtoPrimitive)
                    .append("requestId", requestId.toHexString)
                  // TODO(#35267) should be provided from outside by client (PartyReplicator)
                  val acsTransferContractHandler = new TargetParticipantAcsPersistence(
                    requestId,
                    connectedSynchronizer.psid,
                    onboardingAt,
                    new TargetParticipantAcsPersistence.PersistsContractsImpl(
                      syncService.participantNodePersistentState
                    ),
                    connectedSynchronizer.ephemeral.requestTracker,
                    connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
                      .getOrElse(
                        throw new IllegalStateException("Expect store when OnPR enabled")
                      ),
                    contextualLoggerFactory,
                  )
                  (
                    new AcsReplicationTargetParticipantProcessor(
                      connectedSynchronizer.psid,
                      params.partyId,
                      requestId,
                      onboardingAt,
                      partiesAlreadyHostedByTargetParticipant,
                      params.sourceParticipantId,
                      agreedAt,
                      acsReplicationStateManager,
                      recordSequencerChannelError(requestId, traceContext),
                      markDisconnected(requestId),
                      acsTransferContractHandler,
                      config.target,
                      futureSupervisor,
                      exitOnFatalFailures,
                      timeouts,
                      contextualLoggerFactory,
                      testInterceptorO.getOrElse(PartyReplicationTestInterceptor.AlwaysProceed),
                    ): AcsReplicationProcessor,
                    params.sourceParticipantId,
                    ownsSessionKey,
                  )
                }
              )
            } else {
              EitherT.leftT[
                FutureUnlessShutdown,
                (AcsReplicationProcessor, ParticipantId, Boolean),
              ](
                s"participant $participantId is neither source nor target"
              )
            }
          (processor, counterParticipantId, isSessionKeyOwner) = processorInfo
          // Set replication state before channel connect so that processor always finds "progress" state.
          _ <- acsReplicationStateManager.update_(requestId, _.setProcessor(processor))
          _ <- channelClient
            .connectToSequencerChannel(
              sequencerId,
              SequencerChannelId(requestId.toHexString),
              counterParticipantId,
              processor,
              isSessionKeyOwner,
              onboardingAt.value,
            )
        } yield {
          logger.info(s"ACS replication $requestId connected to sequencer $sequencerId")
        }
    }
  }

  private def attemptToReconnectToSequencerChannel(
      requestId: AcsReplicationRequestId
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnectedWithChannelSupport(requestId) {
      case (
            PartyReplicationStatus(
              params,
              PartyReplicationStatus.AgreementStatus.Exists(_, _, sequencerId),
              Some(PartyReplicationAuthorization(effectiveAt, _)),
              Some(EphemeralSequencerChannelProgress(_, _, _, _, Some(processor))),
              _,
              _,
              _,
              Some(Disconnected(_)),
            ),
            _,
            channelClient,
          ) =>
        for {
          processorInfo <-
            if (participantId == params.sourceParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (processor, params.targetParticipantId, /* isSessionKeyOwner = */ false)
              )
            } else if (participantId == params.targetParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (processor, params.sourceParticipantId, /* isSessionKeyOwner = */ true)
              )
            } else {
              EitherT.leftT[
                FutureUnlessShutdown,
                (AcsReplicationProcessor, ParticipantId, Boolean),
              ](
                s"participant $participantId is neither source nor target"
              )
            }
          (processor, counterParticipantId, isSessionKeyOwner) = processorInfo
          // Error during attempt to reconnect should not terminally fail OnPR.
          // Instead, turn an error into an optional message as an indication that
          // a retry is warranted.
          cannotReconnectMessageO <- EitherT
            .right[String](
              // Before attempting to reconnect via the bidirectionally streaming request,
              // try a channel-ping because the former does not return an error immediately,
              // but subsequently produces another disconnect. Doing a ping lowers the
              // chances of unnecessary and noisy status-change toggling.
              channelClient
                .ping(sequencerId)
                .flatMap(_ =>
                  channelClient
                    .connectToSequencerChannel(
                      sequencerId,
                      SequencerChannelId(requestId.toHexString),
                      counterParticipantId,
                      processor,
                      isSessionKeyOwner,
                      effectiveAt.value,
                    )
                )
                .value
                .map(_.swap.toOption)
            )
          _ <- cannotReconnectMessageO.fold {
            logger.info(s"ACS replication $requestId reconnected to sequencer $sequencerId")
            acsReplicationStateManager.update_(requestId, _.modifyErrorO(_ => None))
          } { cannotReconnectMsg =>
            logger.info(
              s"ACS replication $requestId not yet able to reconnect to sequencer $sequencerId: $cannotReconnectMsg"
            )
            EitherTUtil.unitUS
          }
        } yield ()
    }

  private def partiesHostedByParticipant(
      participantId: ParticipantId,
      except: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
      asOfExclusive: EffectiveTime,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    topologyStore
      .inspect(
        proposals = false,
        timeQuery = TimeQuery.Snapshot(asOfExclusive.value),
        asOfExclusiveO = None, // ignored for TimeQuery.Snapshot; always exclusive
        op = Some(TopologyChangeOp.Replace),
        types = Seq(TopologyMapping.Code.PartyToParticipant),
        idFilter = None,
        namespaceFilter = None,
      )
      .map(
        _.collectOfMapping[PartyToParticipant]
          .collectOfType[TopologyChangeOp.Replace]
          .result
          .withFilter { x =>
            val ptp = x.mapping
            ptp.partyId != except &&
            ptp.participants.exists(_.participantId == participantId)
          }
          .map(_.mapping.partyId)
          .toSet
      )

  // TODO(#35267) remove verification of serial and onboarding flag
  private def verifyOnboardingTopology(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnectedWithChannelSupport(requestId) {
      case (
            PartyReplicationStatus(params, _, None, _, _, _, _, _),
            connectedSynchronizer,
            _,
          ) =>
        for {
          authorizedAtO <- topologyWorkflow.verifyOnboardingTopology(
            params,
            connectedSynchronizer,
          )
          // To be sure the authorization has become effective, wait until the topology change is visible via the ledger api
          _ <- authorizedAtO match {
            case Some((EffectiveTime(authorizedAt), _)) =>
              val operation = s"observe ${params.partyId} topology transaction via ledger api"
              val retryCounter = new AtomicInteger(0)
              retryUntilLocalStoreUpdatedInExpectedState(operation)(
                synchronizeWithClosingF(_) {
                  for {
                    offsetO <- syncService.participantNodePersistentState.value.ledgerApiStore
                      .topologyEventOffsetPublishedOnRecordTime(
                        params.synchronizerId,
                        authorizedAt,
                      )
                    _ <- {
                      // Hopefully temporary logging aid to debug a rare OnlinePartyReplicationRecoverFromDisruptionsTest
                      // flake in which a PartyToParticipant addition has been authorized by topology, but the
                      // corresponding event does not appear in the ledger api store.
                      val currentCounter = retryCounter.get()
                      // Only begin additional debug logging once sufficiently many retries have not helped.
                      if (currentCounter <= 3 || !logger.underlying.isDebugEnabled()) Future.unit
                      else {
                        syncService.participantNodePersistentState.value.ledgerApiStore
                          .topologyPartyEventBatch(SequentialIdBatch.EventSeqIdRange(0L, 1000000L))
                          .map { partyAuthorizations =>
                            logger.debug(
                              s"Party events on $participantId (querying at $authorizedAt retry $currentCounter, offset $offsetO):\n${partyAuthorizations
                                  .mkString("\n")}"
                            )
                          }
                      }
                    }
                  } yield {
                    retryCounter.incrementAndGet().discard
                    Either.cond(offsetO.nonEmpty, (), s"failed to $operation")
                  }
                }
              )
            case None => EitherT.rightT[FutureUnlessShutdown, String](())
          }
          _ <- authorizedAtO.fold {
            logger.debug(
              s"Onboarding topology for party replication $requestId and party ${params.partyId} not yet authorized."
            )
            EitherTUtil.unitUS[String]
          } { case (authorizedAt, topologySerial) =>
            logger.info(
              s"Party replication $requestId onboarding topology of party ${params.partyId} authorized with serial $topologySerial and effective time $authorizedAt"
            )
            acsReplicationStateManager.update_(
              requestId,
              _.setAuthorization(
                PartyReplicationAuthorization(authorizedAt, isOnboardingFlagCleared = false)
              ).setTopologySerial(topologySerial),
            )
          }
        } yield ()
    }

  /** This completes ACS replication by executing the following final steps if they are found to not
    * have been executed yet:
    *
    *   - Archive the ACS replication agreement Daml contract to inform the SP that the TP no longer
    *     needs help replicating the party's ACS.
    *   - Mark ACS replication as completed once Daml contract is archived
    */
  private def finishAcsReplication(requestId: AcsReplicationRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId) {
      case (
            previous @ PartyReplicationStatus(
              params,
              agreementO,
              _,
              _,
              _,
              _,
              false, // not completed
              None,
            ),
            _,
          ) =>
        for {
          isAgreementArchived <- EitherT.right[String](
            (agreementO, damlAdminWorkflowO.get) match {
              case (
                    PartyReplicationStatus.AgreementStatus.Exists(damlAgreementCid, _, sequencerId),
                    Some(workflow),
                  ) =>
                workflow.markAcsReplicationAgreementDone(
                  AcsReplicationAgreementParams
                    .fromAgreedReplicationStatus(params, sequencerId),
                  damlAgreementCid,
                  traceContext,
                )
              case (PartyReplicationStatus.AgreementStatus.Archived, Some(_)) =>
                FutureUnlessShutdown.pure(true)
              case _ => FutureUnlessShutdown.pure(false)
            }
          )

          statusUpdates = {
            def statusUpdate(
                condition: Boolean,
                update: PartyReplicationStateManager.Modification,
            ): Seq[PartyReplicationStateManager.Modification] =
              if (condition) Seq(update) else Seq.empty

            statusUpdate(
              isAgreementArchived,
              _.setAgreementStatus(PartyReplicationStatus.AgreementStatus.Archived),
            )
              ++ statusUpdate(
                isAgreementArchived || agreementO.isEmpty, // not sure isEmpty should be here
                _.setCompleted(),
              )
          }

          status <-
            if (statusUpdates.nonEmpty) {
              // Compose potentially multiple modifications into a single modification.
              val combinedModification = statusUpdates
                .foldLeft[PartyReplicationStateManager.Modification](
                  // Seed the modification chain with a dummy modification that returns the previous status.
                  identity
                ) { case (composedModifications, nextModification) =>
                  prev => nextModification(composedModifications(prev))
                }
              acsReplicationStateManager.update(requestId, combinedModification)
            } else EitherT.rightT[FutureUnlessShutdown, String](previous)
        } yield {
          if (status.hasCompleted) {
            logger.info(s"ACS replication $requestId has completed")
          } else {
            logger.debug(
              s"ACS replication $requestId not yet completed. AgreementArchived: $isAgreementArchived"
            )
          }
        }
    }

  private def proposeAcsReplicationSequencerChannel(
      replicationParams: ReplicationParams
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      adminWorkflow <- EitherT.fromEither[FutureUnlessShutdown](
        damlAdminWorkflowO.get.toRight(
          "ACS replication requires the `unsafe_sequencer_channel_support` configuration flag to be true"
        )
      )
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(replicationParams.synchronizerId)
            .toRight(s"Unknown synchronizer $replicationParams.synchronizerId")
        )
      topologySnapshot = connectedSynchronizer.synchronizerHandle.topologyClient.headSnapshot
      sequencerIds <- EitherT
        .fromOptionF(
          topologySnapshot
            .sequencerGroup()
            .map(sg => NonEmpty.from(sg.toList.flatMap(_.active))),
          s"No active sequencer for synchronizer ${replicationParams.synchronizerId}",
        )
      sequencerCandidates <- selectSequencerCandidates(
        replicationParams.synchronizerId,
        sequencerIds,
      )
      _ <- adminWorkflow.proposeAcsReplication(
        replicationParams.requestId,
        replicationParams.partyId,
        replicationParams.synchronizerId,
        replicationParams.sourceParticipantId,
        sequencerCandidates,
        replicationParams.serial,
        replicationParams.participantPermission,
      )
      _ <- acsReplicationStateManager.update_(
        replicationParams.requestId,
        replicationStatus => {
          val withoutError = replicationStatus.modifyErrorO(_ => None)
          withoutError.setAgreementStatus(PartyReplicationStatus.AgreementStatus.Proposed)
        },
      )
    } yield ()

  private def selectSequencerCandidates(
      synchronizerId: SynchronizerId,
      sequencerIds: NonEmpty[List[SequencerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonEmpty[Seq[SequencerId]]] =
    for {
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight("Synchronizer not found")
        )
      channelClient <- EitherT.fromEither[FutureUnlessShutdown](
        connectedSynchronizer.sequencerChannelClientO.toRight("Channel client not configured")
      )
      // Only propose sequencers on which the target participant can perform a channel ping.
      withChannelSupport <- EitherT.right[String](
        MonadUtil
          .parTraverseWithLimit(parallelism)(sequencerIds)(sequencerId =>
            channelClient
              .ping(sequencerId)
              .fold(
                err => {
                  logger.info(s"Skipping sequencer $sequencerId: $err")
                  None
                },
                _ => Some(sequencerId),
              )
          )
          .map(_.flatten)
      )
      nonEmpty <- EitherT.fromOption[FutureUnlessShutdown](
        NonEmpty.from(withChannelSupport),
        s"No sequencers ${sequencerIds.mkString(",")} support channels",
      )
    } yield nonEmpty

  private def recordSequencerChannelError(requestId: AcsReplicationRequestId, tc: TraceContext)(
      error: String
  ): Unit = {
    implicit val traceContext: TraceContext = tc
    logger.error(s"ACS replication $requestId failed: $error")
    executeAsync(requestId, "error ACS replication") {
      EitherT.leftT[FutureUnlessShutdown, Unit](error)
    }
  }

  private def markDisconnected(
      requestId: AcsReplicationRequestId
  )(message: String, tc: TraceContext): Unit = {
    implicit val traceContext: TraceContext = tc
    executeAsync(requestId, "disconnect ACS replication") {
      for {
        status <- EitherT.fromEither[FutureUnlessShutdown](
          acsReplicationStateManager
            .get(requestId)
            .toRight(s"Unknown ACS replication $requestId")
        )
        _ <- status match {
          case PartyReplicationStatus(_, _, _, _, _, _, _, None) =>
            acsReplicationStateManager.update_(
              requestId,
              _.modifyErrorO(_ => Some(Disconnected(message))),
            )
          case PartyReplicationStatus(_, _, _, _, _, _, _, Some(_)) =>
            EitherTUtil.unitUS[String]
          case unexpectedStatus =>
            EitherT.leftT[FutureUnlessShutdown, Unit](
              s"ACS replication $requestId status $unexpectedStatus not expected upon channel disconnect"
            )
        }
      } yield ()
    }
  }

  /** Asynchronously execute the provided code block reflecting any returned "left" in the error
    * status.
    */
  private def executeAsync(requestId: AcsReplicationRequestId, operation: String)(
      code: => EitherT[FutureUnlessShutdown, String, Unit]
  )(implicit traceContext: TraceContext): Unit = {
    def recordIfError(requestId: AcsReplicationRequestId)(
        resultET: EitherT[FutureUnlessShutdown, String, Unit]
    ): FutureUnlessShutdown[Unit] = resultET.leftSemiflatMap { err =>
      acsReplicationStateManager
        .update(
          requestId,
          _.modifyErrorO { prevErrorO =>
            prevErrorO.foreach(prevError =>
              logger.warn(
                s"ACS replication $requestId has unexpectedly encountered error after previous error $prevError. Ignoring new error: $err"
              )
            )
            Some(PartyReplicationFailed(err))
          },
        )
        .fold(updateErr => logger.warn(s"$updateErr: $err"), _ => logger.warn(err))
    }.merge

    executeAsyncWithCustomResultHandling(requestId, s"$operation $requestId")(code)(recordIfError)
  }

  /** Asynchronously execute the provided code block and handle the result with a custom handler.
    * The custom "handleResult" handler allows deviating from the default error handling such as
    * when the SP rejects a TP-proposed ACS replication.
    */
  private def executeAsyncWithCustomResultHandling[A, I](
      operationId: I,
      operation: String,
  )(code: => EitherT[FutureUnlessShutdown, String, A])(
      handleResult: I => EitherT[
        FutureUnlessShutdown,
        String,
        A,
      ] => FutureUnlessShutdown[Unit]
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"About to $operation")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      handleResult(operationId)(executionQueue.executeEUS[String, A](code, operation)),
      s"$operation failed",
    )
  }

  /** Asynchronous, scheduled execution relies on the clock's scheduled executor for scheduling, but
    * relies on the simple execution queue for execution to prevent blocking the participant clock
    * scheduler for too long. This avoids introducing another scheduler along with a mostly unused
    * thread whenever the participant does not replicate a party.
    */
  private def scheduleExecuteAsync(
      delta: PositiveFiniteDuration
  )(code: => Unit)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Scheduling next check in $delta")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAfterCancelledOnShutdown(
        _ => code,
        s"${getClass.getName}: scheduled execution",
        delta.asJava,
      ),
      "ACS replicator progress scheduling",
    )
  }

  private def ensureParticipantStateAndSynchronizerConnected(requestId: AcsReplicationRequestId)(
      matchIfStateIsAsExpected: PartialFunction[
        (PartyReplicationStatus, ConnectedSynchronizer),
        EitherT[FutureUnlessShutdown, String, Unit],
      ]
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    _ <- EitherT.cond[FutureUnlessShutdown](
      syncService.isActive(),
      (),
      s"Stopping as participant $participantId is inactive",
    )
    status <- EitherT.fromEither[FutureUnlessShutdown](
      acsReplicationStateManager
        .get(requestId)
        .toRight(s"Unknown request id $requestId")
    )
    connectedSynchronizer <-
      EitherT.fromEither[FutureUnlessShutdown](
        syncService
          .readyConnectedSynchronizerById(status.params.synchronizerId)
          .toRight(
            s"Synchronizer ${status.params.synchronizerId} not connected during $status"
          )
      )
    _ <- matchIfStateIsAsExpected
      .lift((status, connectedSynchronizer))
      .getOrElse(
        EitherT.leftT[FutureUnlessShutdown, Unit](
          s"Unexpected status (synchronizer connected) $status"
        )
      )
  } yield ()

  /** Ensures that a number of prerequisites are met for the party replication with the specified
    * request id. Particularly, verify that:
    *   1. the participant is HA-active as only the active replica should perform OnPR,
    *   1. the request id is known,
    *   1. the participant is connected to the synchronizer logging only if it isn't rather than
    *      failing OnPR,
    *   1. the current status of the party replication is as expected,
    *   1. if connected, the synchronizer exposes a sequencer channel client.
    *
    * If all prerequisites are met, invokes the provided `onSuccess` callback. Returns an error if
    * any prerequisite is not met other than synchronizer connectivity such that progress can resume
    * upon reconnect.
    */
  private def ensureParticipantStateAndSynchronizerConnectedWithChannelSupport(
      requestId: AcsReplicationRequestId
  )(
      matchIfStateIsAsExpected: PartialFunction[
        (PartyReplicationStatus, ConnectedSynchronizer, SequencerChannelClient),
        EitherT[FutureUnlessShutdown, String, Unit],
      ]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val stateET = for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        syncService.isActive(),
        (),
        ReplicateAcsError.Other
          .Failure(requestId, s"Stopping as participant $participantId is inactive"),
      )
      status <- EitherT.fromEither[FutureUnlessShutdown](
        acsReplicationStateManager
          .get(requestId)
          .toRight(ReplicateAcsError.Other.Failure(requestId, s"Unknown request id $requestId"))
      )
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(status.params.synchronizerId)
            .toRight(
              ReplicateAcsError.DisconnectedFromSynchronizer.Failure(
                requestId,
                status.params.synchronizerId,
                s"Synchronizer ${status.params.synchronizerId} not connected during $status",
              )
            )
        )
      channelClient <- EitherT.fromEither[FutureUnlessShutdown](
        connectedSynchronizer.synchronizerHandle.sequencerChannelClientO.toRight(
          ReplicateAcsError.Other.Failure(
            requestId,
            s"Synchronizer ${status.params.synchronizerId} does not expose necessary sequencer channel client",
          )
        )
      )
      _ <- matchIfStateIsAsExpected
        .lift((status, connectedSynchronizer, channelClient))
        .getOrElse(
          EitherT.leftT[FutureUnlessShutdown, Unit](
            s"Unexpected status (synchronizer connected with channel support) $status"
          )
        )
        .leftMap(ReplicateAcsError.Other.Failure(requestId, _): ReplicateAcsError)
    } yield ()

    // Skip processing if the participant is not connected to the synchronizer instead of returning an error.
    stateET.leftFlatMap {
      case ReplicateAcsError.DisconnectedFromSynchronizer.Failure(requestId, synchronizerId, err) =>
        logger.info(
          s"ACS replication $requestId disconnected from synchronizer $synchronizerId. Waiting until reconnect: $err"
        )
        EitherTUtil.unitUS
      case err: ReplicateAcsError.Other.Failure =>
        EitherT.leftT[FutureUnlessShutdown, Unit](err.cause)
    }
  }

  // TODO(#35267) stop verifying the serial and use a timestamp instead (or nothing)
  /** Checks that the party is
    *   - hosted by the source and target participant
    *   - serial matches head authorized topology
    *
    * Called at target participant when starting the ACS replication and at source participant when
    * processing the ACS replication Daml proposal.
    */
  private def ensurePartyHostedBySourceAndTargetParticipant(
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        sourceParticipantId != targetParticipantId,
        (),
        s"Source and target participants $targetParticipantId cannot match",
      )
      partyToParticipantTopologyHeadTx <- topologyWorkflow.partyToParticipantTopologyHead(
        partyId,
        topologyStore,
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        serial == partyToParticipantTopologyHeadTx.serial,
        (),
        s"Specified party $partyId serial $serial does not match the encountered head serial ${partyToParticipantTopologyHeadTx.serial}.",
      )
      participantsOfParty = partyToParticipantTopologyHeadTx.mapping.participants.map(
        _.participantId
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          participantsOfParty.contains(sourceParticipantId) && participantsOfParty.contains(
            targetParticipantId
          ),
          (),
          s"PartyToParticipant mapping for party $partyId doesn't exist for either source or target participant $sourceParticipantId. Only exists for ${participantsOfParty
              .mkString(",")}",
        )
      )
    } yield ()

  private def ensureCanReplicateAcs(): Either[String, Unit] = for {
    _ <- acsReplicationStateManager
      .collectFirst {
        case (
              id,
              status @ PartyReplicationStatus(
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                Some(PartyReplicationStatus.PartyReplicationFailed(errorMsg)),
              ),
            ) =>
          (id, status, errorMsg)
      }
      .fold(Right(()): Either[String, Unit]) { case (id, previousStatus, errorMsg) =>
        Left(
          s"Participant $participantId has encountered previous error \"$errorMsg\" during ACS replication" +
            s" request $id after status $previousStatus and needs to be repaired"
        )
      }
    _ <- acsReplicationStateManager
      .collectFirst { case (id, status) if !status.hasCompleted => id -> status }
      .fold(Right(()): Either[String, Unit]) { case (id, status) =>
        Left(
          s"Only a single party ACS replication can be in progress: $status, but found ACS replication $id" +
            s" of party ${status.params.partyId} on synchronizer ${status.params.synchronizerId}" +
            s" with status $status"
        )
      }
  } yield ()

  override private[admin] def processAcsReplicationAgreementArchival(
      contractId: LfContractId
  )(implicit traceContext: TraceContext): Unit =
    executeAsyncWithCustomResultHandling(
      contractId,
      s"process archival of ACS replication agreement contract $contractId",
    ) {
      acsReplicationStateManager
        .findByAgreementContractId(contractId)
        .fold(EitherT.pure[FutureUnlessShutdown, String](Option.empty[AddPartyRequestId]))(
          replicationStatus =>
            for {
              requestId <- EitherT.fromEither[FutureUnlessShutdown](
                (replicationStatus.agreementStatus match {
                  case _: PartyReplicationStatus.AgreementStatus.Exists =>
                    Some(replicationStatus.params.requestId)
                  case _ => None
                }).toRight(s"No existing agreement for contract id $contractId")
              )
              _ <- acsReplicationStateManager.update_(
                requestId,
                _.setAgreementStatus(PartyReplicationStatus.AgreementStatus.Archived),
              )
            } yield Some(requestId)
        )
    } { _ => resET =>
      resET.value.map {
        case Left(err) =>
          logger.warn(
            s"Failed to process archival of ACS replication agreement contract $contractId: $err"
          )
        case Right(None) =>
          logger.info(
            s"ACS replication for contract agreement $contractId no longer tracked as local participant has issued the archival."
          )
        case Right(Some(requestId)) =>
          logger.info(
            s"ACS replication $requestId agreement contract $contractId has been archived"
          )
      }
    }

  /** Validates a channel proposal at the source participant and chooses a sequencer to participate
    * in party replication and respond accordingly by invoking the provided admin workflow callback.
    */
  override private[admin] def processAcsReplicationProposalAtSourceParticipant(
      proposalOrError: Either[String, AcsReplicationProposalParams],
      respondToProposal: Either[String, AcsReplicationAgreementParams] => Unit,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[String, Unit]] = {
    val operation = proposalOrError.fold(
      err => s"reject ACS replication: $err",
      params => s"respond to ACS replication proposal ${params.requestId}",
    )
    logger.info(s"About to $operation")
    val responseET = executionQueue.executeEUS(
      for {
        proposal <- EitherT.fromEither[FutureUnlessShutdown](proposalOrError)
        AcsReplicationProposalParams(
          _,
          partyId,
          synchronizerId,
          targetParticipantId,
          sequencerIdsProposed,
          serial,
          _,
        ) = proposal
        connectedSynchronizer <-
          EitherT.fromEither[FutureUnlessShutdown](
            syncService
              .readyConnectedSynchronizerById(synchronizerId)
              .toRight(s"Synchronizer $synchronizerId not connected")
          )
        _ <- EitherT.fromEither[FutureUnlessShutdown](ensureCanReplicateAcs())
        topologySnapshot =
          connectedSynchronizer.synchronizerHandle.topologyClient.headSnapshot
        sequencerIdsInTopology <- EitherT
          .fromOptionF(
            topologySnapshot.sequencerGroup().map(_.map(_.active)),
            s"No sequencer group for synchronizer $synchronizerId",
          )
        sequencerIdsTopologyIntersection <- EitherT.fromEither[FutureUnlessShutdown](
          NonEmpty
            .from(
              sequencerIdsProposed.forgetNE.filter(sequencerId =>
                sequencerIdsInTopology
                  .contains(sequencerId)
                  .tap(isKnown =>
                    if (!isKnown)
                      logger
                        .info(
                          s"Skipping sequencer $sequencerId not active on synchronizer $synchronizerId"
                        )
                  )
              )
            )
            .toRight(
              s"None of the proposed sequencers are active on synchronizer $synchronizerId"
            )
        )
        candidateSequencerIds <- selectSequencerCandidates(
          synchronizerId,
          sequencerIdsTopologyIntersection,
        )
        sequencerId <- EitherT.fromEither[FutureUnlessShutdown](
          candidateSequencerIds.headOption.toRight("No common sequencer")
        )
        _ = logger.info(
          s"Choosing sequencer $sequencerId among ${candidateSequencerIds.mkString(",")}"
        )
        syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
        // TODO(#35267) stop verifying onboarding flag once PartyReplicator can verify it itself
        _ <- ensurePartyHostedBySourceAndTargetParticipant(
          partyId,
          participantId,
          targetParticipantId,
          serial,
          syncPersistentState.topologyStore,
        )
      } yield (
        AcsReplicationAgreementParams.fromProposal(proposal, participantId, sequencerId),
        connectedSynchronizer.staticSynchronizerParameters.protocolVersion,
      ),
      operation,
    )

    for {
      agreementResponseE <- responseET.value
      // Respond to the proposal depending on the outcome of the agreement response.
      _ = respondToProposal(agreementResponseE.map { case (agreement, _) => agreement })
      _ <- agreementResponseE.fold(
        _ => FutureUnlessShutdown.unit,
        { case (response, protocolVersion) =>
          // Upon success indicate that the SP has processed the proposal.
          val newStatus = PartyReplicationStatus(
            PartyReplicationStatus.ReplicationParams(
              response.requestId,
              response.partyId,
              response.synchronizerId,
              response.sourceParticipantId,
              response.targetParticipantId,
              // TODO(#35267) replace with CantonTimestamp
              response.serial,
              // TODO(#35267) remove permission once using AcsReplicationStatus
              response.participantPermission,
            ),
            protocolVersion,
            AgreementStatus.Proposed,
          )
          acsReplicationStateManager
            .add(newStatus)
            .bimap(
              err =>
                logger.warn(
                  s"Unexpectedly unable to add already-tracked ACS replication ${response.requestId}. Dropping $newStatus: $err"
                ),
              _ => {
                logger.info(
                  s"ACS replication ${response.requestId} proposal processed at source participant"
                )
                activateProgressMonitoring(response.requestId)
              },
            )
            .merge
        },
      )
    } yield agreementResponseE.map(_ => ())
  }

  /** Processes creation of the ACS replication sequencer channel agreement.
    *
    * Called on both source and target participant.
    *
    * @param damlAgreementCid
    *   Daml contract of the agreement
    * @param agreedAt
    *   Holds the ledger effective time of the agreement contract creation for human consumption of
    *   when it was agreed to replicate the ACS.
    * @param mightNotRememberProposal
    *   whether it's allowed to not know about the proposal. Only possible at source participant.
    */
  override private[admin] def processAcsReplicationAgreement(
      damlAgreementCid: LfContractId,
      agreedAt: CantonTimestamp,
      mightNotRememberProposal: Boolean,
  )(
      agreementParams: AcsReplicationAgreementParams
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(agreementParams.requestId, "process agreement of ACS replication") {
      val requestId = agreementParams.requestId
      val paramsReceived =
        PartyReplicationStatus.ReplicationParams.fromAgreementParams(agreementParams)
      val agreement =
        PartyReplicationStatus.AgreementStatus.Exists(
          damlAgreementCid,
          agreedAt,
          agreementParams.sequencerId,
        )

      // If the party replication is legitimately not yet known (after a source participant node restart),
      // set the AgreementAccepted status.
      def processUntrackedAgreement() = {
        // TODO(#20636): Remove this eager action on the part of the SP, and have the TP
        //  renegotiate OnPR via a new proposal.
        logger.info(
          s"Backfilling party replication $requestId agreement not known due to a suspected participant node restart"
        )
        val synchronizerId = agreementParams.synchronizerId
        (for {
          latestSynchronizerConnectionConfig <- EitherT.fromEither[FutureUnlessShutdown](
            syncService.synchronizerConnectionConfigStore
              .getActive(synchronizerId)
              .leftMap(_.message)
          )
          psid <- EitherT.fromEither[FutureUnlessShutdown](
            latestSynchronizerConnectionConfig.configuredPsid.toOption
              .toRight(
                s"Latest synchronizer $synchronizerId config $latestSynchronizerConnectionConfig has no physical synchronizer id set"
              )
          )
          agreementReceived = PartyReplicationStatus(
            paramsReceived,
            psid.protocolVersion,
            agreementStatus = agreement,
          )
          _ <- acsReplicationStateManager.add(agreementReceived)
        } yield activateProgressMonitoring(requestId)).leftMap(err =>
          s"Unable to process party replication $requestId agreement: $err"
        )
      }

      def processExpectedAgreement(statusO: Option[PartyReplicationStatus]) =
        for {
          status <- EitherT.fromEither[FutureUnlessShutdown](
            statusO.toRight(s"Unknown request id $requestId")
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            status.ensureCanSetAgreement(paramsReceived)
          )
          _ <- acsReplicationStateManager.update_(
            requestId,
            _.setAgreementStatus(agreement),
          )
        } yield {
          logger.info(
            s"Party replication $requestId agreement $agreement accepted for party ${agreementParams.partyId}"
          )
          activateProgressMonitoring(requestId)
        }

      val statusO = acsReplicationStateManager.get(requestId)
      if (mightNotRememberProposal && statusO.isEmpty)
        processUntrackedAgreement()
      else processExpectedAgreement(statusO)
    }

  private[party] def retryUntilLocalStoreUpdatedInExpectedState[T](
      operation: String
  )(
      checkLocalStoreState: String => FutureUnlessShutdown[Either[String, T]]
  )(implicit traceContext: TraceContext) =
    EitherT(
      retry
        .Backoff(
          logger,
          this,
          maxRetries = timeouts.unbounded.retries(1.second),
          initialDelay = 1.second,
          maxDelay = 10.seconds,
          operationName = operation,
        )
        .unlessShutdown(
          checkLocalStoreState(operation),
          DbExceptionRetryPolicy,
        )
    )

  private[admin] def initializeDamlAdminWorkflow(
      workflow: AcsReplicationAdminWorkflow
  ): Unit = {
    damlAdminWorkflowO
      .putIfAbsent(workflow)
      .foreach(_ =>
        throw new IllegalStateException("ACS replication admin workflow already initialized")
      )
    workflow.registerAcsReplicationTransactionHandler(this)
  }

  override protected def onClosed(): Unit = {
    def getProcessors: Seq[AutoCloseable] =
      acsReplicationStateManager.collect[AcsReplicationProcessor] {
        case (
              _,
              PartyReplicationStatus(
                _,
                _,
                _,
                Some(EphemeralSequencerChannelProgress(_, _, _, _, Some(processor))),
                _,
                _,
                _,
                _,
              ),
            ) =>
          processor
      }

    // Close the execution queue first to prevent activity and races wrt partyReplications.
    LifeCycle.close(
      executionQueue +: topologyWorkflow +: getProcessors :+ acsReplicationStateManager
    )(logger)
  }

}

object AcsReplicator {
  type AcsReplicationRequestId = Hash

  // TODO(#35267) start using AcsReplicationArguments instead of ReplicationParams
  final case class AcsReplicationArguments(
      requestId: AcsReplicationRequestId,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      serial: PositiveInt,
      timestamp: CantonTimestamp,
      participantPermission: ParticipantPermission,
  )

  lazy val defaultParallelism: PositiveInt = PositiveInt.tryCreate(4)
  lazy val defaultProgressSchedulingInterval: PositiveFiniteDuration =
    PositiveFiniteDuration.ofSeconds(1L)

}
