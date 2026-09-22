// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicationStage.*
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.*
import com.digitalasset.canton.participant.admin.party.PartyReplicator.{
  AddPartyRequestId,
  PartyReplicationArguments,
}
import com.digitalasset.canton.participant.admin.party.acsreplication.*
import com.digitalasset.canton.participant.config.AlphaOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.protocol.party.PartyReplicationFileImporter
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationProcessor
import com.digitalasset.canton.participant.store.PartyReplicationStateManager
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** The party replicator acts on behalf of the participant's online party replication requests
  * handling asynchronous requests and driving progress in its execution queue and based on state
  * from the PartyReplicationStateManager.
  */
final class PartyReplicator(
    participantId: ParticipantId,
    private[admin] val acsReplicator: AcsReplicator,
    syncService: CantonSyncService,
    clock: Clock,
    config: AlphaOnlinePartyReplicationConfig,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    progressSchedulingInterval: PositiveFiniteDuration =
      PartyReplicator.defaultProgressSchedulingInterval,
)(implicit
    executionContext: ExecutionContext,
    mat: Materializer,
) extends FlagCloseable
    with HasCloseContext
    with NamedLogging {
  // Party replications state must be modified only within the simple executionQueue.
  // When read outside executeAsync*, readers must be aware that the map concurrently
  // changes and read state may be immediately stale.
  private val partyReplicationStateManager =
    new PartyReplicationStateManager(
      participantId,
      storage,
      futureSupervisor,
      exitOnFatalFailures,
      loggerFactory,
      timeouts,
      acsReplicator.acsReplicationStateManager,
    )

  private val indexingWorkflow =
    syncService.partyReplicationTriggersO
      .getOrElse(
        ErrorUtil.invalidState("PartyReplicator requires OnPR triggers")(
          errorLoggingContext(TraceContext.empty)
        )
      )
      .indexingWorkflow

  private val executionQueue = new SimpleExecutionQueue(
    "party-replicator-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private val progressSchedulingActive = new AtomicBoolean(false)

  private val topologyWorkflow =
    new PartyReplicationTopologyWorkflow(participantId, timeouts, loggerFactory)

  private val testInterceptorO: Option[PartyReplicationTestInterceptor] =
    config.testInterceptor.map(_())

  /** Validates online party replication arguments and propose party replication via the provided
    * admin workflow service.
    */
  private[admin] def addPartyAsync(args: PartyReplicationArguments)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Hash] =
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
              s"Initiating replication of party $partyId from participant $sourceParticipantId on synchronizer $synchronizerId"
            ),
            s"Participant $participantId is inactive",
          )
          connectedSynchronizer <-
            EitherT.fromEither[FutureUnlessShutdown](
              syncService
                .readyConnectedSynchronizerById(synchronizerId)
                .toRight(s"Unknown synchronizer $synchronizerId")
            )
          syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
          sourceParticipantId <- ensurePartyHostedBySourceButNotTargetParticipant(
            partyId,
            sourceParticipantId,
            participantId,
            syncPersistentState.topologyStore,
            serial,
          )
          requestId = buildRequestIdHash(args, syncPersistentState.pureCryptoApi)
          _ <- EitherT.fromEither[FutureUnlessShutdown](ensureCanAddParty())
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
          _ <- partyReplicationStateManager.add(newStatus)
        } yield {
          logger.info(s"Party replication $requestId proposal processed")
          activateProgressMonitoring(requestId)
          requestId
        }
      },
      s"add party ${args.partyId} on ${args.synchronizerId}",
    )

  private def buildRequestIdHash(args: PartyReplicationArguments, pureCryptoApi: CryptoPureApi) =
    pureCryptoApi
      .build(HashPurpose.OnlinePartyReplicationId)
      .addString(args.partyId.toProtoPrimitive)
      .addString(args.synchronizerId.toProtoPrimitive)
      .addString(args.sourceParticipantId.toProtoPrimitive)
      .addInt(args.serial.unwrap)
      .finish()

  private def ensureCanAddParty(): Either[String, Unit] = for {
    _ <- partyReplicationStateManager
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
          s"Participant $participantId has encountered previous error \"$errorMsg\" during add_party_async" +
            s" request $id after status $previousStatus and needs to be repaired"
        )
      }
    _ <- partyReplicationStateManager
      .collectFirst { case (id, status) if !status.hasCompleted => id -> status }
      .fold(Right(()): Either[String, Unit]) { case (id, status) =>
        Left(
          s"Only a single party replication can be in progress: $status, but found party replication $id" +
            s" of party ${status.params.partyId} on synchronizer ${status.params.synchronizerId}" +
            s" with status $status"
        )
      }
  } yield ()

  private[admin] def getAddPartyStatus(
      addPartyRequestId: AddPartyRequestId
  ): Option[PartyReplicationStatus] = partyReplicationStateManager.get(addPartyRequestId)

  private[admin] def getAddPartyStatus(
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      targetParticipantId: ParticipantId,
  ): Option[PartyReplicationStatus] = partyReplicationStateManager
    .collectFirst {
      case (_, status)
          if status.params.partyId == partyId &&
            status.params.synchronizerId == synchronizerId &&
            status.params.targetParticipantId == targetParticipantId =>
        status
    }
    .flatMap(status => partyReplicationStateManager.get(status.params.requestId))

  /** Adds a party to the local target participant using the ACS snapshot provided by a file via an
    * ACS stream by importing the ACS synchronously, i.e. when the returned EitherT succeeds, but
    * only fully completing party replication asynchronously (e.g. clearing the onboarding flag).
    *
    * @param args
    *   arguments shared with the [[addPartyAsync]] method
    * @param acsReader
    *   Pekko Source of ACS contracts streamed from the client
    * @return
    *   a request id that can be used to query for progress or errors via [[getAddPartyStatus]]
    */
  private[admin] def addPartyWithAcsAsync(
      args: PartyReplicationArguments,
      acsReader: Source[ActiveContract, NotUsed],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AddPartyRequestId] = {

    val PartyReplicationArguments(
      partyId,
      synchronizerId,
      sourceParticipantId,
      serial,
      participantPermission,
    ) = args

    executionQueue.executeEUS(
      for {
        _ <- EitherT.cond[FutureUnlessShutdown](
          syncService.isActive(),
          logger.info(
            s"Initiating import of party $partyId with ACS from participant $sourceParticipantId on synchronizer $synchronizerId"
          ),
          s"Participant $participantId is inactive",
        )
        connectedSynchronizer <-
          EitherT.fromEither[FutureUnlessShutdown](
            syncService
              .readyConnectedSynchronizerById(synchronizerId)
              .toRight(s"Unknown synchronizer $synchronizerId")
          )
        syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
        onboardingAt <- ensurePartyHostedBySourceAndOnboardingOnTargetParticipant(
          args,
          syncPersistentState.topologyStore,
        )
        requestId = buildRequestIdHash(args, syncPersistentState.pureCryptoApi)
        fileImporter = PartyReplicationFileImporter(
          partyId,
          requestId,
          onboardingAt,
          partyReplicationStateManager,
          syncService.participantNodePersistentState,
          connectedSynchronizer,
          acsReader,
          testInterceptorO,
          () => isClosing,
          loggerFactory,
        )
        // Check if this is a retry of a previously failed import
        existingStatusO = partyReplicationStateManager.get(requestId)

        _ <- existingStatusO match {
          case None =>
            // Brand new import
            val initialStatus = PartyReplicationStatus(
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
              agreementStatus = PartyReplicationStatus.AgreementStatus.NotNeeded,
              authorizationO = Some(
                PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared = false)
              ),
              replicationO = Some(AcsReplicationProgress.initialize(fileImporter)),
            )
            partyReplicationStateManager.add(initialStatus)

          // Parameter equality check omitted because:
          // - Request ID authenticates all fields expect for participant permission.
          // - ensurePartyHostedBySourceAndOnboardingOnTargetParticipant catches permission mismatches.
          case Some(existingStatus) =>
            // Unfinished existing import -> Verify it's in a valid state and retry to finish it
            EitherT
              .cond[FutureUnlessShutdown](
                !existingStatus.replicationO.exists(_.fullyProcessedAcs),
                (),
                s"ACS import on behalf of $requestId has already completed.",
              )
              .flatMap { _ =>
                partyReplicationStateManager.update_(
                  requestId,
                  _.modifyReplication {
                    case Some(_: AcsReplicationProgress) =>
                      logger.info(
                        "Restart the ACS import from the beginning, so reset the progress from the previous call."
                      )
                      AcsReplicationProgress.initialize(fileImporter)
                    case other =>
                      logger.info(
                        s"Unexpectedly missing ACS import progress $other during retry. Re-initializing."
                      )
                      AcsReplicationProgress.initialize(fileImporter)
                  }.modifyErrorO(_ =>
                    None
                  ), // Clear previous errors so the state machine can advance!
                )
              }
        }

        _ <- fileImporter.importAcsSnapshot().leftSemiflatMap { err =>
          partyReplicationStateManager
            .update(
              requestId,
              _.modifyErrorO { prevErrorO =>
                prevErrorO.foreach(prevError =>
                  logger.warn(
                    s"Party replication $requestId encountered error $err overwriting unexpected previous error $prevError"
                  )
                )
                Some(PartyReplicationFailed(err))
              },
            )
            .map(_ => err) // Return the error, not the updated status
            .merge
        }

      } yield {
        logger.info(s"Adding party $partyId with ACS request $requestId is in progress")
        activateProgressMonitoring(requestId)
        requestId
      },
      s"add party ${args.partyId} on ${args.synchronizerId}",
    )
  }

  /** Checks that the party is
    *   - hosted by the source participant
    *   - hosted by the target participant with onboarding flag set
    *   - serial matches head authorized topology
    *
    * Called only at source participant.
    */
  private def ensurePartyHostedBySourceAndOnboardingOnTargetParticipant(
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, EffectiveTime] =
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
      activeParticipantsOfParty = partyToParticipantTopologyHeadTx.mapping.participants
      eligibleSourceParticipants = activeParticipantsOfParty.collect {
        case HostingParticipant(pid, _, isOnboarding)
            if pid != targetParticipantId && !isOnboarding =>
          pid
      }
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          eligibleSourceParticipants.contains(sourceParticipantId),
          (),
          s"Party $partyId is not hosted by source participant $sourceParticipantId. Only hosted on ${activeParticipantsOfParty
              .mkString(",")}",
        )
      )
      onboarding = true
      _ <- EitherT.cond[FutureUnlessShutdown](
        activeParticipantsOfParty.contains(
          HostingParticipant(targetParticipantId, participantPermission, onboarding)
        ),
        (),
        // TODO(#30328): Add resilience to the operator forgetting to set onboarding on the TP.
        s"Party $partyId is not marked as onboarding with permission $participantPermission on the target participant $targetParticipantId. Hosted on ${activeParticipantsOfParty
            .mkString(",")}",
      )
    } yield partyToParticipantTopologyHeadTx.validFrom

  /** Checks that the party is
    *   - hosted by the source participant
    *   - hosted by the target participant with onboarding flag set
    *   - serial matches head authorized topology
    *
    * Called only at source participant.
    */
  private def ensurePartyHostedBySourceAndOnboardingOnTargetParticipant(
      args: PartyReplicationArguments,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, EffectiveTime] = {
    val PartyReplicationArguments(
      partyId,
      _,
      sourceParticipantId,
      serial,
      participantPermission,
    ) = args
    val targetParticipantId = participantId

    ensurePartyHostedBySourceAndOnboardingOnTargetParticipant(
      partyId,
      sourceParticipantId,
      targetParticipantId,
      serial,
      participantPermission,
      topologyStore,
    )
  }

  private[admin] def generatePartyTopologyUpdate(
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      targetParticipantId: ParticipantId,
      permission: ParticipantPermission,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyTransaction[Replace, PartyToParticipant]] =
    executionQueue.executeEUS(
      for {
        connectedSynchronizer <- EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight(s"Unknown synchronizer $synchronizerId")
        )

        topologyStore = connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore
        headTx <- topologyWorkflow.partyToParticipantTopologyHead(partyId, topologyStore)

        currentParticipants = headTx.mapping.participants
        _ <- EitherT.cond[FutureUnlessShutdown](
          !currentParticipants.exists(_.participantId == targetParticipantId),
          (),
          s"Target participant $targetParticipantId is already hosting party $partyId",
        )

        newParticipants = currentParticipants :+ HostingParticipant(
          targetParticipantId,
          permission,
          onboarding = true,
        )

        newMapping <- EitherT.fromEither[FutureUnlessShutdown](
          PartyToParticipant.create(
            partyId = headTx.mapping.partyId,
            threshold = headTx.mapping.threshold,
            participants = newParticipants,
            partySigningKeysWithThreshold = headTx.mapping.partySigningKeysWithThreshold,
            isOffline = false,
          )
        )

        nextSerial <- EitherT.fromEither[FutureUnlessShutdown](
          headTx.serial.increment.leftMap(_.message)
        )
        protocolVersion = connectedSynchronizer.staticSynchronizerParameters.protocolVersion
        newTx <- EitherT.fromEither(
          TopologyTransaction.create(
            op = TopologyChangeOp.Replace,
            serial = nextSerial,
            mapping = newMapping,
            protocolVersion = protocolVersion,
          )
        )
      } yield newTx,
      s"generate topology update for $partyId to $targetParticipantId",
    )

  private[admin] def authorizePartyUpdate(
      synchronizerId: SynchronizerId,
      transaction: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      signatures: Seq[TopologyTransactionSignature],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    executionQueue.executeEUS(
      for {
        psid <- EitherT.fromOption[FutureUnlessShutdown](
          syncService.activePsidForLsid(synchronizerId),
          s"Node is not connected to synchronizer $synchronizerId",
        )

        participantId = syncService.participantId

        onboardingParticipants = transaction.mapping.participants.filter(_.onboarding)
        _ <- EitherT.cond[FutureUnlessShutdown](
          onboardingParticipants.nonEmpty,
          (),
          s"The topology transaction must contain at least one onboarding participant.",
        )
        targetParticipantIds = onboardingParticipants.map(_.participantId).toSet

        connectedSynchronizer <- EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight(s"Unknown synchronizer $synchronizerId")
        )

        partyId = transaction.mapping.partyId
        topologyStore = connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore
        headTx <- topologyWorkflow.partyToParticipantTopologyHead(partyId, topologyStore)

        alreadyHosted = targetParticipantIds.filter(targetId =>
          headTx.mapping.participants.exists(p => p.participantId == targetId && !p.onboarding)
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          alreadyHosted.isEmpty,
          (),
          s"Party $partyId is already hosted on the target participant(s): ${alreadyHosted.mkString(", ")}.",
        )

        topologyManager <- EitherT.fromOption[FutureUnlessShutdown](
          syncService.lookupTopologyManager(psid),
          s"Topology manager not found for synchronizer $synchronizerId",
        )

        // Delegate to Topology Manager (No assumptions about internal vs. external or full authorization)
        _ <- NonEmpty.from(signatures.toSet) match {
          case Some(signaturesNE) =>
            for {
              signedTx <- EitherT.fromEither[FutureUnlessShutdown](
                SignedTopologyTransaction
                  .create(
                    transaction,
                    signaturesNE,
                    isProposal = true,
                    psid.protocolVersion,
                  )
              )

              // Extend the signature with the participant's own key if applicable
              extendedTx <- topologyManager
                .extendSignature(
                  signedTx,
                  signingKeys = Seq.empty,
                  namespacesToSignFor = Seq.empty,
                  forceFlags = ForceFlags.none,
                )
                .leftMap(error => s"Failed to append participant signature: $error")

              // Submit the transaction. By using expectFullAuthorization = false, we let
              // the TopologyStateProcessor automatically evaluate if the combined signatures
              // satisfy the authorization requirements and strip the proposal flag if they do.
              _ <- topologyManager
                .add(
                  Seq(extendedTx),
                  forceChanges = ForceFlags.none,
                  expectFullAuthorization = false,
                )
                .leftMap(error => s"Topology manager rejected the transaction: $error")
            } yield ()

          case None =>
            topologyManager
              .proposeAndAuthorize(
                op = transaction.operation,
                mapping = transaction.mapping,
                serial = Some(transaction.serial),
                signingKeys = Seq.empty,
                namespacesToSignFor = Seq.empty,
                protocolVersion = psid.protocolVersion,
                expectFullAuthorization = false,
                forceChanges = ForceFlags.none,
                waitToBecomeEffective = None,
              )
              .leftMap(err => s"Failed to propose and authorize topology transaction: $err")
              .map(_ => ())
        }

      } yield {
        logger.info(s"Authorized party update for $partyId on participant $participantId")
      },
      "authorize party update",
    )

  /** Checks that the party is
    *   - hosted by the source participant
    *   - not yet hosted by the target participant, but can be proposed to be with the provided
    *     serial
    *
    * Called only at target participant.
    */
  private def ensurePartyHostedBySourceButNotTargetParticipant(
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      topologyStore: TopologyStore[SynchronizerStore],
      serial: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, ParticipantId] =
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
      activeParticipantsOfParty = partyToParticipantTopologyHeadTx.mapping.participants.collect {
        case HostingParticipant(participantId, _, false) => participantId
      }
      participantsExceptTargetParticipant = activeParticipantsOfParty.filterNot(
        _ == targetParticipantId
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          participantsExceptTargetParticipant.contains(sourceParticipantId),
          (),
          s"Party $partyId is not hosted by source participant $sourceParticipantId. Only hosted on ${activeParticipantsOfParty
              .mkString(",")}",
        )
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        !activeParticipantsOfParty.contains(targetParticipantId),
        (),
        s"Party $partyId is already hosted by target participant $targetParticipantId",
      )
      expectedSerial <- EitherT.fromEither(
        partyToParticipantTopologyHeadTx.transaction.serial.increment.leftMap(_.message)
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        serial == expectedSerial,
        (),
        s"Specified serial $serial does not match the expected serial $expectedSerial add $partyId to $targetParticipantId.",
      )
    } yield sourceParticipantId

  // TODO(#35267) reflect this in the PartyReplicationStatus?
  private def startAcsReplication(
      replicationParams: ReplicationParams
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- acsReplicator.replicateAcsAsync(
        PartyReplicationArguments(
          replicationParams.partyId,
          replicationParams.synchronizerId,
          replicationParams.sourceParticipantId,
          replicationParams.serial,
          replicationParams.participantPermission,
        ),
        replicationParams.requestId,
      )
    } yield ()

  private def authorizeOnboardingTopology(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnectedWithChannelSupport(requestId) {
      case (
            PartyReplicationStatus(params, _, None, _, _, _, _, _),
            connectedSynchronizer,
            _,
          ) =>
        for {
          authorizedAtO <- topologyWorkflow.authorizeOnboardingTopology(
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
            partyReplicationStateManager.update_(
              requestId,
              _.setAuthorization(
                PartyReplicationAuthorization(authorizedAt, isOnboardingFlagCleared = false)
              ).setTopologySerial(topologySerial),
            )
          }
        } yield ()
    }

  private def transitionToIndexing(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId, "transition to indexing") {
      // TODO(#35267) separate file-based and sequencer channel based replication better
      case (
            PartyReplicationStatus(
              _,
              _,
              Some(PartyReplicationAuthorization(_, false)),
              Some(acsReplicationProgressCompleted),
              _,
              None, // not yet indexing
              false, // not completed
              _,
            ),
            _,
          ) if acsReplicationProgressCompleted.fullyProcessedAcs =>
        partyReplicationStateManager.update_(requestId, _.setIndexing())
    }

  private def progressIndexing(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId, "progress indexing") {
      case (
            PartyReplicationStatus(
              params,
              _,
              _,
              _,
              _,
              Some(indexingProgress),
              false, // not completed
              None,
            ),
            connectedSynchronizer,
          ) =>
        val indexingStore =
          connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
            .getOrElse(throw new IllegalStateException("Expect store when OnPR enabled"))
        val recordOrderPublisher = connectedSynchronizer.ephemeral.recordOrderPublisher
        val pureCrypto = connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi

        for {
          progress <- EitherT.right[String](
            indexingWorkflow.indexNextContractActivationChangeBatch(
              params.partyId.toLf,
              params.synchronizerId,
              indexingProgress,
              indexingStore,
              recordOrderPublisher,
              pureCrypto,
            )
          )
          _ <- partyReplicationStateManager.update_(requestId, _.updateIndexing(progress))
        } yield ()
    }

  /** This completes party replication by executing the following final steps if they are found to
    * not have been executed yet:
    *
    *   - Clear the onboarding flag from the target participant in the PartyToParticipant topology.
    *   - Archive the party replication agreement Daml contract to inform the SP that the TP no
    *     longer needs help replicating the party's ACS.
    */
  private def finishPartyReplication(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId, "finishing party replication") {
      case (
            previous @ PartyReplicationStatus(
              params,
              agreementO,
              Some(PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared)),
              _,
              acsReplicationStatusO,
              _,
              false, // not completed
              None,
            ),
            connectedSynchronizer,
          ) =>
        for {
          isAgreementArchived <- EitherT.rightT[FutureUnlessShutdown, String] {
            (agreementO, acsReplicationStatusO) match {
              case (AgreementStatus.NotNeeded | AgreementStatus.Archived, _) => true
              case (_, Some(acsReplicationStatus))
                  if acsReplicationStatus.agreementStatus == AgreementStatus.Archived =>
                true
              case _ => false
            }
          }
          isOnboardingFlagVerifiedCleared <-
            if (!isOnboardingFlagCleared)
              topologyWorkflow.authorizeClearingOnboardingFlag(
                params,
                onboardingAt,
                connectedSynchronizer,
              )
            else EitherT.rightT[FutureUnlessShutdown, String](false)

          statusUpdates = {
            def statusUpdate(
                condition: Boolean,
                update: PartyReplicationStateManager.Modification,
            ): Seq[PartyReplicationStateManager.Modification] =
              if (condition) Seq(update) else Seq.empty

            statusUpdate(isAgreementArchived, _.setAgreementStatus(AgreementStatus.Archived))
              ++ statusUpdate(
                isOnboardingFlagVerifiedCleared,
                _.setAuthorization(
                  PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared = true)
                ),
              )
              ++ statusUpdate(
                agreementO.isEmpty && (isOnboardingFlagVerifiedCleared || isOnboardingFlagCleared),
                _.setCompleted(),
              )
          }

          // If pausing the indexer, delete the items from the party replication indexing store since all contract
          // activation changes have been indexed.
          _ <- EitherTUtil.ifThenET(config.target.pauseSynchronizerIndexingDuringPartyReplication)(
            EitherT.right[String](
              connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
                .traverse(_.purgeContractActivationChanges())
            )
          )

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
              partyReplicationStateManager.update(requestId, combinedModification)
            } else EitherT.rightT[FutureUnlessShutdown, String](previous)
        } yield {
          if (status.hasCompleted) {
            logger.info(s"Party replication $requestId has completed")
          } else {
            logger.debug(
              s"Party replication $requestId not yet completed. AgreementArchived $isAgreementArchived, PartyOnboarded $isOnboardingFlagVerifiedCleared."
            )
          }
        }
    }

  /** Asynchronously execute the provided code block and handle the result with a custom handler.
    * The custom "handleResult" handler allows deviating from the default error handling such as
    * when the SP rejects a TP-proposed party replication.
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

  /** Asynchronously execute the provided code block reflecting any returned "left" in the error
    * status.
    */
  private def executeAsync(requestId: AddPartyRequestId, operation: String)(
      code: => EitherT[FutureUnlessShutdown, String, Unit]
  )(implicit traceContext: TraceContext): Unit = {
    def recordIfError(requestId: AddPartyRequestId)(
        resultET: EitherT[FutureUnlessShutdown, String, Unit]
    ): FutureUnlessShutdown[Unit] = resultET.leftSemiflatMap { err =>
      partyReplicationStateManager
        .update(
          requestId,
          _.modifyErrorO { prevErrorO =>
            prevErrorO.foreach(prevError =>
              logger.warn(
                s"Party replication $requestId has unexpectedly encountered error after previous error $prevError. Ignoring new error: $err"
              )
            )
            Some(PartyReplicationFailed(err))
          },
        )
        .fold(updateErr => logger.warn(s"$updateErr: $err"), _ => logger.warn(err))
    }.merge

    executeAsyncWithCustomResultHandling(requestId, s"$operation $requestId")(code)(recordIfError)
  }

  /** Activates progress scheduling once a new party replication request is received unless already
    * active.
    */
  private def activateProgressMonitoring(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit = {
    val previouslyActive = progressSchedulingActive.getAndSet(true)
    if (previouslyActive) {
      logger.info(s"Progress scheduling already active, so no need to activate for $requestId.")
    } else {
      logger.info(s"Activating progress scheduling for party replication $requestId.")
      scheduleExecuteAsync(progressSchedulingInterval)(progressPartyReplications())
    }
  }

  /** Single point of entry for progress monitoring and advancing of party replication states for
    * those states that are driven by the party replicator.
    */
  private def progressPartyReplications()(implicit traceContext: TraceContext): Unit = {
    val activePartyReplications = partyReplicationStateManager.collect {
      case (requestId, status) if status.isProgressExpected => requestId
    }

    if (activePartyReplications.isEmpty) {
      logger.info("No party replication progress to monitor, deactivating progress scheduling.")
      progressSchedulingActive.set(false)
    }
    // Check if any OnPR work is currently running and back off if it is to avoid eagerly queuing
    // obsolete state transitions.
    else {
      if (!executionQueue.isEmpty) {
        logger.debug(
          s"Skipping advancing party replication progress because still busy with ${executionQueue.queued
              .mkString(", ")}."
        )
      } else {
        // In case the set of requestIds has changed (particularly if grown) since "activePartyReplications"
        // has been read above, we will pick it up on the next invocation scheduled below.
        activePartyReplications.foreach(progressPartyReplication)
      }
      // Schedule the next time to progress-check party replications asynchronously, i.e. not recursively.
      scheduleExecuteAsync(progressSchedulingInterval)(progressPartyReplications())
    }
  }

  private def progressPartyReplication(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(requestId, s"progress party replication $requestId")(
      partyReplicationStateManager
        .get(requestId)
        .flatMap(PartyReplicationStage.fromPartyReplicationStatus)
        .fold(EitherTUtil.unitUS[String]) {
          // Stages listed in order of occurrence
          // Note that in file-based OnPR, the onboarding authorization is obtained before
          // involving the TP. Therefore, this stage is specific to SequencerChannel-based OnPR.
          case ObtainingOnboardingTopologyAuthorization(params) =>
            logger.debug(s"Authorizing party replication $requestId topology")
            authorizeOnboardingTopology(params.requestId)

          // Specific to SequencerChannel-based OnPR. About to trigger ACS replication via SequencerChannel
          case NeedsToReplicatePartyAcs(params) =>
            logger.debug(
              s"Starting ACS replication for party replication $requestId of party ${params.partyId}"
            )
            startAcsReplication(params)

          // Specific to SequencerChannel-based OnPR. Replication has been triggered but the transfer hasn't started yet
          case TriggeredPartyAcsReplication(params) =>
            logger.debug(
              s"ACS replication $requestId for party ${params.partyId} via sequencer channel was triggered but hasn't started yet"
            )
            EitherTUtil.unitUS

          // Shared between file-based and SequencerChannel-based replication. ACS replication is in progress
          case AcsReplicationInProgress(p, progress) =>
            if (progress.fullyProcessedAcs) {
              logger.debug(
                s"Party replication $requestId has finished replicating all ${progress.processedContractCount} contracts for ${p.partyId}."
              )
              for {
                // Ensure the PartyReplicator has the latest AcsReplicator progress, so that
                // it doesn't get the impression that indexing started before ACS replication happened
                // which can flakily happen e.g. if the ACS is empty.
                _ <- partyReplicationStateManager.updateAcsReplicationProgress(requestId, progress)
                _ <- transitionToIndexing(requestId)
              } yield ()
            } else {
              progress match {
                case _: EphemeralSequencerChannelProgress =>
                  for {
                    newProgress <- EitherT.fromOption[FutureUnlessShutdown](
                      acsReplicator.acsReplicationStateManager.getAcsReplicationProgress(requestId),
                      "No ACS replication progress found",
                    )
                    _ <- partyReplicationStateManager
                      .updateAcsReplicationProgress(requestId, newProgress)
                  } yield {
                    logger.debug(
                      s"Party replication $requestId has replicated ${newProgress.processedContractCount} contracts for ${p.partyId}. Progress driven by processor."
                    )
                  }
                case _: EphemeralFileImporterProgress =>
                  logger.debug(
                    s"Party replication $requestId has replicated ${progress.processedContractCount} contracts for ${p.partyId}. Progress driven by file importer."
                  )
                  // The file importer is self-paced and does not need to be pinged to make progress unlike the sequencer processors
                  EitherTUtil.unitUS
                case replicationNonRuntime: PersistentProgress =>
                  // TODO(#29498): As part of TP-resilience to restart and crash recovery, rebuild target participant
                  //  processor once reconnected to synchronizer.
                  EitherT.leftT[FutureUnlessShutdown, Unit](
                    s"Party replication ${p.requestId} AcsReplicationProgress not in runtime state: $replicationNonRuntime"
                  )
              }
            }

          // ACS transfer has been finished. Indexing the replicated ACS.
          case IndexingContractActivationChanges(params) =>
            logger.debug(
              s"Indexing replicated ACS during party replication $requestId of party ${params.partyId}..."
            )
            progressIndexing(requestId)

          case CleaningUp(params) =>
            logger.debug(
              s"Finishing party replication $requestId of party ${params.partyId}..."
            )
            finishPartyReplication(requestId)

          case IsInInvalidState(error) =>
            EitherT.leftT[FutureUnlessShutdown, Unit](error.message)
          case invalid => EitherT.leftT[FutureUnlessShutdown, Unit](s"Invalid status: $invalid")
        }
    )

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
      "party replicator progress scheduling",
    )
  }

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
      requestId: AddPartyRequestId
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
        AddPartyError.Other
          .Failure(requestId, s"Stopping as participant $participantId is inactive"),
      )
      status <- EitherT.fromEither[FutureUnlessShutdown](
        partyReplicationStateManager
          .get(requestId)
          .toRight(AddPartyError.Other.Failure(requestId, s"Unknown request id $requestId"))
      )
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(status.params.synchronizerId)
            .toRight(
              AddPartyError.DisconnectedFromSynchronizer.Failure(
                requestId,
                status.params.synchronizerId,
                s"Synchronizer ${status.params.synchronizerId} not connected during $status",
              )
            )
        )
      channelClient <- EitherT.fromEither[FutureUnlessShutdown](
        connectedSynchronizer.synchronizerHandle.sequencerChannelClientO.toRight(
          AddPartyError.Other.Failure(
            requestId,
            s"Synchronizer ${status.params.synchronizerId} does not expose necessary sequencer channel client",
          )
        )
      )
      _ <- matchIfStateIsAsExpected
        .lift((status, connectedSynchronizer, channelClient))
        .getOrElse(
          EitherT.leftT[FutureUnlessShutdown, Unit](
            s"Unexpected status while ensuring synchronizer is connected with channel support) $status"
          )
        )
        .leftMap(AddPartyError.Other.Failure(requestId, _): AddPartyError)
    } yield ()

    // Skip processing if the participant is not connected to the synchronizer instead of returning an error.
    stateET.leftFlatMap {
      case AddPartyError.DisconnectedFromSynchronizer.Failure(requestId, synchronizerId, err) =>
        logger.info(
          s"Party replication $requestId disconnected from synchronizer $synchronizerId. Waiting until reconnect: $err"
        )
        EitherTUtil.unitUS
      case err: AddPartyError.Other.Failure =>
        EitherT.leftT[FutureUnlessShutdown, Unit](err.cause)
    }
  }

  private def ensureParticipantStateAndSynchronizerConnected(
      requestId: AddPartyRequestId,
      operation: String,
  )(
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
      partyReplicationStateManager
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
          s"Unexpected status while ensuring synchronizer is connected) $status during operation: $operation"
        )
      )
  } yield ()

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

  override protected def onClosed(): Unit = {
    def getProcessors: Seq[AutoCloseable] =
      partyReplicationStateManager.collect[AcsReplicationProcessor] {
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
      executionQueue +: topologyWorkflow +: getProcessors :+ partyReplicationStateManager
    )(logger)
  }
}

object PartyReplicator {
  type AddPartyRequestId = Hash

  final case class PartyReplicationArguments(
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  )

  lazy val defaultProgressSchedulingInterval: PositiveFiniteDuration =
    PositiveFiniteDuration.ofSeconds(1L)
}
