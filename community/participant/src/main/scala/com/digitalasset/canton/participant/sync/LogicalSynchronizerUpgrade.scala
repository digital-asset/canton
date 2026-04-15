// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{
  CantonTimestamp,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LifeCycleContainer
import com.digitalasset.canton.participant.admin.data.{
  LateLsuRequest as AdminLateLsuRequest,
  ManualLsuRequest as AdminManualLsuRequest,
}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.UnknownPsid
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SyncPersistentState,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.CheckedLogicalSynchronizerUpgrade.UpgradabilityCheckResult
import com.digitalasset.canton.participant.sync.LogicalSynchronizerUpgrade.{
  AutomaticLsuRequest,
  LateLsuRequest,
  LsuRequest,
  NegativeResult,
  OfflineManualLsuRequest,
  OnlineManualLsuRequest,
}
import com.digitalasset.canton.participant.synchronizer.{
  PendingLsuOperation,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot
import com.digitalasset.canton.topology.store.NoPackageDependencies
import com.digitalasset.canton.topology.transaction.{
  GrpcConnection,
  LsuAnnouncement,
  LsuSequencerConnectionSuccessor,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{
  KnownPhysicalSynchronizerId,
  Lsu,
  PhysicalSynchronizerId,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.retry.Backoff
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue}
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import io.scalaland.chimney.dsl.*

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.chaining.*

/** Performs the upgrade from one physical synchronizer to its successor. The final step is to mark
  * the successor configuration as active.
  */
sealed trait LogicalSynchronizerUpgrade[Req <: LsuRequest] extends NamedLogging with FlagCloseable {

  /** Sequential execution queue on which actions must be run. This queue is shared with the
    * CantonSyncService, which uses it for synchronizer connections. Sharing it ensures that we
    * cannot connect to the synchronizer while an upgrade action is running and vice versa.
    *
    * The use of the shared execution queue implies the following:
    *   - It introduces the limitation that a single upgrade can be run at a time. This is a
    *     reasonable limitation for now considering the frequency of upgrades.
    *   - Since upgrades should be quick, not allowing reconnects to other synchronizers is
    *     reasonable as well.
    */
  def executionQueue: SimpleExecutionQueue

  def connectedSynchronizersLookup: ConnectedSynchronizersLookup
  def synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore

  /** Function to disconnect to a synchronizer. Needs to be synchronized using the `executionQueue`.
    */
  def disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
    FutureUnlessShutdown,
    SyncServiceError,
    Unit,
  ]

  def kind: String

  // Retry policy for the readiness check and upgrade
  protected val retryPolicy: Backoff = Backoff(
    logger = logger,
    hasSynchronizeWithClosing = this,
    maxRetries = Int.MaxValue,
    initialDelay = LogicalSynchronizerUpgrade.RetryInitialDelay,
    maxDelay = LogicalSynchronizerUpgrade.RetryMaxDelay,
    operationName = "lsu",
  )

  /** Run the operation using the specified retry strategy. The reason we have a more complicated
    * logic is that we schedule many operations on a `executionQueue` (that is also used for
    * synchronizer connections) and any failed task switches the queue to `failure` mode which
    * prevents other operations to be scheduled. Hence, we avoid signalling failed operations as a
    * failed future.
    *
    * Behavior depending on the result of `operation`:
    *   - Failed future: the failed future bubbles up. Note that if the task is scheduled on the
    *     `executionQueue`, the queue will be on failure mode.
    *   - Left: will lead to a retry if the NegativeResult is retryable. If not, the error will be
    *     returned.
    *   - Right: result is returned.
    */
  protected def runWithRetries[T](operation: => FutureUnlessShutdown[Either[NegativeResult, T]])(
      implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Either[String, T]] =
    retryPolicy
      .unlessShutdown(
        operation.map {
          case Left(NegativeResult(details, isRetryable)) =>
            if (isRetryable)
              Left[String, Either[String, T]](details)
            else
              Right[String, Either[String, T]](Left(details))

          case Right(success) =>
            Right[String, Either[String, T]](Right(success))
        },
        DbExceptionRetryPolicy,
      )
      .map(_.flatten)

  /** Performs the upgrade. Fails if the upgrade cannot be performed.
    *
    * Prerequisite:
    *   - Node is disconnected from the synchronizer
    *   - See prerequisites for each implementation
    */
  protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Run `operation` only if connectivity to `lsid` matches `shouldBeConnected`.
    */
  protected def enqueueOperation[T](
      lsid: SynchronizerId,
      operation: => FutureUnlessShutdown[Either[NegativeResult, T]],
      description: String,
      shouldBeConnected: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[NegativeResult, T]] =
    executionQueue.executeUS(
      {
        val isConnected = connectedSynchronizersLookup.isConnected(lsid)
        val text = if (isConnected) "" else "not "

        if (isConnected == shouldBeConnected) {
          logger.info(s"Scheduling $description")
          operation
        } else {
          logger.info(s"Not scheduling $description because the node is ${text}connected to $lsid")
          FutureUnlessShutdown.pure(
            Left(
              NegativeResult(
                s"Not scheduling $description because the node is ${text}connected to $lsid",
                isRetryable = true,
              )
            )
          )
        }
      },
      description,
    )

  /** Runs `f` if the upgrade was not done yet.
    */
  protected def performIfNotUpgradedYet[E](
      successorPsid: PhysicalSynchronizerId
  )(
      f: => EitherT[FutureUnlessShutdown, E, Unit],
      operation: String,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, E, Unit] =
    synchronizerConnectionConfigStore.get(successorPsid) match {
      case Right(
            StoredSynchronizerConnectionConfig(_, SynchronizerConnectionConfigStore.Active, _, _)
          ) =>
        logger.info(s"Not running $operation as the successor is already marked as active")
        EitherT.pure(())
      case _ =>
        logger.info(s"Running $operation")
        f
    }

  /** This method ensures that the node is disconnected from the synchronizer before performing the
    * upgrade.
    */
  protected def performUpgrade(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = {
    val lsid = currentPsid.logical

    performIfNotUpgradedYet(request.successorPsid)(
      for {
        _disconnect <- disconnectSynchronizer(Traced(alias)).leftMap(err =>
          NegativeResult(err.toString, isRetryable = true)
        )

        _ <- EitherT(
          enqueueOperation(
            lsid,
            operation = performUpgradeInternal(
              alias,
              currentPsid = currentPsid,
              request,
            ).leftMap { error =>
              /*
                Because preconditions are checked, a failure of the upgrade is not something that can be automatically
                recovered from (except DB exceptions).
               */
              NegativeResult(
                s"Unable to upgrade $currentPsid to ${request.successorPsid}. Not retrying. Cause: $error",
                isRetryable = false,
              )
            }.value,
            description = s"$kind-lsu-upgrade",
            shouldBeConnected = false,
          )
        )
      } yield (),
      operation = s"$kind upgrade from $currentPsid to ${request.successorPsid}",
    )
  }

  /** Attempt to prepare the new synchronizer connection config
    *   - Compute the new list of new sequencer connections
    *   - If the list has enough elements (more than the sequencer threshold), register the
    *     synchronizer.
    *
    * Prerequisite:
    *   - Successor was not already registered
    *
    * @return
    *   Current config as well as new sequencer connections
    */
  protected def prepareNewSynchronizerConnectionConfig(
      sequencerSuccessors: Map[SequencerId, LsuSequencerConnectionSuccessor],
      currentSyncPersistentState: SyncPersistentState,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    NegativeResult,
    (StoredSynchronizerConnectionConfig, SequencerConnections),
  ] =
    for {
      currentConfig <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerConnectionConfigStore
          .get(currentSyncPersistentState.psid)
          .leftMap(err => NegativeResult(err.message, isRetryable = false))
      )
      currentSequencerConnections = currentConfig.config.sequencerConnections

      _ = logger.info(
        s"Trying to register successor config. Known sequencers are ${currentSequencerConnections.aliasToConnection.keySet} with threshold ${currentSequencerConnections.sequencerTrustThreshold}"
      )

      (withoutId, withoutSuccessor, newConnections) = currentSequencerConnections.aliasToConnection
        .foldLeft(
          (
            List.empty[SequencerAlias],
            List.empty[SequencerId],
            List.empty[SequencerConnection],
          )
        ) {
          case ((withoutId, withoutSuccessor, successorConnections), (alias, currentConnection)) =>
            currentConnection.sequencerId match {
              case Some(sequencerId) =>
                sequencerSuccessors.get(sequencerId) match {
                  case Some(successor) =>
                    val newConnection = successor.toGrpcSequencerConnection(alias)
                    (withoutId, withoutSuccessor, newConnection +: successorConnections)

                  case None => (withoutId, sequencerId +: withoutSuccessor, successorConnections)
                }

              case None => (alias +: withoutId, withoutSuccessor, successorConnections)
            }
        }

      _ = logger.info(
        s"Sequencers without known id: $withoutId. Successors without known successor: $withoutSuccessor. Successors known for: ${newConnections
            .map(_.sequencerAlias)}"
      )

      newConnectionsNE <- EitherT.fromOption[FutureUnlessShutdown](
        NonEmpty.from(newConnections),
        NegativeResult("No sequencer successor was found", isRetryable = false),
      )

      newSequencerConnections <-
        if (newConnectionsNE.sizeIs >= currentSequencerConnections.sequencerTrustThreshold.unwrap) {
          EitherT.fromEither[FutureUnlessShutdown](
            currentSequencerConnections
              .modifyConnections(newConnectionsNE)
              .leftMap(err =>
                NegativeResult(
                  s"Unable to build new sequencer connections: $err",
                  isRetryable = false,
                )
              )
          )
        } else {
          EitherT.leftT[FutureUnlessShutdown, SequencerConnections](
            NegativeResult(
              s"Not enough successors sequencers (${newConnections.size}) to meet the sequencer threshold (${currentSequencerConnections.sequencerTrustThreshold})",
              isRetryable = false,
            )
          )
        }

      lostConnections = currentSequencerConnections.aliasToConnection.keySet.diff(
        newSequencerConnections.aliasToConnection.keySet
      )

      _ = if (lostConnections.nonEmpty) {
        logger.warn(
          s"Missing successor information for the following sequencers: $lostConnections. They will be removed from the pool of sequencers."
        )
      }
    } yield (currentConfig, newSequencerConnections)

  /** Attempt to register the successor synchronizer
    *
    * Prerequisite:
    *   - Successor was not already registered
    */
  protected def attemptSuccessorSynchronizerRegistration(
      currentPsid: PhysicalSynchronizerId,
      successorPsid: PhysicalSynchronizerId,
      upgradeTime: CantonTimestamp,
      currentConfig: StoredSynchronizerConnectionConfig,
      successorSequencerConnections: SequencerConnections,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = {
    logger.info("Trying to save the config of the successor")

    synchronizerConnectionConfigStore
      .put(
        config = currentConfig.config.copy(
          sequencerConnections = successorSequencerConnections,
          synchronizerId = Some(successorPsid),
        ),
        status = SynchronizerConnectionConfigStore.LsuTarget,
        configuredPsid = KnownPhysicalSynchronizerId(successorPsid),
        synchronizerPredecessor = Some(
          SynchronizerPredecessor(
            currentPsid,
            upgradeTime,
            isLateUpgrade = false,
          )
        ),
      )
      .leftMap(err =>
        NegativeResult(s"Unable to store new synchronizer connection: $err", isRetryable = false)
      )
  }

  def isUpgradeDone(successorPSId: PhysicalSynchronizerId): Boolean =
    synchronizerConnectionConfigStore
      .get(successorPSId)
      .fold(
        _ => false, // successor is not registered yet
        _.status match {
          case SynchronizerConnectionConfigStore.Active |
              SynchronizerConnectionConfigStore.Inactive |
              SynchronizerConnectionConfigStore.HardMigratingSource |
              SynchronizerConnectionConfigStore.LsuSource =>
            true

          case SynchronizerConnectionConfigStore.HardMigratingTarget |
              SynchronizerConnectionConfigStore.LsuTarget =>
            false
        },
      )
}

/** Contains methods that are used for both the [[AutomaticLogicalSynchronizerUpgrade]] amd
  * [[OnlineManualLogicalSynchronizerUpgrade]]. Unlike [[UncheckedLateLogicalSynchronizerUpgrade]],
  * both automatic and manual lsu perform a few checks before doing the migration.
  *
  * @tparam Req
  *   Type parameter for the request.
  */
trait CheckedLogicalSynchronizerUpgrade[Req <: LsuRequest] extends LogicalSynchronizerUpgrade[Req] {
  def ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer]

  def connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
    FutureUnlessShutdown,
    SyncServiceError,
    Option[PhysicalSynchronizerId],
  ]
  def pendingLsuOperationsStore: PendingLsuOperation.Store

  protected def upgrade(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val successorPsid = request.successorPsid
    implicit val logger = Lsu.Logger(loggerFactory, getClass, successorPsid)

    logger.info(s"Upgrade from $currentPsid to $successorPsid")

    val upgradabilityCheck =
      if (request.isOnline) onlineUpgradabilityCheck _ else offlineUpgradabilityCheck _

    // Ensure upgraded is not attempted if announcement was revoked
    performIfNotUpgradedYet(successorPsid)(
      for {
        upgradabilityCheckResult <- EitherT(
          runWithRetries(upgradabilityCheck(alias, currentPsid, request))
        )

        _ <- upgradabilityCheckResult match {
          case UpgradabilityCheckResult.ReadyToUpgrade =>
            logger.info(
              s"Upgrade from $currentPsid to $successorPsid is possible, starting internal upgrade"
            )

            EitherT.liftF[FutureUnlessShutdown, String, Unit](
              retryPolicy
                .unlessShutdown(
                  performUpgrade(
                    alias,
                    currentPsid,
                    request,
                  ).value,
                  DbExceptionRetryPolicy,
                )
                .map(_ => ())
            )

          case UpgradabilityCheckResult.UpgradeDone =>
            logger.info(s"Upgrade from $currentPsid to $successorPsid already done.")
            EitherTUtil.unitUS
        }

        _ <- connectSynchronizer(Traced(alias)).leftMap(_.toString)
      } yield (),
      operation = s"$kind upgrade from $currentPsid to $successorPsid",
    )
  }

  /** Check whether the upgrade can be done.
    *   - A left indicates that the upgrade cannot be done (yet). Retryability is encoded in
    *     NegativeResult.
    *   - A right indicates that the upgrade can be attempted or was already done.
    *
    * This method wraps the call to [[canBeUpgradedTo]] to ensure proper connectivity to the
    * synchronizer.
    *
    * Checks are done when the participant is connected to the synchronizer. This method is used
    * when the upgrade process has to wait for the processing to reach a certain state (clean
    * synchronizer index, ACS commitment processor, ...).
    */
  protected def onlineUpgradabilityCheck(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): FutureUnlessShutdown[Either[NegativeResult, UpgradabilityCheckResult]] = {
    val successorPsid = request.successorPsid
    val lsid = currentPsid.logical

    logger.info("Attempting online upgradability check")

    connectSynchronizer(Traced(alias)).value.flatMap {
      case Left(error) =>
        // Left will lead to a retry
        FutureUnlessShutdown.pure(
          NegativeResult(
            s"Failed to connect to $alias to perform upgradability check: $error",
            isRetryable = true,
          ).asLeft
        )

      case Right(Some(`successorPsid`)) =>
        FutureUnlessShutdown.pure(UpgradabilityCheckResult.UpgradeDone.asRight)

      case Right(_) =>
        enqueueOperation(
          lsid,
          canBeUpgradedTo(currentPsid, request).value,
          description = "lsu-online-upgradability-check",
          shouldBeConnected = true,
        )
    }
  }

  /** Check whether the upgrade can be done.
    *   - A left indicates that the upgrade cannot be done (yet). Retryability is encoded in
    *     NegativeResult.
    *   - A right indicates that the upgrade can be attempted or was already done.
    *
    * This method wraps the call to [[canBeUpgradedTo]] to ensure proper connectivity to the
    * synchronizer.
    *
    * Checks are done when the participant is not connected to the synchronizer. This is used in the
    * offline manual LSU where the latest clean synchronizer is picked as upgrade time.
    */
  protected def offlineUpgradabilityCheck(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): FutureUnlessShutdown[Either[NegativeResult, UpgradabilityCheckResult]] = {
    val lsid = currentPsid.logical

    logger.info("Attempting offline upgradability check")

    disconnectSynchronizer(Traced(alias)).value.flatMap {
      case Left(error) =>
        // Left will lead to a retry
        FutureUnlessShutdown.pure(
          NegativeResult(
            s"Failed to disconnect from $alias to perform upgradability check: $error",
            isRetryable = true,
          ).asLeft
        )

      case Right(_) =>
        enqueueOperation(
          lsid,
          canBeUpgradedTo(currentPsid, request).value,
          description = "lsu-offline-upgradability-check",
          shouldBeConnected = false,
        )
    }
  }

  protected def canBeUpgradedTo(
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult]

  /** Check whether the value of the clean synchronizer index is compatible with LSU.
    */
  protected def checkCleanSynchronizerIndex(
      lsid: SynchronizerId,
      upgradeTime: CantonTimestamp,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] =
    for {
      synchronizerIndex <- EitherT.fromOptionF(
        ledgerApiIndexer.asEval.value.ledgerApiStore.value.cleanSynchronizerIndex(lsid),
        NegativeResult(
          s"Unable to get synchronizer index for $lsid",
          isRetryable = true,
        ),
      )

      checkResultE =
        if (synchronizerIndex.recordTime < upgradeTime)
          NegativeResult(
            s"Synchronizer index is not yet at upgrade time: should be at $upgradeTime time but found ${synchronizerIndex.recordTime}",
            isRetryable = true,
          ).asLeft
        else if (synchronizerIndex.recordTime > upgradeTime)
          NegativeResult(
            s"Synchronizer index is past upgrade time: should not be higher than $upgradeTime time but found ${synchronizerIndex.recordTime}",
            isRetryable = false,
          ).asLeft
        else ().asRight

      _ <- EitherT.fromEither[FutureUnlessShutdown](checkResultE)
    } yield ()

  /** Performs the upgrade. Fails if the upgrade cannot be performed.
    *
    * Prerequisite:
    *   - Time on the current synchronizer has reached the upgrade time.
    *   - `canBeUpgradedTo` returns Right(`ReadyToUpgrade`)
    *   - Node is disconnected from the synchronizer
    */
  override protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    logger.info(s"Marking synchronizer connection $currentPsid as inactive")

    for {
      // Should have been checked before but this is cheap
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(currentPsid),
          SynchronizerConnectionConfigStore.LsuSource,
        )
        .leftMap(err => s"Unable to mark current synchronizer $currentPsid as inactive: $err")

      /*
      Handshake and topology copy are useless now: the node is doing the LSU regardless of the outcome.
      Potential issues will be flagged upon connection attempt to the synchronizer.
       */
      _ <- EitherT.rightT[FutureUnlessShutdown, String](
        pendingLsuOperationsStore.delete(
          currentPsid,
          PendingLsuOperation.operationKey,
          PendingLsuOperation.operationName,
        )
      )

      // Comes last to indicate that the node is ready to connect to the successor
      _ = logger.info(s"Marking synchronizer connection ${request.successorPsid} as active")
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(request.successorPsid),
          SynchronizerConnectionConfigStore.Active,
        )
        .leftMap(err =>
          s"Unable to mark successor synchronizer ${request.successorPsid} as active: $err"
        )
    } yield logger.info("Automatic upgrade was successful")
  }
}

protected object CheckedLogicalSynchronizerUpgrade {
  // Positive result
  sealed trait UpgradabilityCheckResult extends Product with Serializable
  object UpgradabilityCheckResult {
    final case object ReadyToUpgrade extends UpgradabilityCheckResult
    final case object UpgradeDone extends UpgradabilityCheckResult
  }
}

/** This class implements automatic LSU. It should be called for participants that are not upgrading
  * too late (after the old synchronizer has been decommissioned).
  *
  * @param connectSynchronizer
  *   Function to connect to a synchronizer. Needs to be synchronized using the `executionQueue`.
  */
class AutomaticLogicalSynchronizerUpgrade(
    override val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override val ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    syncPersistentStateManager: SyncPersistentStateManager,
    override val executionQueue: SimpleExecutionQueue,
    override val connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    override val connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CheckedLogicalSynchronizerUpgrade[AutomaticLsuRequest] {

  override def kind: String = "automatic"

  /** Attempt to perform the upgrade from `currentPsid` to `synchronizerSuccessor`.
    *
    * Prerequisites:
    *   - Time on `currentPsid` has reached `synchronizerSuccessor.upgradeTime`
    *   - Successor synchronizer is registered
    *
    * Strategy:
    *   - All failing operations that have a chance to succeed are retried.
    *
    * Note:
    *   - The upgrade involves operations that are retried, so the method can take some time to
    *     complete.
    */
  def upgrade(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val successorPsid = synchronizerSuccessor.psid
    implicit val logger = Lsu.Logger(loggerFactory, getClass, successorPsid)

    logger.info(s"Upgrade from $currentPsid to $successorPsid")

    // Ensure upgraded is not attempted if announcement was revoked
    def ensureUpgradeOngoing(): EitherT[FutureUnlessShutdown, String, Unit] = for {
      topologyStore <- EitherT.fromOption[FutureUnlessShutdown](
        syncPersistentStateManager.get(currentPsid).map(_.topologyStore),
        "Unable to find topology store",
      )

      announcements <- EitherT
        .liftF(
          topologyStore.findPositiveTransactions(
            asOf = synchronizerSuccessor.upgradeTime,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(TopologyMapping.Code.LsuAnnouncement),
            filterUid = None,
            filterNamespace = None,
          )
        )
        .map(_.collectOfMapping[LsuAnnouncement])
        .map(_.result.map(_.transaction.mapping))

      _ <- announcements match {
        case Seq() => EitherT.leftT[FutureUnlessShutdown, Unit]("No synchronizer upgrade ongoing")
        case Seq(head) =>
          EitherT.cond[FutureUnlessShutdown](
            head.successor == synchronizerSuccessor,
            (),
            s"Expected synchronizer successor to be $synchronizerSuccessor but found ${head.successor} in topology state",
          )
        case _more =>
          EitherT.liftF[FutureUnlessShutdown, String, Unit](
            FutureUnlessShutdown.failed(
              new IllegalStateException("Found several SynchronizerUpgradeAnnouncement")
            )
          )
      }
    } yield ()

    performIfNotUpgradedYet(successorPsid)(
      for {
        _ <- ensureUpgradeOngoing()
        _ <- super.upgrade(alias, currentPsid, AutomaticLsuRequest(synchronizerSuccessor))
      } yield (),
      operation = s"automatic upgrade from $currentPsid to $successorPsid",
    )
  }

  /** Finish the upgrade. Fails if the upgrade cannot be performed.
    *
    * Is called to finish an upgrade after a crash.
    *
    * Prerequisite:
    *   - Current/old connection was marked as LsuSource by this class. In particular, it means:
    *     - Time on the current synchronizer has reached the upgrade time.
    *     - `canBeUpgradedTo` returns Right(`ReadyToUpgrade`)
    *     - Node is disconnected from the synchronizer
    */
  def finishUpgradeWithoutChecks(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    implicit val logger = Lsu.Logger(loggerFactory, getClass, synchronizerSuccessor.psid)

    for {
      _disconnect <- disconnectSynchronizer(Traced(alias))
        .leftMap(err => NegativeResult(err.toString, isRetryable = true))
        .leftMap(_.details)

      _ <- enqueueOperation(
        currentPsid.logical,
        operation = performUpgradeInternal(
          alias,
          currentPsid = currentPsid,
          AutomaticLsuRequest(synchronizerSuccessor),
        ).leftMap { error =>
          /*
          Because preconditions are checked, a failure of the upgrade is not something that can be automatically
          recovered from (except DB exceptions).
           */
          NegativeResult(
            s"Unable to finish upgrade of $currentPsid to ${synchronizerSuccessor.psid}. Not retrying. Cause: $error",
            isRetryable = false,
          )
        }.value,
        description = s"finish-lsu-upgrade",
        shouldBeConnected = false,
      ).pipe(EitherT(_)).leftMap(_.details)
    } yield ()
  }

  /** Check whether the upgrade can be done. A left indicates that the upgrade cannot be done. A
    * right indicates that the upgrade can be attempted or was already done.
    */
  override protected def canBeUpgradedTo(
      currentPsid: PhysicalSynchronizerId,
      request: AutomaticLsuRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult] = {
    val upgradeTime = request.upgradeTime

    def runningCommitmentWatermarkCheck(
        runningCommitmentWatermark: CantonTimestamp
    ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] =
      if (runningCommitmentWatermark == upgradeTime)
        EitherT.rightT(())
      else if (runningCommitmentWatermark < upgradeTime)
        EitherT.leftT(
          NegativeResult(
            s"Running commitment watermark ($runningCommitmentWatermark) did not reach the upgrade time ($upgradeTime) yet",
            isRetryable = true,
          )
        )
      else
        EitherT.leftT(
          NegativeResult(
            s"Running commitment watermark ($runningCommitmentWatermark) is already past the upgrade time ($upgradeTime). Upgrade is impossible.",
            isRetryable = false,
          )
        )

    def upgradeCheck(): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = for {
      _ <- checkCleanSynchronizerIndex(currentPsid.logical, upgradeTime)

      currentSyncPersistentState <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .get(currentPsid)
          .toRight(
            NegativeResult(
              s"Unable to find persistent state for $currentPsid",
              isRetryable = false,
            )
          )
      )

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          synchronizerConnectionConfigStore.get(request.successorPsid).map(_ => ())
        )
        .orElse {
          val topologySnapshot = new StoreBasedTopologySnapshot(
            psid = currentSyncPersistentState.psid,
            timestamp = request.upgradeTime,
            store = currentSyncPersistentState.topologyStore,
            packageDependencyResolver = NoPackageDependencies,
            loggerFactory = loggerFactory,
          )

          for {
            successors <- EitherT.liftF(
              topologySnapshot.sequencerConnectionSuccessors(request.successorPsid)
            )
            currentConfigAndNewSequencers <- prepareNewSynchronizerConnectionConfig(
              successors,
              currentSyncPersistentState,
            )
            (currentConfig, successorSequencerConnections) = currentConfigAndNewSequencers
            _ <- attemptSuccessorSynchronizerRegistration(
              currentPsid,
              request.successorPsid,
              upgradeTime,
              currentConfig,
              successorSequencerConnections,
            )
          } yield ()
        }

      runningCommitmentWatermark <- EitherT.liftF(
        currentSyncPersistentState.acsCommitmentStore.runningCommitments.watermark.map(_.timestamp)
      )

      _ <- runningCommitmentWatermarkCheck(runningCommitmentWatermark)
    } yield ()

    if (isUpgradeDone(request.successorPsid))
      EitherT.pure[FutureUnlessShutdown, NegativeResult](UpgradabilityCheckResult.UpgradeDone)
    else upgradeCheck().map(_ => UpgradabilityCheckResult.ReadyToUpgrade)
  }
}

object ManualLogicalSynchronizerUpgrade {
  def upgrade(
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
      syncPersistentStateManager: SyncPersistentStateManager,
      executionQueue: SimpleExecutionQueue,
      connectedSynchronizersLookup: ConnectedSynchronizersLookup,
      connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
        FutureUnlessShutdown,
        SyncServiceError,
        Option[PhysicalSynchronizerId],
      ],
      disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
        FutureUnlessShutdown,
        SyncServiceError,
        Unit,
      ],
      pendingLsuOperationsStore: PendingLsuOperation.Store,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(
      alias: SynchronizerAlias,
      request: AdminManualLsuRequest,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    request.upgradeTime match {
      case Some(upgradeTime) =>
        new OnlineManualLogicalSynchronizerUpgrade(
          synchronizerConnectionConfigStore,
          ledgerApiIndexer,
          syncPersistentStateManager,
          executionQueue,
          connectedSynchronizersLookup,
          connectSynchronizer,
          disconnectSynchronizer,
          pendingLsuOperationsStore,
          timeouts,
          loggerFactory,
        ).upgrade(
          alias,
          request.currentPsid,
          request.into[OnlineManualLsuRequest].withFieldConst(_.upgradeTime, upgradeTime).transform,
        )

      case None =>
        new OfflineManualLogicalSynchronizerUpgrade(
          synchronizerConnectionConfigStore,
          ledgerApiIndexer,
          syncPersistentStateManager,
          executionQueue,
          connectedSynchronizersLookup,
          connectSynchronizer,
          disconnectSynchronizer,
          pendingLsuOperationsStore,
          timeouts,
          loggerFactory,
        ).upgrade(
          alias,
          request.currentPsid,
          request.into[OfflineManualLsuRequest].withFieldConst(_.alias, alias).transform,
        )
    }
}

/** Perform a manual logical synchronizer upgrade. This endpoint should ONLY be used when the
  * current synchronizer was declared irrevocably broken.
  *
  * The upgrade process will wait until the clean synchronizer index reaches update time and fail if
  * it is beyond. The waiting requires the node to be connected to the synchronizer (thus the
  * 'online' prefix).
  */
class OnlineManualLogicalSynchronizerUpgrade(
    override val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override val ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    syncPersistentStateManager: SyncPersistentStateManager,
    override val executionQueue: SimpleExecutionQueue,
    override val connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    override val connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CheckedLogicalSynchronizerUpgrade[OnlineManualLsuRequest] {
  override def kind: String = "manual-online"

  def upgrade(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: OnlineManualLsuRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    implicit val logger = Lsu.Logger(loggerFactory, getClass, request.successorPsid)
    val successorPsid = request.successorPsid

    logger.info(s"Manual upgrade from $currentPsid to ${request.successorPsid}")

    performIfNotUpgradedYet(request.successorPsid)(
      super.upgrade(
        alias,
        currentPsid,
        request,
      ),
      operation = s"manual upgrade from $currentPsid to $successorPsid",
    )
  }

  override protected def canBeUpgradedTo(
      currentPsid: PhysicalSynchronizerId,
      request: OnlineManualLsuRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult] = {

    val successors: Map[SequencerId, LsuSequencerConnectionSuccessor] =
      request.sequencerSuccessors.map { case (sequencerId, connection) =>
        sequencerId -> LsuSequencerConnectionSuccessor(
          sequencerId,
          successorPsid = request.successorPsid,
          connection,
        )
      }

    def upgradeCheck(): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = for {
      _ <- checkCleanSynchronizerIndex(request.lsid, request.upgradeTime)

      currentSyncPersistentState <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .get(currentPsid)
          .toRight(
            NegativeResult(
              s"Unable to find persistent state for $currentPsid",
              isRetryable = false,
            )
          )
      )

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          synchronizerConnectionConfigStore.get(request.successorPsid).map(_ => ())
        )
        .orElse(
          for {
            currentConfigAndNewSequencers <- prepareNewSynchronizerConnectionConfig(
              successors,
              currentSyncPersistentState,
            )
            (currentConfig, successorSequencerConnections) = currentConfigAndNewSequencers
            _ <- attemptSuccessorSynchronizerRegistration(
              currentPsid,
              request.successorPsid,
              request.upgradeTime,
              currentConfig,
              successorSequencerConnections,
            )
          } yield ()
        )
    } yield ()

    if (isUpgradeDone(request.successorPsid))
      EitherT.pure[FutureUnlessShutdown, NegativeResult](UpgradabilityCheckResult.UpgradeDone)
    else upgradeCheck().map(_ => UpgradabilityCheckResult.ReadyToUpgrade)
  }
}

/** Perform a manual logical synchronizer upgrade. This endpoint should ONLY be used when the
  * current synchronizer was declared irrevocably broken.
  *
  * The process will not perform any check related to the clean synchronizer index. The clean
  * synchronizer index will be used as upgrade time. To avoid race conditions between the read of
  * the clean synchronizer index and the updates in the stores (in [[performUpgradeInternal]]), the
  * node will disconnect from the synchronizer (thus the `offline` prefix).
  */
class OfflineManualLogicalSynchronizerUpgrade(
    override val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override val ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    syncPersistentStateManager: SyncPersistentStateManager,
    override val executionQueue: SimpleExecutionQueue,
    override val connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    override val connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CheckedLogicalSynchronizerUpgrade[OfflineManualLsuRequest] {
  override def kind: String = "manual-offline"

  def upgrade(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: OfflineManualLsuRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    implicit val logger = Lsu.Logger(loggerFactory, getClass, request.successorPsid)
    val successorPsid = request.successorPsid

    logger.info(s"Manual upgrade from $currentPsid to ${request.successorPsid}")

    performIfNotUpgradedYet(request.successorPsid)(
      super.upgrade(
        alias,
        currentPsid,
        request,
      ),
      operation = s"manual upgrade from $currentPsid to $successorPsid",
    )
  }

  // This method MUST be called when the node is disconnected from the synchronizer
  override protected def canBeUpgradedTo(
      currentPsid: PhysicalSynchronizerId,
      request: OfflineManualLsuRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult] = {

    val successors: Map[SequencerId, LsuSequencerConnectionSuccessor] =
      request.sequencerSuccessors.map { case (sequencerId, connection) =>
        sequencerId -> LsuSequencerConnectionSuccessor(
          sequencerId,
          successorPsid = request.successorPsid,
          connection,
        )
      }

    def upgradeCheck(): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = for {
      currentSyncPersistentState <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .get(currentPsid)
          .toRight(
            NegativeResult(
              s"Unable to find persistent state for $currentPsid",
              isRetryable = false,
            )
          )
      )

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          synchronizerConnectionConfigStore.get(request.successorPsid).map(_ => ())
        )
        .orElse(
          for {
            currentConfigAndNewSequencers <- prepareNewSynchronizerConnectionConfig(
              successors,
              currentSyncPersistentState,
            )
            (currentConfig, successorSequencerConnections) = currentConfigAndNewSequencers

            // Ensure the node will not connect to the current synchronizer...
            _ <- synchronizerConnectionConfigStore
              .setStatus(
                request.alias,
                KnownPhysicalSynchronizerId(currentPsid),
                SynchronizerConnectionConfigStore.LsuSource,
              )
              .leftMap(err =>
                NegativeResult(
                  s"Unable to mark current synchronizer $currentPsid as inactive: $err",
                  isRetryable = false,
                )
              )

            // ... so that the clean synchronizer index will not progress anymore
            upgradeTime <- OptionT(
              ledgerApiIndexer.asEval.value.ledgerApiStore.value
                .cleanSynchronizerIndex(request.lsid)
            ).map(_.recordTime)
              .toRight(
                NegativeResult(
                  s"Unable to get synchronizer index for ${request.lsid}",
                  isRetryable = true,
                )
              )

            _ <- attemptSuccessorSynchronizerRegistration(
              currentPsid,
              request.successorPsid,
              upgradeTime,
              currentConfig,
              successorSequencerConnections,
            )
          } yield ()
        )
    } yield ()

    if (isUpgradeDone(request.successorPsid))
      EitherT.pure[FutureUnlessShutdown, NegativeResult](UpgradabilityCheckResult.UpgradeDone)
    else upgradeCheck().map(_ => UpgradabilityCheckResult.ReadyToUpgrade)
  }
}

/** This class implements late manual LSU. It should be called for participants that are upgrading
  * manually:
  *   - because automatic LSU failed, or
  *   - because the node is upgrading after the old synchronizer has been decommissioned.
  *
  * The use cases imply that the class should be used only when:
  *   - Upgrade time has been reached.
  *   - The node has processed everything it could process on the old synchronizer.
  */
class UncheckedLateLogicalSynchronizerUpgrade(
    override val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override val executionQueue: SimpleExecutionQueue,
    override val connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    override val disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends LogicalSynchronizerUpgrade[LateLsuRequest] {

  override def kind: String = "late"

  def upgrade(
      request: AdminLateLsuRequest
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val currentPsid = request.currentPsid
    val successorPsid = request.successorPsid
    val alias = request.successorConfig.synchronizerAlias
    implicit val logger = Lsu.Logger(loggerFactory, getClass, successorPsid)

    EitherT(
      runWithRetries(
        performUpgrade(
          alias,
          currentPsid,
          LateLsuRequest(
            request.successorConfig,
            SynchronizerSuccessor(successorPsid, request.upgradeTime),
          ),
        ).value
      )
    )
  }

  override protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      request: LateLsuRequest,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Marking synchronizer connection $currentPsid as inactive")
    val successorPsid = request.successorPsid

    for {
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          request.successorConfig.synchronizerAlias,
          KnownPhysicalSynchronizerId(currentPsid),
          SynchronizerConnectionConfigStore.LsuSource,
        )
        .leftMap(_.message)

      /* Handshake and topology copy are useless now: the node is doing the LSU regardless of the
       * outcome. Potential issues will be flagged upon connection attempt to the synchronizer.
       */
      _ <- EitherT.rightT[FutureUnlessShutdown, String](
        pendingLsuOperationsStore.delete(
          currentPsid,
          PendingLsuOperation.operationKey,
          PendingLsuOperation.operationName,
        )
      )

      _ <- synchronizerConnectionConfigStore.get(successorPsid) match {
        case Left(_: UnknownPsid) =>
          logger.info(s"Storing synchronizer connection config for $successorPsid")
          synchronizerConnectionConfigStore
            .put(
              request.successorConfig,
              SynchronizerConnectionConfigStore.Active,
              KnownPhysicalSynchronizerId(successorPsid),
              Some(
                SynchronizerPredecessor(
                  currentPsid,
                  request.upgradeTime,
                  isLateUpgrade = true,
                )
              ),
            )
            .leftMap(err => s"Unable to store connection config for $successorPsid: $err")

        case Right(foundConfig) =>
          logger.info(s"Marking synchronizer connection $successorPsid as active")
          synchronizerConnectionConfigStore
            .setStatus(
              request.successorConfig.synchronizerAlias,
              foundConfig.configuredPsid,
              SynchronizerConnectionConfigStore.Active,
            )
            .leftMap(err => s"Unable to mark successor synchronizer $successorPsid as active: $err")
      }
    } yield logger.info("Late upgrade was successful")
  }
}

object LogicalSynchronizerUpgrade {
  private val RetryInitialDelay: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS)
  private val RetryMaxDelay: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  /** Indicate that an operations or a check (e.g., whether upgrade can be done) has negative
    * result.
    * @param details
    *   Context about the failure.
    * @param isRetryable
    *   - True when the operation can be retried (e.g., if the upgrade is not ready *yet*)
    *   - False when the operation should not be retried (e.g., invariant of a store violated)
    */
  final case class NegativeResult(details: String, isRetryable: Boolean)

  sealed trait LsuRequest {
    def successorPsid: PhysicalSynchronizerId
    def lsid: SynchronizerId = successorPsid.logical

    def isOnline: Boolean
  }

  final case class AutomaticLsuRequest(successor: SynchronizerSuccessor) extends LsuRequest {
    override def successorPsid: PhysicalSynchronizerId = successor.psid
    def upgradeTime: CantonTimestamp = successor.upgradeTime

    override def isOnline: Boolean = true
  }

  sealed trait ManualLsuRequest extends LsuRequest {
    def sequencerSuccessors: Map[SequencerId, GrpcConnection]
  }

  final case class OnlineManualLsuRequest(
      successorPsid: PhysicalSynchronizerId,
      upgradeTime: CantonTimestamp,
      sequencerSuccessors: Map[SequencerId, GrpcConnection],
  ) extends ManualLsuRequest {
    override def isOnline: Boolean = true
  }

  final case class OfflineManualLsuRequest(
      alias: SynchronizerAlias,
      successorPsid: PhysicalSynchronizerId,
      sequencerSuccessors: Map[SequencerId, GrpcConnection],
  ) extends ManualLsuRequest {
    override def isOnline: Boolean = false
  }

  final case class LateLsuRequest(
      successorConfig: SynchronizerConnectionConfig,
      successor: SynchronizerSuccessor,
  ) extends LsuRequest {
    override def successorPsid: PhysicalSynchronizerId = successor.psid
    override def isOnline: Boolean = false

    def upgradeTime: CantonTimestamp = successor.upgradeTime
  }
}
