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
import com.digitalasset.canton.participant.admin.data.ManualLsuRequest as AdminManualLsuRequest
import com.digitalasset.canton.participant.config.LsuConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.UnknownPsid
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SyncPersistentState,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.CheckedLogicalSynchronizerUpgrade.UpgradabilityCheckResult
import com.digitalasset.canton.participant.sync.CheckedLogicalSynchronizerUpgrade.UpgradabilityCheckResult.ReadyToUpgrade
import com.digitalasset.canton.participant.sync.LogicalSynchronizerUpgrade.{
  FinishAutomaticLsuRequest,
  FullAutomaticLsuRequest,
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
  PhysicalSynchronizerId,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Backoff
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue}
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import io.scalaland.chimney.dsl.*

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered
import scala.util.chaining.*

/** Performs the upgrade from one physical synchronizer to its successor. The final step is to mark
  * the successor configuration as active.
  */
sealed trait LogicalSynchronizerUpgrade[Req <: LsuRequest] extends NamedLogging with FlagCloseable {

  protected def request: Req
  protected lazy val currentPsid: PhysicalSynchronizerId = request.currentPsid
  protected lazy val successorPsid: PhysicalSynchronizerId = request.successorPsid

  protected lazy val alias: SynchronizerAlias = request.alias
  protected lazy val lsid: SynchronizerId = request.lsid

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
  def disconnectSynchronizer: TraceContext => EitherT[FutureUnlessShutdown, SyncServiceError, Unit]

  def kind: String
  def lsuConfig: LsuConfig

  // Retry policy for the readiness check and upgrade
  protected val retryPolicy: Backoff = Backoff.fromConfig(
    logger = logger,
    hasSynchronizeWithClosing = this,
    config = lsuConfig.lsuRetry,
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
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Run `operation` only if connectivity to `lsid` matches `shouldBeConnected`.
    */
  protected def enqueueOperation[T](
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
          val msg = s"Not scheduling $description because the node is ${text}connected to $lsid"
          logger.info(msg)
          FutureUnlessShutdown.pure(
            Left(
              NegativeResult(
                msg,
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
      f: => EitherT[FutureUnlessShutdown, E, Unit],
      operation: String,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
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
  protected def performUpgrade()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] =
    performIfNotUpgradedYet(
      for {
        _disconnect <- disconnectSynchronizer(traceContext).leftMap(err =>
          NegativeResult(err.toString, isRetryable = true)
        )

        _ <- EitherT(
          enqueueOperation(
            operation = performUpgradeInternal().leftMap { error =>
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
          // initial values
          (
            List.empty[SequencerAlias],
            List.empty[SequencerId],
            List.empty[SequencerConnection],
          )
        ) {
          // iterate over (accumulator, (key, value)) from the map currentSequencerConnections.aliasToConnection
          case ((withoutId, withoutSuccessor, successorConnections), (alias, currentConnection)) =>
            // look at the sequencerId in this connection assigned to this alias
            currentConnection.sequencerId match {

              case Some(sequencerId) =>
                sequencerSuccessors.get(sequencerId) match {
                  case Some(successor) =>
                    val newConnection = successor.toGrpcSequencerConnection(alias)

                    // new accumulator for the next item in foldLeft: new connection to the successor is added to the connections list
                    (withoutId, withoutSuccessor, newConnection +: successorConnections)

                  // if the sequencerId is not assigned any successor, add it to the list of "orphaned" sequencerIds for this alias
                  case None => (withoutId, sequencerId +: withoutSuccessor, successorConnections)
                }
              // case where the current connection for this alias has no sequencerId assigned: add the alias to the list of "orphaned" aliases
              case None => (alias +: withoutId, withoutSuccessor, successorConnections)
            }
        }

      _ = logger.info(
        s"Sequencers without known id: $withoutId. Sequencers without known successor: $withoutSuccessor. Successors known for: ${newConnections
            .map(_.sequencerAlias)}"
      )

      newConnectionsNE <- EitherT.fromOption[FutureUnlessShutdown](
        NonEmpty.from(newConnections),
        // if there are no new connections with known successors, i.e. list newConnections is empty, return an error
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

  def isUpgradeDone(): Boolean =
    synchronizerConnectionConfigStore
      .get(successorPsid)
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

  def connectSynchronizer: TraceContext => EitherT[
    FutureUnlessShutdown,
    SyncServiceError,
    Option[PhysicalSynchronizerId],
  ]
  def pendingLsuOperationsStore: PendingLsuOperation.Store

  def metrics: ParticipantMetrics

  protected def upgrade()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Upgrade from $currentPsid to $successorPsid")

    val upgradabilityCheck =
      if (request.isOnline) onlineUpgradabilityCheck _ else offlineUpgradabilityCheck _

    // Ensure upgraded is not attempted if announcement was revoked
    performIfNotUpgradedYet(
      for {
        upgradabilityCheckResult <- EitherT(runWithRetries(upgradabilityCheck()))

        _ <- upgradabilityCheckResult match {
          case UpgradabilityCheckResult.ReadyToUpgrade =>
            logger.info(
              s"Upgrade from $currentPsid to $successorPsid is possible, starting internal upgrade"
            )

            EitherT(runWithRetries(performUpgrade().value))

          case UpgradabilityCheckResult.UpgradeDone =>
            logger.info(s"Upgrade from $currentPsid to $successorPsid already done.")
            EitherTUtil.unitUS
        }

        _ <- connectSynchronizer(traceContext).leftMap(_.toString)
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
  protected def onlineUpgradabilityCheck()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Either[NegativeResult, UpgradabilityCheckResult]] = {
    logger.info("Attempting online upgradability check")

    connectSynchronizer(traceContext).value.flatMap {
      case Left(error) =>
        // Left will lead to a retry
        FutureUnlessShutdown.pure(
          NegativeResult(
            s"Failed to connect to $alias to perform upgradability check: $error",
            isRetryable = true,
          ).asLeft
        )

      case Right(None) =>
        FutureUnlessShutdown.pure(
          NegativeResult(
            s"Failed to connect to $alias to perform upgradability check.",
            isRetryable = true,
          ).asLeft
        )

      case Right(Some(`successorPsid`)) =>
        FutureUnlessShutdown.pure(UpgradabilityCheckResult.UpgradeDone.asRight)

      case Right(Some(`currentPsid`)) =>
        enqueueOperation(
          canBeUpgradedTo().value,
          description = "lsu-online-upgradability-check",
          shouldBeConnected = true,
        )

      case Right(Some(other)) =>
        FutureUnlessShutdown.pure(
          NegativeResult(
            s"Node is connected to $other which is incompatible with upgrade from $currentPsid to $successorPsid",
            isRetryable = false,
          ).asLeft
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
  protected def offlineUpgradabilityCheck()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Either[NegativeResult, UpgradabilityCheckResult]] = {
    logger.info("Attempting offline upgradability check")

    disconnectSynchronizer(traceContext).value.flatMap {
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
          canBeUpgradedTo().value,
          description = "lsu-offline-upgradability-check",
          shouldBeConnected = false,
        )
    }
  }

  protected def canBeUpgradedTo()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult]

  /** Check whether the value of the clean synchronizer index is compatible with LSU.
    */
  protected def checkCleanSynchronizerIndex(
      upgradeTime: CantonTimestamp
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

  /** Check whether the current psid and the successor psid are compatible.
    */
  protected def checkPsids()(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = for {
    _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
      currentPsid.logical == request.lsid,
      NegativeResult(
        s"Current psid ($currentPsid) and request lsid (${request.lsid}) are incompatible ",
        isRetryable = false,
      ),
    )

    _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
      currentPsid < request.successorPsid,
      NegativeResult(
        s"Current psid ($currentPsid) is not smaller than successor psid (${request.successorPsid})",
        isRetryable = false,
      ),
    )
  } yield ()

  /** Performs the upgrade. Fails if the upgrade cannot be performed.
    *
    * Prerequisites:
    *   - Time on the current synchronizer has reached the upgrade time.
    *   - `canBeUpgradedTo` returns Right(`ReadyToUpgrade`)
    *   - Node is disconnected from the synchronizer
    */
  override protected def performUpgradeInternal()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
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
        .leftMap(err =>
          s"Unable to mark current synchronizer $currentPsid as inactive (Status: LsuSource): $err"
        )

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

      _ = metrics.setLsuStatus(ParticipantMetrics.LsuStatus.LsuDone, request.successorPsid)
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
    override val connectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val metrics: ParticipantMetrics,
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val lsuConfig: LsuConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(override val request: FullAutomaticLsuRequest)(implicit
    executionContext: ExecutionContext
) extends CheckedLogicalSynchronizerUpgrade[FullAutomaticLsuRequest] {

  private def synchronizerSuccessor: SynchronizerSuccessor = request.successor
  private def upgradeTime: CantonTimestamp = request.successor.upgradeTime

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
  def upgrade()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Upgrade from $currentPsid to $successorPsid")

    // Ensure upgrade is not attempted if announcement was revoked
    def ensureUpgradeOngoing(): EitherT[FutureUnlessShutdown, String, Unit] = for {
      topologyStore <- EitherT.fromOption[FutureUnlessShutdown](
        syncPersistentStateManager.get(currentPsid).map(_.topologyStore),
        "Unable to find topology store",
      )

      announcements <- EitherT
        .liftF(
          topologyStore.findPositiveTransactions(
            asOf = upgradeTime,
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

    performIfNotUpgradedYet(
      for {
        _ <- ensureUpgradeOngoing()
        _ <- super.upgrade()
      } yield (),
      operation = s"automatic upgrade from $currentPsid to $successorPsid",
    )
  }

  /** Check whether the upgrade can be done. A left indicates that the upgrade cannot be done. A
    * right indicates that the upgrade can be attempted or was already done.
    */
  override protected def canBeUpgradedTo()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult] = {
    val upgradeTime = request.upgradeTime

    def runningCommitmentWatermarkCheck(
        runningCommitmentWatermark: CantonTimestamp
    ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] =
      if (runningCommitmentWatermark == upgradeTime)
        EitherTUtil.unitUS
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
      _ <- checkPsids()

      _ <- checkCleanSynchronizerIndex(upgradeTime)

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
            psid = currentSyncPersistentState.psid, // guaranteed to be same as currentPsid
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

    if (isUpgradeDone())
      EitherT.pure[FutureUnlessShutdown, NegativeResult](UpgradabilityCheckResult.UpgradeDone)
    else upgradeCheck().map(_ => UpgradabilityCheckResult.ReadyToUpgrade)
  }
}

class FinishAutomaticLogicalSynchronizerUpgrade(
    override val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override val ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    override val executionQueue: SimpleExecutionQueue,
    override val connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    override val connectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val metrics: ParticipantMetrics,
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val lsuConfig: LsuConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(override val request: FinishAutomaticLsuRequest)(implicit
    executionContext: ExecutionContext
) extends CheckedLogicalSynchronizerUpgrade[FinishAutomaticLsuRequest] {

  override def kind: String = "finish-upgrade"

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
  def finishUpgradeWithoutChecks()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _disconnect <- disconnectSynchronizer(traceContext)
        .leftMap(err => NegativeResult(err.toString, isRetryable = true))
        .leftMap(_.details)

      _ <- enqueueOperation(
        operation = performUpgradeInternal().leftMap { error =>
          /*
          Because preconditions are checked, a failure of the upgrade is not something that can be automatically
          recovered from (except DB exceptions).
           */
          NegativeResult(
            s"Unable to finish upgrade of $currentPsid to $successorPsid. Not retrying. Cause: $error",
            isRetryable = false,
          )
        }.value,
        description = s"finish-lsu-upgrade",
        shouldBeConnected = false,
      ).pipe(EitherT(_)).leftMap(_.details)
    } yield ()

  override protected def canBeUpgradedTo()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult] =
    EitherT.pure(ReadyToUpgrade)
}

object ManualLogicalSynchronizerUpgrade {
  def upgrade(
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
      syncPersistentStateManager: SyncPersistentStateManager,
      executionQueue: SimpleExecutionQueue,
      connectedSynchronizersLookup: ConnectedSynchronizersLookup,
      connectSynchronizer: TraceContext => EitherT[FutureUnlessShutdown, SyncServiceError, Option[
        PhysicalSynchronizerId
      ]],
      disconnectSynchronizer: TraceContext => EitherT[FutureUnlessShutdown, SyncServiceError, Unit],
      metrics: ParticipantMetrics,
      pendingLsuOperationsStore: PendingLsuOperation.Store,
      lsuConfig: LsuConfig,
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
          metrics,
          pendingLsuOperationsStore,
          lsuConfig,
          timeouts,
          loggerFactory.append("lsu", request.successorPsid.suffix),
        )(
          request
            .into[OnlineManualLsuRequest]
            .withFieldConst(_.upgradeTime, upgradeTime)
            .withFieldConst(_.alias, alias)
            .transform
        )
          .upgrade()

      case None =>
        new OfflineManualLogicalSynchronizerUpgrade(
          synchronizerConnectionConfigStore,
          ledgerApiIndexer,
          syncPersistentStateManager,
          executionQueue,
          connectedSynchronizersLookup,
          connectSynchronizer,
          disconnectSynchronizer,
          metrics,
          pendingLsuOperationsStore,
          lsuConfig,
          timeouts,
          loggerFactory.append("lsu", request.successorPsid.suffix),
        )(request.into[OfflineManualLsuRequest].withFieldConst(_.alias, alias).transform).upgrade()
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
    override val connectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val metrics: ParticipantMetrics,
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val lsuConfig: LsuConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(
    override val request: OnlineManualLsuRequest
)(implicit executionContext: ExecutionContext)
    extends CheckedLogicalSynchronizerUpgrade[OnlineManualLsuRequest] {
  override def kind: String = "manual-online"

  def upgrade()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Manual upgrade from $currentPsid to ${request.successorPsid}")

    performIfNotUpgradedYet(
      super.upgrade(),
      operation = s"manual upgrade from $currentPsid to $successorPsid",
    )
  }

  override protected def canBeUpgradedTo()(implicit
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
      _ <- checkPsids()

      _ <- checkCleanSynchronizerIndex(request.upgradeTime)

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
              request.upgradeTime,
              currentConfig,
              successorSequencerConnections,
            )
          } yield ()
        )
    } yield ()

    if (isUpgradeDone())
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
    override val connectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Option[PhysicalSynchronizerId],
    ],
    override val disconnectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    override val metrics: ParticipantMetrics,
    override val pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val lsuConfig: LsuConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(
    override val request: OfflineManualLsuRequest
)(implicit executionContext: ExecutionContext)
    extends CheckedLogicalSynchronizerUpgrade[OfflineManualLsuRequest] {
  override def kind: String = "manual-offline"

  def upgrade()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Manual upgrade from $currentPsid to $successorPsid")

    performIfNotUpgradedYet(
      super.upgrade(),
      operation = s"manual upgrade from $currentPsid to $successorPsid",
    )
  }

  // This method MUST be called when the node is disconnected from the synchronizer
  override protected def canBeUpgradedTo()(implicit
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
      _ <- checkPsids()

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
              upgradeTime,
              currentConfig,
              successorSequencerConnections,
            )
          } yield ()
        )
    } yield ()

    if (isUpgradeDone())
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
    override val disconnectSynchronizer: TraceContext => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    metrics: ParticipantMetrics,
    pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val lsuConfig: LsuConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(override val request: LateLsuRequest)(implicit executionContext: ExecutionContext)
    extends LogicalSynchronizerUpgrade[LateLsuRequest] {

  override def kind: String = "late"

  def upgrade()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT(runWithRetries(performUpgrade().value))

  override protected def performUpgradeInternal()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Marking synchronizer connection $currentPsid as inactive")

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

      _ = metrics.setLsuStatus(ParticipantMetrics.LsuStatus.LsuDone, request.successorPsid)
    } yield logger.info("Late upgrade was successful")
  }
}

object LogicalSynchronizerUpgrade {

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
    def currentPsid: PhysicalSynchronizerId
    def successorPsid: PhysicalSynchronizerId

    def alias: SynchronizerAlias
    def lsid: SynchronizerId = successorPsid.logical

    def isOnline: Boolean
  }

  sealed trait AutomaticLsu extends LsuRequest

  final case class FullAutomaticLsuRequest(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      successor: SynchronizerSuccessor,
  ) extends AutomaticLsu {
    override def successorPsid: PhysicalSynchronizerId = successor.psid
    def upgradeTime: CantonTimestamp = successor.upgradeTime

    override def isOnline: Boolean = true
  }

  // Finish the LSU without checks (e.g., after a crash)
  final case class FinishAutomaticLsuRequest(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      successorPsid: PhysicalSynchronizerId,
  ) extends AutomaticLsu {
    override def isOnline: Boolean = true
  }

  sealed trait ManualLsuRequest extends LsuRequest {
    def sequencerSuccessors: Map[SequencerId, GrpcConnection]
  }

  final case class OnlineManualLsuRequest(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      successorPsid: PhysicalSynchronizerId,
      upgradeTime: CantonTimestamp,
      sequencerSuccessors: Map[SequencerId, GrpcConnection],
  ) extends ManualLsuRequest {
    override def isOnline: Boolean = true
  }

  final case class OfflineManualLsuRequest(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      successorPsid: PhysicalSynchronizerId,
      sequencerSuccessors: Map[SequencerId, GrpcConnection],
  ) extends ManualLsuRequest {
    override def isOnline: Boolean = false
  }

  final case class LateLsuRequest(
      successorConfig: SynchronizerConnectionConfig,
      currentPsid: PhysicalSynchronizerId,
      successor: SynchronizerSuccessor,
  ) extends LsuRequest {
    def alias: SynchronizerAlias = successorConfig.synchronizerAlias
    override def successorPsid: PhysicalSynchronizerId = successor.psid
    override def isOnline: Boolean = false

    def upgradeTime: CantonTimestamp = successor.upgradeTime
  }
}
