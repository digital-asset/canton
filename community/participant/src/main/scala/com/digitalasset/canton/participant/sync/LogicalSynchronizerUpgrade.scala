// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
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
import com.digitalasset.canton.participant.admin.data.LateLsuRequest
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.UnknownPsid
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SyncPersistentState,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.CheckedLogicalSynchronizerUpgrade.UpgradabilityCheckResult
import com.digitalasset.canton.participant.sync.LogicalSynchronizerUpgrade.NegativeResult
import com.digitalasset.canton.participant.synchronizer.PendingHandshakeWithLsuSuccessor.PendingHandshakesWithSuccessorsStore
import com.digitalasset.canton.participant.synchronizer.{
  PendingHandshakeWithLsuSuccessor,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot
import com.digitalasset.canton.topology.store.NoPackageDependencies
import com.digitalasset.canton.topology.transaction.{
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

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/** Performs the upgrade from one physical synchronizer to its successor. The final step is to mark
  * the successor configuration as active.
  */
sealed trait LogicalSynchronizerUpgrade[Req] extends NamedLogging with FlagCloseable {

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
      synchronizerSuccessor: SynchronizerSuccessor,
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
      synchronizerSuccessor: SynchronizerSuccessor,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] = {
    val successorPsid = synchronizerSuccessor.psid
    val lsid = currentPsid.logical

    performIfNotUpgradedYet(successorPsid)(
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
              synchronizerSuccessor,
              request,
            ).leftMap { error =>
              /*
                Because preconditions are checked, a failure of the upgrade is not something that can be automatically
                recovered from (except DB exceptions).
               */
              NegativeResult(
                s"Unable to upgrade $currentPsid to $successorPsid. Not retrying. Cause: $error",
                isRetryable = false,
              )
            }.value,
            description = s"$kind-lsu-upgrade",
            shouldBeConnected = false,
          )
        )
      } yield (),
      operation = s"$kind upgrade from $currentPsid to $successorPsid",
    )
  }

  /** Attempt to register the successor synchronizer
    *   - Compute the new list of new sequencer connections
    *   - If the list has enough elements (more than the sequencer threshold), register the
    *     synchronizer.
    *
    * Prerequisite:
    *   - Successor was not already registered
    */
  protected def attemptSuccessorSynchronizerRegistration(
      successors: Map[SequencerId, LsuSequencerConnectionSuccessor],
      currentSyncPersistentState: SyncPersistentState,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, NegativeResult, Unit] =
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
                successors.get(sequencerId) match {
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

      _ = logger.info("Trying to save the config of the successor")

      _ <- synchronizerConnectionConfigStore
        .put(
          config = currentConfig.config.copy(sequencerConnections = newSequencerConnections),
          status = SynchronizerConnectionConfigStore.LsuTarget,
          configuredPsid = KnownPhysicalSynchronizerId(synchronizerSuccessor.psid),
          synchronizerPredecessor = Some(
            SynchronizerPredecessor(
              currentSyncPersistentState.psid,
              synchronizerSuccessor.upgradeTime,
              isLateUpgrade = false,
            )
          ),
        )
        .leftMap(err =>
          NegativeResult(s"Unable to store new synchronizer connection: $err", isRetryable = false)
        )

    } yield ()
}

trait CheckedLogicalSynchronizerUpgrade[Req] extends LogicalSynchronizerUpgrade[Req] {
  def connectSynchronizer: Traced[SynchronizerAlias] => EitherT[
    FutureUnlessShutdown,
    SyncServiceError,
    Option[PhysicalSynchronizerId],
  ]
  def pendingHandshakesWithSuccessorsStore: PendingHandshakesWithSuccessorsStore

  /** Check whether the upgrade can be done.
    *   - A left indicates that the upgrade cannot be done (yet). Retryability is encoded in
    *     NegativeResult.
    *   - A right indicates that the upgrade can be attempted or was already done.
    *
    * This method wraps the call to [[canBeUpgradedTo]] to ensure proper connectivity to the
    * synchronizer.
    */
  protected def upgradabilityCheck(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): FutureUnlessShutdown[Either[NegativeResult, UpgradabilityCheckResult]] = {
    val successorPsid = synchronizerSuccessor.psid
    val lsid = currentPsid.logical

    logger.info("Attempting upgradability check")

    connectSynchronizer(Traced(alias)).value.flatMap {
      case Left(error) =>
        // Left will lead to a retry
        FutureUnlessShutdown.pure(
          NegativeResult(s"Unable to connect to $alias: $error", isRetryable = true).asLeft
        )

      case Right(Some(`successorPsid`)) =>
        FutureUnlessShutdown.pure(UpgradabilityCheckResult.UpgradeDone.asRight)

      case Right(_) =>
        enqueueOperation(
          lsid,
          canBeUpgradedTo(currentPsid, synchronizerSuccessor, request).value,
          description = "lsu-upgradability-check",
          shouldBeConnected = true,
        )
    }
  }

  protected def canBeUpgradedTo(
      currentPsid: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      request: Req,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult]

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
      synchronizerSuccessor: SynchronizerSuccessor,
      request: Req,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      // Should have been checked before but this is cheap
      _ <- canBeUpgradedTo(currentPsid, synchronizerSuccessor, request).leftMap(_.details)

      // Prevent reconnect to the current/old synchronizer
      _ = logger.info(s"Marking synchronizer connection $currentPsid as inactive")
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(currentPsid),
          SynchronizerConnectionConfigStore.LsuSource,
        )
        .leftMap(err => s"Unable to mark current synchronizer $currentPsid as inactive: $err")

      /*
       Handshake is useless now: the node is doing the LSU regardless of the outcome of the handshake.
       Potential issues will be flagged upon connection attempt to the synchronizer.
       */
      _ <- EitherT.rightT[FutureUnlessShutdown, String](
        pendingHandshakesWithSuccessorsStore.delete(
          currentPsid,
          PendingHandshakeWithLsuSuccessor.operationKey,
          PendingHandshakeWithLsuSuccessor.operationName,
        )
      )

      // Comes last to indicate that the node is ready to connect to the successor
      _ = logger.info(s"Marking synchronizer connection ${synchronizerSuccessor.psid} as active")
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          alias,
          KnownPhysicalSynchronizerId(synchronizerSuccessor.psid),
          SynchronizerConnectionConfigStore.Active,
        )
        .leftMap(err =>
          s"Unable to mark successor synchronizer ${synchronizerSuccessor.psid} as active: $err"
        )
    } yield logger.info("Automatic upgrade was successful")
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
    ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
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
    override val pendingHandshakesWithSuccessorsStore: PendingHandshakesWithSuccessorsStore,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CheckedLogicalSynchronizerUpgrade[Unit] {

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
    implicit val logger = Lsu.Logger(loggerFactory, getClass, synchronizerSuccessor)

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

        upgradabilityCheckResult <- EitherT(
          runWithRetries(upgradabilityCheck(alias, currentPsid, synchronizerSuccessor, ()))
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
                    synchronizerSuccessor,
                    (),
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
    implicit val logger = Lsu.Logger(loggerFactory, getClass, synchronizerSuccessor)

    performUpgradeInternal(alias, currentPsid, synchronizerSuccessor, ())
  }

  /** Check whether the upgrade can be done. A left indicates that the upgrade cannot be done. A
    * right indicates that the upgrade can be attempted or was already done.
    */
  protected def canBeUpgradedTo(
      currentPsid: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      request: Unit,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, NegativeResult, UpgradabilityCheckResult] = {
    val upgradeTime = synchronizerSuccessor.upgradeTime

    def isUpgradeDone(): EitherT[FutureUnlessShutdown, NegativeResult, Boolean] =
      EitherT
        .fromOptionF(
          ledgerApiIndexer.asEval.value.ledgerApiStore.value
            .cleanSynchronizerIndex(currentPsid.logical),
          NegativeResult(
            s"Unable to get synchronizer index for ${currentPsid.logical}",
            isRetryable = true,
          ),
        )
        .map(_.recordTime > upgradeTime)

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
      synchronizerIndex <- EitherT.fromOptionF(
        ledgerApiIndexer.asEval.value.ledgerApiStore.value
          .cleanSynchronizerIndex(currentPsid.logical),
        NegativeResult(
          s"Unable to get synchronizer index for ${currentPsid.logical}",
          isRetryable = true,
        ),
      )

      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          synchronizerIndex.recordTime == upgradeTime,
          NegativeResult(
            s"Synchronizer index should be at $upgradeTime time but found ${synchronizerIndex.recordTime}",
            isRetryable = true,
          ),
        )

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
          synchronizerConnectionConfigStore.get(synchronizerSuccessor.psid).map(_ => ())
        )
        .orElse {
          val topologySnapshot = new StoreBasedTopologySnapshot(
            psid = currentSyncPersistentState.psid,
            timestamp = synchronizerSuccessor.upgradeTime,
            store = currentSyncPersistentState.topologyStore,
            packageDependencyResolver = NoPackageDependencies,
            loggerFactory = loggerFactory,
          )

          for {
            successors <- EitherT.liftF(
              topologySnapshot.sequencerConnectionSuccessors(synchronizerSuccessor.psid)
            )
            _ <- attemptSuccessorSynchronizerRegistration(
              successors,
              currentSyncPersistentState,
              synchronizerSuccessor,
            )
          } yield ()
        }

      runningCommitmentWatermark <- EitherT.liftF(
        currentSyncPersistentState.acsCommitmentStore.runningCommitments.watermark.map(_.timestamp)
      )

      _ <- runningCommitmentWatermarkCheck(runningCommitmentWatermark)
    } yield ()

    isUpgradeDone().flatMap { isDone =>
      if (isDone)
        EitherT.pure[FutureUnlessShutdown, NegativeResult](UpgradabilityCheckResult.UpgradeDone)
      else upgradeCheck().map(_ => UpgradabilityCheckResult.ReadyToUpgrade)
    }
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
class LateLogicalSynchronizerUpgrade(
    override val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override val executionQueue: SimpleExecutionQueue,
    override val connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    override val disconnectSynchronizer: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncServiceError,
      Unit,
    ],
    pendingHandshakesWithSuccessorsStore: PendingHandshakesWithSuccessorsStore,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends LogicalSynchronizerUpgrade[SynchronizerConnectionConfig] {

  override def kind: String = "late"

  def upgrade(
      request: LateLsuRequest
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val currentPsid = request.currentPsid
    val successorPsid = request.successorPsid
    val upgradeTime = request.upgradeTime
    val alias = request.successorConfig.synchronizerAlias
    val synchronizerSuccessor = SynchronizerSuccessor(successorPsid, upgradeTime)
    implicit val logger = Lsu.Logger(loggerFactory, getClass, synchronizerSuccessor)

    EitherT(
      runWithRetries(
        performUpgrade(
          alias,
          currentPsid,
          synchronizerSuccessor,
          request.successorConfig,
        ).value
      )
    )
  }

  override protected def performUpgradeInternal(
      alias: SynchronizerAlias,
      currentPsid: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
      successorConfig: SynchronizerConnectionConfig,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      logger: Lsu.Logger,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Marking synchronizer connection $currentPsid as inactive")
    val successorPsid = synchronizerSuccessor.psid

    for {
      _ <- synchronizerConnectionConfigStore
        .setStatus(
          successorConfig.synchronizerAlias,
          KnownPhysicalSynchronizerId(currentPsid),
          SynchronizerConnectionConfigStore.LsuSource,
        )
        .leftMap(_.message)

      /*
       Handshake is useless now: the node is doing the LSU regardless of the outcome of the handshake.
       Potential issues will be flagged upon connection attempt to the synchronizer.
       */
      _ <- EitherT.rightT[FutureUnlessShutdown, String](
        pendingHandshakesWithSuccessorsStore.delete(
          currentPsid,
          PendingHandshakeWithLsuSuccessor.operationKey,
          PendingHandshakeWithLsuSuccessor.operationName,
        )
      )

      _ <- synchronizerConnectionConfigStore.get(successorPsid) match {
        case Left(_: UnknownPsid) =>
          logger.info(s"Storing synchronizer connection config for $successorPsid")
          synchronizerConnectionConfigStore
            .put(
              successorConfig,
              SynchronizerConnectionConfigStore.Active,
              KnownPhysicalSynchronizerId(successorPsid),
              Some(
                SynchronizerPredecessor(
                  currentPsid,
                  synchronizerSuccessor.upgradeTime,
                  isLateUpgrade = true,
                )
              ),
            )
            .leftMap(err => s"Unable to store connection config for $successorPsid: $err")

        case Right(foundConfig) =>
          logger.info(s"Marking synchronizer connection $successorPsid as active")
          synchronizerConnectionConfigStore
            .setStatus(
              successorConfig.synchronizerAlias,
              foundConfig.configuredPsid,
              SynchronizerConnectionConfigStore.Active,
            )
            .leftMap(err => s"Unable to mark successor synchronizer $successorPsid as active: $err")
      }
    } yield logger.info("Late upgrade was successful")
  }
}

protected object LogicalSynchronizerUpgrade {
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

}
