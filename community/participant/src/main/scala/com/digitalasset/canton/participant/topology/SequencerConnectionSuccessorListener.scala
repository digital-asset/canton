// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.syntax.functor.*
import com.digitalasset.canton.data.{
  CantonTimestamp,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.LsuConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.{
  LogicalSynchronizerUpgrade,
  SyncPersistentStateManager,
  SyncServiceError,
}
import com.digitalasset.canton.participant.synchronizer.{
  PendingLsuOperation,
  SynchronizerRegistryHelpers,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.store.PendingOperation
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.{KnownPhysicalSynchronizerId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil
import com.digitalasset.canton.{SequencerCounter, SynchronizerAlias}
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.util.chaining.*

/** Listens to topology changes and creates a synchronizer connection config in the synchronizer
  * connection config store if the following requirements are satisfied:
  *
  *   - the topology state is frozen by the synchronizer owners, which also includes the physical
  *     synchronizer id of the successor synchronizer
  *   - there is no configuration for the successor physical synchronizer in the synchronizer
  *     connection configuration store
  *   - all sequencers that are configured in the currently active synchronizer connection for the
  *     given synchronizer alias have announced the connection details for connecting to the
  *     sequencer on the successor synchronizer
  */
class SequencerConnectionSuccessorListener(
    alias: SynchronizerAlias,
    topologyClient: SynchronizerTopologyClient,
    synchronizerHandshake: HandshakeWithSuccessor,
    syncPersistentStateManager: SyncPersistentStateManager,
    lsuConfig: LsuConfig,
    pendingLsuOperationsStore: PendingLsuOperation.Store,
    metrics: ParticipantMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with NamedLogging {

  private val configStore = syncPersistentStateManager.synchronizerConnectionConfigStore

  /** If enough successors are known, registers the config for the successors. Overwrite any
    * existing config to take into account all known successors.
    *
    * Uses the approximate topology snapshot for the successors.
    */
  def checkAndCreateSynchronizerConfig()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    checkAndCreateSynchronizerConfig(topologyClient.approximateTimestamp)

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    Monad[FutureUnlessShutdown].whenA(
      transactions.exists(_.mapping.code == Code.LsuSequencerConnectionSuccessor)
    )(checkAndCreateSynchronizerConfig(effectiveTimestamp.value.immediateSuccessor))

  private def checkAndCreateSynchronizerConfig(
      snapshotTs: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val resultOT = for {
      snapshot <- OptionT.liftF(topologyClient.awaitSnapshot(snapshotTs))

      (synchronizerSuccessor, _) <- OptionT(snapshot.announcedLsu())
      SynchronizerSuccessor(successorPsid, upgradeTime) = synchronizerSuccessor

      sequencerSuccessors <- OptionT
        .liftF(
          snapshot.sequencerConnectionSuccessors(successorPsid = successorPsid)
        )
        .map(_.fmap(_.mapping))

      successorConfig <- LogicalSynchronizerUpgrade
        .prepareNewSynchronizerConnectionConfig(
          psid = topologyClient.psid,
          successorPsid = successorPsid,
          sequencerSuccessors = sequencerSuccessors,
          configStore = configStore,
          warnOnIncomplete = false,
        )
        .fold(
          err => {
            logger.info(s"Unable to prepare new synchronizer config: $err")
            None
          },
          config => Option(config),
        )
        .pipe(OptionT(_))

      _ = metrics.setLsuStatus(ParticipantMetrics.LsuStatus.SequencerSuccessorsKnown, successorPsid)

      predecessor =
        SynchronizerPredecessor(topologyClient.psid, upgradeTime, isLateUpgrade = false)

      currentSuccessorConfigO =
        configStore.get(alias, KnownPhysicalSynchronizerId(successorPsid)).toOption

      updatedSuccessorConfig <- configStore
        .upsert(
          psid = successorPsid,
          insert =
            (successorConfig, SynchronizerConnectionConfigStore.LsuTarget, Some(predecessor)),
          /*
          Overwriting the sequencer connections is fine because we always reconstruct from topology state and
          current config.
           */
          overrideSequencerConnections = Some(successorConfig.sequencerConnections),
        )
        .tapLeft(err =>
          logger.warn(s"Unable to upsert synchronizer config of $successorPsid: $err")
        )
        .toOption
        .map(_.config)

      sequencerConnectionsChanged = !currentSuccessorConfigO
        .map(_.config.sequencerConnections)
        .contains(updatedSuccessorConfig.sequencerConnections)

      _ = if (lsuConfig.automaticallyPerformLsu && sequencerConnectionsChanged) {
        logger.info(s"Performing handshake to validate connection to $successorPsid")
        performHandshakeAndInitiateTopology(successorPsid, predecessor)
      }
    } yield ()

    resultOT.value.void
  }

  private def performHandshakeAndInitiateTopology(
      successorPsid: PhysicalSynchronizerId,
      predecessor: SynchronizerPredecessor,
  )(implicit
      traceContext: TraceContext
  ): Unit = {

    val rpv = PendingLsuOperation.protocolVersionRepresentativeFor(topologyClient.protocolVersion)
    val pendingOperation = PendingLsuOperation(successorPsid)(rpv)
      .toPendingOperation(currentPsid = topologyClient.psid)

    val resF: FutureUnlessShutdown[Unit] = for {
      _ <- pendingLsuOperationsStore
        .insert(pendingOperation)
        /* Left can happen only on inconsistent successor for a given psid which cannot happen because:
        - successor psid cannot be changed
        - the entry is removed upon LSU cancellation
         */
        .toOption
        .value
        .void

      _ <- synchronizerHandshake
        .handshakeWithSuccessor(pendingOperation)
        .value
        .flatMap {
          case Left(error) =>
            val isRetryable = error.retryable.isDefined

            // e.g., transient network or pool errors
            if (isRetryable)
              logger.info(s"Unable to perform handshake with $successorPsid: $error")
            else
              logger.error(s"Unable to perform handshake with $successorPsid: $error")
            FutureUnlessShutdown.unit

          case Right(Some(staticParams)) =>
            logger.info(s"Handshake with $successorPsid was successful")
            copyTopologyFromPredecessor(successorPsid, staticParams, predecessor)
              .flatMap(_ =>
                pendingLsuOperationsStore.delete(
                  topologyClient.psid,
                  PendingLsuOperation.operationKey,
                  PendingLsuOperation.operationName,
                )
              )

          case Right(None) =>
            logger.info(
              s"Handshake with $successorPsid did not return any static synchronizer parameters. LSU was cancelled or a non-retryable error occurred during handshake."
            )
            FutureUnlessShutdown.unit

        }
    } yield ()

    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      resF,
      level = Level.INFO,
      failureMessage = s"Failed to perform the synchronizer handshake with $successorPsid",
    )
  }

  private def copyTopologyFromPredecessor(
      successorPsid: PhysicalSynchronizerId,
      successorStaticParams: StaticSynchronizerParameters,
      predecessor: SynchronizerPredecessor,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    (for {
      persistentState <- syncPersistentStateManager
        .lookupOrCreatePersistentState(
          successorPsid,
          successorStaticParams,
          Some(predecessor),
        )

      _ <- SynchronizerRegistryHelpers
        .copyTopologyStateFromLocalPredecessorIfNeeded(
          Some(predecessor),
          persistentState,
          syncPersistentStateManager,
          metrics,
        )
    } yield logger.info(s"Successfully copied topology from predecessor to $successorPsid"))
      .valueOr { error =>
        logger.warn(s"Failed to copy topology from predecessor to $successorPsid: $error")
      }
}

trait HandshakeWithSuccessor {
  def handshakeWithSuccessor(
      pendingOperation: PendingOperation[PendingLsuOperation, PhysicalSynchronizerId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Option[StaticSynchronizerParameters]]
}
