// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{
  CantonTimestamp,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.{SyncPersistentStateManager, SyncServiceError}
import com.digitalasset.canton.participant.synchronizer.{
  PendingLsuOperation,
  SynchronizerRegistryHelpers,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.{KnownPhysicalSynchronizerId, Lsu, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil
import com.digitalasset.canton.{SequencerCounter, SynchronizerAlias}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

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
    automaticallyConnectToUpgradedSynchronizer: Boolean,
    pendingLsuOperationsStore: PendingLsuOperation.Store,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with NamedLogging {

  private val configStore = syncPersistentStateManager.synchronizerConnectionConfigStore

  def init()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
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
      activeConfig <- OptionT.fromOption[FutureUnlessShutdown](
        configStore.get(topologyClient.psid).toOption
      )
      configuredSequencers =
        activeConfig.config.sequencerConnections.aliasToConnection.forgetNE.toSeq.flatMap {
          case (sequencerAlias, connection) =>
            connection.sequencerId.map(_ -> sequencerAlias)
        }.toMap
      configuredSequencerIds = configuredSequencers.keySet

      (synchronizerSuccessor, _) <- OptionT(snapshot.announcedLsu())
      SynchronizerSuccessor(successorPsid, upgradeTime) = synchronizerSuccessor

      logger = Lsu.Logger(loggerFactory, getClass, synchronizerSuccessor.psid)

      _ = logger.info(
        s"Checking whether the participant can migrate $alias config from ${activeConfig.configuredPsid} to $successorPsid"
      )
      _ = logger.info(s"Configured sequencer connections: $configuredSequencerIds")

      sequencerSuccessors <- OptionT.liftF(
        snapshot.sequencerConnectionSuccessors(successorPsid = successorPsid)
      )

      _ = logger.info(s"Successors are currently known for: $sequencerSuccessors")

      configuredSequencersWithoutSuccessor = configuredSequencerIds
        .diff(sequencerSuccessors.keySet)
      _ = if (configuredSequencersWithoutSuccessor.nonEmpty)
        logger.info(
          s"Some sequencer have not yet announced their endpoints on the successor synchronizer: $configuredSequencersWithoutSuccessor"
        )
      _ <- OptionT
        .when[FutureUnlessShutdown, Unit](configuredSequencersWithoutSuccessor.isEmpty)(())

      successorConnections <- OptionT.fromOption[FutureUnlessShutdown](
        NonEmpty.from(sequencerSuccessors.flatMap { case (successorSequencerId, successorConfig) =>
          configuredSequencers.get(successorSequencerId).map { sequencerAlias =>
            successorConfig.toGrpcSequencerConnection(sequencerAlias)
          }
        }.toSeq)
      )

      _ = logger.info(s"New set of sequencer connections for successors: $successorConnections")

      sequencerConnections <- OptionT.fromOption[FutureUnlessShutdown](
        activeConfig.config.sequencerConnections.modifyConnections(successorConnections).toOption
      )

      predecessor =
        SynchronizerPredecessor(topologyClient.psid, upgradeTime, isLateUpgrade = false)

      currentSuccessorConfigO =
        configStore.get(alias, KnownPhysicalSynchronizerId(successorPsid)).toOption

      successorConfig = activeConfig.config.copy(
        synchronizerId = Some(successorPsid),
        sequencerConnections = sequencerConnections,
      )

      updatedSuccessorConfig <- configStore
        .upsert(
          psid = successorPsid,
          insert =
            (successorConfig, SynchronizerConnectionConfigStore.LsuTarget, Some(predecessor)),
          transform = _.focus(_.synchronizerId)
            .replace(Some(successorPsid))
            .focus(_.sequencerConnections)
            .replace(sequencerConnections),
        )
        .tapLeft(err =>
          logger.warn(s"Unable to upsert synchronizer config of $successorPsid: $err")
        )
        .toOption
        .map(_.config)

      sequencerConnectionsChanged = !currentSuccessorConfigO
        .map(_.config.sequencerConnections)
        .contains(updatedSuccessorConfig.sequencerConnections)

      _ = if (automaticallyConnectToUpgradedSynchronizer && sequencerConnectionsChanged) {
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

    val resF: FutureUnlessShutdown[Unit] = for {
      _ <- pendingLsuOperationsStore
        .insert(
          PendingLsuOperation(successorPsid)(rpv)
            .toPendingOperation(currentPsid = topologyClient.psid)
        )
        /* Left can happen only on inconsistent successor for a given psid which cannot happen because:
        - successor psid cannot be changed
        - the entry is removed upon LSU cancellation
         */
        .toOption
        .value
        .void

      _ <- synchronizerHandshake
        .handshakeWithSuccessor(successorPsid)
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
          case Right(staticParams) =>
            logger.info(s"Handshake with $successorPsid was successful")
            copyTopologyFromPredecessor(successorPsid, staticParams, predecessor)
              .map(_ =>
                pendingLsuOperationsStore.delete(
                  topologyClient.psid,
                  PendingLsuOperation.operationKey,
                  PendingLsuOperation.operationName,
                )
              )
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
        )

      _ <- SynchronizerRegistryHelpers
        .copyTopologyStateFromLocalPredecessorIfNeeded(
          Some(predecessor),
          persistentState,
          syncPersistentStateManager,
        )
    } yield logger.info(s"Successfully copied topology from predecessor to $successorPsid"))
      .valueOr { error =>
        logger.warn(s"Failed to copy topology from predecessor to $successorPsid: $error")
      }
}

trait HandshakeWithSuccessor {
  def handshakeWithSuccessor(successorPsid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, StaticSynchronizerParameters]
}
