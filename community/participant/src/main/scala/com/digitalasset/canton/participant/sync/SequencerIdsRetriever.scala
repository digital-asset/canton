// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.syntax.either.*
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.topology.SequencerConnectionSuccessorListener
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.sequencing.client.pool.SequencerConnectionPool
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil
import com.digitalasset.canton.util.retry.Backoff
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

/** This class implements a background task that tries to grab the ids of all the sequencers from
  * the connection pool and store them.
  */
final class SequencerIdsRetriever(
    psid: PhysicalSynchronizerId,
    connectionPool: SequencerConnectionPool,
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    sequencerConnectionSuccessorListener: SequencerConnectionSuccessorListener,
    parameters: ParticipantNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {
  override protected val timeouts: ProcessingTimeout = parameters.processingTimeouts

  /** Starts the background task that attempts to retrieve all sequencer ids and store them in the
    * [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore]].
    *
    * MUST be called at most once.
    */
  def start()(implicit traceContext: TraceContext): Unit = {
    val retryPolicy: Backoff = Backoff.fromConfig(
      logger = logger,
      hasSynchronizeWithClosing = this,
      config = parameters.lsuConfig.sequencerIdsRetrievalRetry,
      operationName = s"retrieve-sequencer-ids-$psid",
      retryLogLevel = Some(Level.INFO),
    )

    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      retryPolicy.unlessShutdown(retrieveAndStoreIds(), DbExceptionRetryPolicy),
      failureMessage = "Unable to retrieve sequencer ids",
    )
  }

  /** Try to retrieve and store the missing sequencer IDs for the synchronizer:
    *   - IDs that can be retrieved are stored.
    *
    * @return
    *   - Right if all sequencer ids are known or if there is a fatal internal error.
    *   - Left if some ids are still unknown and the tasks should be retried.
    */
  private def retrieveAndStoreIds()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, Unit]] =
    getMissingIds match {
      case Left(_: SynchronizerConnectionConfigStore.UnknownPsid) =>
        logger.warn(s"Internal error: cannot get configuration of synchronizer $psid")
        FutureUnlessShutdown.pure(().asRight)

      case Right(missingIds) =>
        if (missingIds.isEmpty) {
          logger.info(s"All sequencer ids are known")

          /*
            Now that all sequencer ids are known, we attempt to create the connection of the successor
            and kick of the handshake with the successor, if any.
           */
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            sequencerConnectionSuccessorListener.checkAndCreateSynchronizerConfig(),
            failureMessage = "Unable to update synchronizer connection config",
          )

          FutureUnlessShutdown.pure(().asRight)
        } else {
          logger.info(s"Missing ids for sequencers $missingIds")
          val knownIds = connectionPool.getAllSequencerIds
          val newlyKnownIds = knownIds.filter { case (alias, _) => missingIds.contains(alias) }

          if (newlyKnownIds.isEmpty) {
            FutureUnlessShutdown.pure(s"No newly known id for sequencers $missingIds".asLeft)
          } else {
            logger.info(s"Retrieved ids for some sequencers: $newlyKnownIds")

            synchronizerConnectionConfigStore
              .setSequencerIds(psid, newlyKnownIds)
              .leftMap(_.message)
              .subflatMap { _ =>
                getMissingIds match {
                  case Left(error) => error.message.asLeft

                  case Right(missingIds) =>
                    if (missingIds.isEmpty) {
                      logger.info("Retrieved all sequencer ids.")
                      ().asRight
                    } else "ids still missing".asLeft
                }
              }
              .value
          }
        }
    }

  /** Retrieves the list of missing sequencer ids for the given psid.
    */
  private def getMissingIds
      : Either[SynchronizerConnectionConfigStore.UnknownPsid, Set[SequencerAlias]] =
    synchronizerConnectionConfigStore
      .get(psid)
      .map(
        _.config.sequencerConnections.aliasToConnection
          .collect { case (alias, connection) if connection.sequencerId.isEmpty => alias }
          .toSet
      )
}
