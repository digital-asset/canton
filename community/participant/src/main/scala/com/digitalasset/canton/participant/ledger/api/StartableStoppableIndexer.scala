// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import java.util.concurrent.atomic.AtomicReference
import akka.actor.ActorSystem
import com.daml.ledger.api.health.{HealthStatus, ReportsHealth}
import com.daml.ledger.participant.state
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.indexer.{IndexerConfig, StandaloneIndexerServer}
import com.daml.platform.store.LfValueTranslationCache
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}

import scala.concurrent.{ExecutionContext, Future}

/** The StartableStoppableIndexer enables a canton participant node to start and stop the ledger api server's indexers
  * independently from the ledger api grpc server depending on whether the participant node is a High Availability
  * active or passive replica.
  *
  * @param indexerConfig indexer config to use when starting the indexer
  * @param metrics metrics for use by started indexer
  * @param lfValueTranslationCache lf value translation cache shared with the indexer's ledger api server
  * @param readService read service for indexer if and when started
  * @param indexerResourceInitial initial indexer resource if indexer already started
  */
class StartableStoppableIndexer(
    indexerConfig: IndexerConfig,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    readService: state.v2.ReadService,
    indexerResourceInitial: Option[Resource[ReportsHealth]],
    additionalMigrationPaths: Seq[String],
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, actorSystem: ActorSystem, loggingContext: LoggingContext)
    extends FlagCloseableAsync
    with NamedLogging {

  // Use a simple execution queue as locking to ensure only one start and stop run at a time.
  private val execQueue = new SimpleExecutionQueue()

  private val indexer = new AtomicReference[Option[Resource[ReportsHealth]]](indexerResourceInitial)

  private val replaceableHealthReporter = new AtomicReference[ReportsHealth](() =>
    HealthStatus.healthy
  )

  def setHealthReporter(maybeReportsHealth: Option[ReportsHealth]): Unit = {
    val _ = replaceableHealthReporter.set(maybeReportsHealth.getOrElse(() => HealthStatus.healthy))
  }

  // Wrapping the health reporter lets us switch in an actual ReportsHealth instance depending on whether an indexer is running.
  // Report healthy otherwise as the ledger api server has no way of dynamically adding or removing a ReportsHealth entry.
  def reportsHealthWrapper(): ReportsHealth = () => replaceableHealthReporter.get.currentHealth()

  /** Start the indexer and remember the resource.
    *
    * Assumes that indexer is currently stopped erroring otherwise. If asked to start during shutdown ignores start.
    *
    * A possible improvement to consider in the future is to abort start upon subsequent call to stop. As is the stop
    * will wait until an inflight start completes.
    */
  def start()(implicit traceContext: TraceContext): Future[Unit] =
    execQueue.execute(
      performUnlessClosingF {
        indexer.get match {
          case Some(_indexerAlreadyStarted) =>
            val err = s"Attempt to start indexer, but indexer already started"
            logger.error(err)
            Future.failed(new IllegalStateException(err))
          case None =>
            implicit val context: ResourceContext = ResourceContext(ec)
            val indexerResource = new StandaloneIndexerServer(
              readService,
              indexerConfig,
              metrics,
              lfValueTranslationCache,
              additionalMigrationPaths,
            ).acquire()

            FutureUtil.logOnFailure(
              indexerResource.asFuture.map { reportsHealth =>
                val _ = indexer.set(Some(indexerResource))
                setHealthReporter(Some(reportsHealth))
              },
              "Failed to start indexer",
            )
        }
      }.onShutdown {
        logger.info("Not starting indexer as we're shutting down")
      },
      "start indexer",
    )

  /** Stops the indexer, e.g. upon shutdown or when participant becomes passive.
    */
  def stop()(implicit traceContext: TraceContext): Future[Unit] =
    execQueue.execute(
      indexer.get match {
        case Some(indexerToStop) =>
          FutureUtil.logOnFailure(
            indexerToStop.release().map { _unit =>
              setHealthReporter(None)
              indexer.set(None)
            },
            "Failed to stop indexer",
          )
        case None =>
          logger.debug("Indexer already stopped")
          Future.unit
      },
      "stop indexer",
    )

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      logger.debug("Shutting down indexer")
      Seq(
        AsyncCloseable("indexer", stop(), timeouts.shutdownNetwork.duration)
      )
    }
}
