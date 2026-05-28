// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.InstrumentedGraph
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.{
  Active,
  Archived,
  ExistingContractStatus,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.{BatchLoaderMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.KeysPageQuery
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.{Metadata, StatusRuntimeException}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait Loader[Key, Value] {

  def load(key: Key)(implicit loggingContext: LoggingContextWithTrace): Future[Option[Value]]

}

class PekkoStreamParallelBatchedLoader[Key, Value](
    batchLoad: Seq[(Key, LoggingContextWithTrace)] => Future[Map[Key, Value]],
    createQueue: () => Source[
      (Key, LoggingContextWithTrace, Promise[Option[Value]]),
      BoundedSourceQueue[
        (Key, LoggingContextWithTrace, Promise[Option[Value]])
      ],
    ],
    maxBatchSize: Int,
    parallelism: Int,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends Loader[Key, Value]
    with NamedLogging {

  private val (queue, done) = createQueue()
    .batchN(
      maxBatchSize = maxBatchSize,
      maxBatchCount = parallelism,
    )
    .mapAsyncUnordered(parallelism) { batch =>
      Future
        .delegate(
          batchLoad(
            batch.view.map { case (key, loggingContext, _) => key -> loggingContext }.toSeq
          )
        )
        .transform {
          case Success(resultMap) =>
            batch.view.foreach { case (key, _, promise) =>
              promise.success(resultMap.get(key))
              ()
            }
            TryUtil.unit

          case Failure(t) =>
            batch.view.foreach { case (_, _, promise) =>
              promise.failure(
                t match {
                  case s: StatusRuntimeException =>
                    // creates a new array under the hood, which prevents un-synchronized concurrent changes in the gRPC serving layer
                    val newMetadata = new Metadata()
                    newMetadata.merge(s.getTrailers)
                    new StatusRuntimeException(s.getStatus, newMetadata)
                  case other => other
                }
              )
              ()
            }
            TryUtil.unit
        }
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  override def load(
      key: Key
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Value]] = {
    val promise = Promise[Option[Value]]()
    queue.offer((key, loggingContext, promise)) match {
      case QueueOfferResult.Enqueued => promise.future

      case QueueOfferResult.Dropped =>
        Future.failed(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("Too many pending contract lookups")(
              ErrorLoggingContext(logger, loggingContext)
            )
            .asGrpcError
        )

      // these should never happen, if this service is closed in the right order
      case QueueOfferResult.QueueClosed => Future.failed(new IllegalStateException("Queue closed"))
      case QueueOfferResult.Failure(t) => Future.failed(new IllegalStateException(t.getMessage))
    }
  }

  def closeAsync(): Future[Unit] = {
    queue.complete()
    done.map(_ => ())
  }
}

/** Efficient cross-request batching contract loader
  *
  * Note that both loaders operate on an identifier -> offset basis. The given offset of a request
  * serves as a lower bound for the states. The states can be newer, but not older. We still need to
  * have an upper bound of the requests as we don't want to read dirty states (due to parallel
  * insertion).
  */
trait ContractLoader {
  def contracts: Loader[(ContractId, Long), ExistingContractStatus]
  def keys: Loader[KeysPageQuery, (Vector[ContractId], Option[Long])]
}

object ContractLoader {

  private[events] def maxOffsetAndContextFromBatch[T](
      batch: Seq[((T, Long), LoggingContextWithTrace)],
      histogram: Histogram,
  ): (Long, LoggingContextWithTrace) = {
    val ((_, latestValidAtEventSeqId), usedLoggingContext) = batch
      .maxByOption(_._1._2)
      .getOrElse(
        throw new IllegalStateException("A batch should never be empty")
      )
    histogram.update(batch.size)(MetricsContext.Empty)
    (latestValidAtEventSeqId, usedLoggingContext)
  }

  private[events] def createQueue[K, V](maxQueueSize: Int, metrics: BatchLoaderMetrics)(implicit
      materializer: Materializer
  ): Source[(K, LoggingContextWithTrace, Promise[Option[V]]), BoundedSourceQueue[
    (K, LoggingContextWithTrace, Promise[Option[V]])
  ]] =
    InstrumentedGraph.queue(
      bufferSize = maxQueueSize,
      capacityCounter = metrics.bufferCapacity,
      lengthCounter = metrics.bufferLength,
      delayTimer = metrics.bufferDelay,
    )

  private def createContractBatchLoader(
      contractStore: LedgerApiContractStore,
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[PekkoStreamParallelBatchedLoader[
    (ContractId, Long),
    ExistingContractStatus,
  ]] =
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          (ContractId, Long),
          ExistingContractStatus,
        ](
          batchLoad = { batch =>
            val (latestValidAtEventSeqId, usedLoggingContext) = maxOffsetAndContextFromBatch(
              batch,
              metrics.index.db.activeContracts.batchSize,
            )
            val contractIds = batch.map(_._1._1)
            for {
              contractIdToInternalContractId <- contractStore
                .lookupBatchedInternalIdsNonReadThrough(contractIds)(
                  usedLoggingContext.traceContext
                )
              internalContractIds = contractIds.flatMap(contractIdToInternalContractId.get)
              contractStatuses <- dbDispatcher
                .executeSql(metrics.index.db.lookupActiveContractsDbMetrics)(
                  contractStorageBackend.activeContracts(
                    internalContractIds = internalContractIds,
                    beforeEventSeqId = latestValidAtEventSeqId,
                  )
                )(usedLoggingContext)
            } yield batch.view.flatMap { case ((contractId, beforeEventSeqId), _) =>
              contractIdToInternalContractId
                .get(contractId)
                .flatMap(contractStatuses.get)
                .map {
                  case true => Active
                  case false => Archived
                }
                .map((contractId, beforeEventSeqId) -> _)
            }.toMap
          },
          createQueue =
            () => ContractLoader.createQueue(maxQueueSize, metrics.index.db.activeContracts),
          maxBatchSize = maxBatchSize,
          parallelism = parallelism,
          loggerFactory = loggerFactory,
        )
      )(_.closeAsync())

  private def createContractKeyBatchLoader(
      contractStore: LedgerApiContractStore,
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[PekkoStreamParallelBatchedLoader[
    KeysPageQuery,
    (Vector[ContractId], Option[Long]),
  ]] =
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          KeysPageQuery,
          (Vector[ContractId], Option[Long]),
        ](
          batchLoad = { batch =>
            // we can use the latest offset as the API only requires us to not return a state older than the given offset
            val (latestValidAtEventSeqId, usedLoggingContext) =
              ContractLoader.maxOffsetAndContextFromBatch(
                batch.map { case (q, loggingContext) =>
                  (((), q.validAtEventSeqId), loggingContext)
                },
                metrics.index.db.activeContractKeys.batchSize,
              )
            val queries = batch.map(_._1)
            for {
              pageResults <- dbDispatcher
                .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
                  contractStorageBackend.contractKeysPlain(
                    queries,
                    latestValidAtEventSeqId,
                  )
                )(usedLoggingContext)
              allInternalIds = pageResults.flatMap(_.internalContractIds)
              internalIdToContractId <- contractStore
                .lookupBatchedContractIdsNonReadThrough(allInternalIds)(
                  usedLoggingContext.traceContext
                )
            } yield queries
              .zip(pageResults)
              .map { case (query, result) =>
                val contractIds =
                  result.internalContractIds.flatMap(internalIdToContractId.get)
                query -> (contractIds, result.nextPageToken)
              }
              .toMap
          },
          createQueue =
            () => ContractLoader.createQueue(maxQueueSize, metrics.index.db.activeContractKeys),
          maxBatchSize = maxBatchSize,
          parallelism = parallelism,
          loggerFactory = loggerFactory,
        )
      )(_.closeAsync())

  private def fetchOneKey(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      contractStore: LedgerApiContractStore,
      metrics: LedgerApiServerMetrics,
  )(
      query: KeysPageQuery
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Option[(Vector[ContractId], Option[Long])]] =
    for {
      pageResult <- dbDispatcher
        .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
          contractStorageBackend.contractKey(query)
        )
      contractIdLookup <- contractStore
        .lookupBatchedContractIdsNonReadThrough(pageResult.internalContractIds)(
          loggingContext.traceContext
        )
    } yield {
      val contractIds =
        pageResult.internalContractIds.flatMap(contractIdLookup.get)
      Some((contractIds, pageResult.nextPageToken))
    }

  def create(
      participantContractStore: LedgerApiContractStore,
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[ContractLoader] =
    for {
      contractsBatchLoader <- createContractBatchLoader(
        participantContractStore,
        contractStorageBackend,
        dbDispatcher,
        metrics,
        maxQueueSize,
        maxBatchSize,
        parallelism,
        loggerFactory,
      )
      contractKeysBatchLoader <-
        if (contractStorageBackend.supportsBatchKeyStateLookups)
          createContractKeyBatchLoader(
            participantContractStore,
            contractStorageBackend,
            dbDispatcher,
            metrics,
            maxQueueSize,
            maxBatchSize,
            parallelism,
            loggerFactory,
          ).map(Some(_))
        else ResourceOwner.successful(None)
    } yield {
      new ContractLoader {
        override final val contracts: Loader[(ContractId, Long), ExistingContractStatus] =
          new Loader[(ContractId, Long), ExistingContractStatus] {
            override def load(key: (ContractId, Long))(implicit
                loggingContext: LoggingContextWithTrace
            ): Future[Option[ExistingContractStatus]] = contractsBatchLoader.load(key)
          }
        override final val keys: Loader[KeysPageQuery, (Vector[ContractId], Option[Long])] =
          contractKeysBatchLoader match {
            case Some(batchLoader) =>
              new Loader[KeysPageQuery, (Vector[ContractId], Option[Long])] {
                override def load(key: KeysPageQuery)(implicit
                    loggingContext: LoggingContextWithTrace
                ): Future[Option[(Vector[ContractId], Option[Long])]] = batchLoader.load(key)
              }
            case None =>
              new Loader[KeysPageQuery, (Vector[ContractId], Option[Long])] {
                override def load(key: KeysPageQuery)(implicit
                    loggingContext: LoggingContextWithTrace
                ): Future[Option[(Vector[ContractId], Option[Long])]] =
                  fetchOneKey(
                    contractStorageBackend,
                    dbDispatcher,
                    participantContractStore,
                    metrics,
                  )(key)
              }
          }
      }
    }

  val dummyLoader = new ContractLoader {
    override final val contracts: Loader[(ContractId, Long), ExistingContractStatus] =
      new Loader[(ContractId, Long), ExistingContractStatus] {
        override def load(key: (ContractId, Long))(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[ExistingContractStatus]] = Future.successful(None)
      }
    override final val keys: Loader[KeysPageQuery, (Vector[ContractId], Option[Long])] =
      new Loader[KeysPageQuery, (Vector[ContractId], Option[Long])] {
        override def load(key: KeysPageQuery)(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[(Vector[ContractId], Option[Long])]] = Future.successful(None)
      }
  }
}
