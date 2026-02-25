// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.caching.{CaffeineCache, ConcurrentCache}
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.{BatchAggregatorConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore, ToDbPrimitive}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.BatchAggregator
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class DbAvailabilityStore(
    batchAggregatorConfig: BatchAggregatorConfig,
    cachingConfigs: CachingConfigs,
    bftOrderingMetrics: BftOrderingMetrics,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends AvailabilityStore[PekkoEnv]
    with DbStore {

  import storage.api.*
  private val profile = storage.profile
  private val converters = storage.converters

  private val addBatchBatchAggregator = {
    val processor =
      new BatchAggregator.Processor[(BatchId, OrderingRequestBatch), Unit] {

        override val kind: String = "Add availability batches"

        override val logger: TracedLogger = DbAvailabilityStore.this.logger

        override def executeBatch(
            items: NonEmpty[Seq[Traced[(BatchId, OrderingRequestBatch)]]]
        )(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[Unit]] =
          // Sorting should prevent deadlocks in Postgres when using concurrent clashing batched inserts
          //  with idempotency "on conflict do nothing" clauses.
          runAddBatches(items.sortBy(_.value._1).map(_.value))
            .map(_ => Seq.fill(items.size)(()))

        override def prettyItem: Pretty[(BatchId, OrderingRequestBatch)] = {
          import com.digitalasset.canton.logging.pretty.PrettyUtil.*
          prettyOfClass[(BatchId, OrderingRequestBatch)](
            param("batchId", _._1.hash)
          )
        }
      }

    BatchAggregator(processor, batchAggregatorConfig)
  }

  private val fetchBatchAggregator = {
    val processor =
      new BatchAggregator.Processor[BatchId, Option[OrderingRequestBatch]] {
        override def kind: String = "availability-lookup-batch"

        override def logger: TracedLogger = DbAvailabilityStore.this.logger
        override def executeBatch(items: NonEmpty[Seq[Traced[BatchId]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[Option[OrderingRequestBatch]]] =
          lookupBatches(items.map(_.value))

        override def prettyItem: Pretty[BatchId] = {
          import com.digitalasset.canton.logging.pretty.PrettyUtil.*
          prettyOfClass[BatchId](
            param("batchId", _.hash)
          )
        }
      }

    BatchAggregator(processor, batchAggregatorConfig)
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private val lookupBatchCache: ConcurrentCache[BatchId, OrderingRequestBatch] =
    CaffeineCache(
      builder = cachingConfigs.bftOrderingBatchCache
        .buildScaffeine(loggerFactory)
        .underlying
        .weigher((_: Any, v: Any) =>
          v.asInstanceOf[OrderingRequestBatch].requests.map(_.value.payload.size()).sum
        ),
      metrics = Some(bftOrderingMetrics.availability.dissemination.batchCache),
    )

  private implicit def readOrderingRequestBatch: GetResult[OrderingRequestBatch] =
    converters.getResultByteArray.andThen { bytes =>
      ProtoConverter.protoParserArray(v30.Batch.parseFrom)(bytes) match {
        case Left(error) =>
          throw new DbDeserializationException(s"Could not deserialize proto request batch: $error")
        case Right(value) =>
          OrderingRequestBatch.fromProtoV30(value) match {
            case Left(error) =>
              throw new DbDeserializationException(s"Could not parse batch: $error")
            case Right(value) => value
          }
      }
    }

  private implicit val setOrderingRequestBatch: SetParameter[OrderingRequestBatch] = { (or, pp) =>
    val array = or.toProtoV30.toByteArray
    converters.setParameterByteArray(array, pp)
  }

  private implicit def readBatchId: GetResult[BatchId] = GetResult { r =>
    BatchId.fromHexString(r.nextString()) match {
      case Left(error) =>
        throw new DbDeserializationException(s"Could not deserialize hash: $error")
      case Right(batchId: BatchId) =>
        batchId
    }
  }

  private implicit val batchIdToPrimitive: ToDbPrimitive[BatchId, String68] = ToDbPrimitive(
    _.hash.toLengthLimitedHexString
  )

  override def addBatch(
      batchId: BatchId,
      batch: OrderingRequestBatch,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = addBatchActionName(batchId)
    PekkoFutureUnlessShutdown(
      name,
      () =>
        lookupBatchCache.getIfPresent(batchId) match {
          case Some(_) =>
            // batch already in cache we don't need to do add it again
            FutureUnlessShutdown.unit
          case None =>
            addBatchBatchAggregator.run((batchId, batch))
        },
      orderingStage = Some(functionFullName),
    )
  }

  private def runAddBatches(
      batches: Seq[(BatchId, OrderingRequestBatch)]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] =
    storage.synchronizeWithClosing("add-batches") {
      val insertSql =
        profile match {
          case _: Postgres =>
            """insert into ord_availability_batch
                      values (?, ?, ?)
                      on conflict (id) do nothing"""
          case _: H2 =>
            """merge into ord_availability_batch using dual
                     on (id = ?1)
                     when not matched then
                       insert (id, batch, epoch_number)
                       values (?1, ?2, ?3)"""
        }

      storage
        .runWrite(
          DbStorage
            .bulkOperation(insertSql, batches, storage.profile) { pp => msg =>
              pp >> msg._1
              pp >> msg._2
              pp >> msg._2.epochNumber
            },
          functionFullName,
          maxRetries = 1,
        )
        .map { results =>
          batches.view.zip(results).filter(_._2 != 0).map(_._1).foreach {
            case (batchId, orderingRequestBatch) =>
              lookupBatchCache.put(batchId, orderingRequestBatch)
          }
        }
    }

  private def lookupBatches(batches: NonEmpty[Seq[BatchId]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[Option[OrderingRequestBatch]]] = {
    import DbStorage.Implicits.BuilderChain.*
    storage.synchronizeWithClosing("lookup-batches") {
      val query =
        sql"""select id, batch from ord_availability_batch where """ ++ DbStorage.toInClause(
          "id",
          batches,
        )
      storage
        .query(
          query.as[(BatchId, OrderingRequestBatch)],
          functionFullName,
        )
        .map(_.toMap)
        .map(result => batches.toSeq.map(result.get))
    }
  }

  override def fetchBatches(batches: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] = {
    val name = fetchBatchesActionName

    val (missingBatchIds, presentInCacheIds) = batches.partitionMap { id =>
      lookupBatchCache.getIfPresent(id) match {
        case Some(value) => Right(id -> value)
        case None => Left(id)
      }
    }
    val presentCache = presentInCacheIds.toMap

    NonEmpty.from(missingBatchIds) match {
      case None =>
        // All batches are already in cache no lookup required

        PekkoFutureUnlessShutdown(
          name,
          () =>
            FutureUnlessShutdown.pure(
              AvailabilityStore.AllBatches(batches.map(id => id -> presentCache(id)))
            ),
        )
      case Some(oneOrMoreBatchesMissing) =>
        val future: () => FutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] =
          () =>
            fetchBatchAggregator.runMany(oneOrMoreBatchesMissing.map(Traced(_))).map {
              batchesThatWeHave =>
                val (stillMissing, newBatchMappingsNotInCache) =
                  oneOrMoreBatchesMissing.zip(batchesThatWeHave).partitionMap {
                    case (batchId, None) =>
                      Left(batchId)
                    case (batchId, Some(value)) =>
                      Right(batchId -> value)
                  }
                val resultMap = newBatchMappingsNotInCache.toMap
                lookupBatchCache.putAll(resultMap)
                if (stillMissing.nonEmpty) {
                  AvailabilityStore.MissingBatches(stillMissing.toSet)
                } else {
                  AvailabilityStore.AllBatches(batches.map { id =>
                    id -> presentCache.getOrElse(id, resultMap(id))
                  })
                }
            }
        PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
    }

  }

  override def gc(staleBatchIds: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    PekkoFutureUnlessShutdown(
      gcName,
      () =>
        NonEmpty.from(staleBatchIds) match {
          case Some(oneOrMoreBatchIds) =>
            import DbStorage.Implicits.BuilderChain.*
            storage
              .update_(
                (sql"""delete from ord_availability_batch where """ ++ DbStorage
                  .toInClause("id", oneOrMoreBatchIds)).asUpdate,
                functionFullName,
              )
              .map(_ => lookupBatchCache.invalidateAll(staleBatchIds))
          case None => FutureUnlessShutdown.unit
        },
      orderingStage = Some(functionFullName),
    )

  override def loadNumberOfRecords(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[AvailabilityStore.NumberOfRecords] =
    PekkoFutureUnlessShutdown(
      loadNumberOfRecordsName,
      () =>
        storage.query(
          (for {
            numberOfBatches <- sql"""select count(*) from ord_availability_batch""".as[Long].head
          } yield AvailabilityStore.NumberOfRecords(numberOfBatches)),
          functionFullName,
        ),
      orderingStage = Some(functionFullName),
    )

  override def prune(epochNumberExclusive: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[AvailabilityStore.NumberOfRecords] =
    PekkoFutureUnlessShutdown(
      pruneName(epochNumberExclusive),
      () =>
        for {
          batchesDeleted <- storage.update(
            sqlu""" delete from ord_availability_batch where epoch_number < $epochNumberExclusive """,
            functionFullName,
          )
          _ = lookupBatchCache.clear { case (_, batch) => batch.epochNumber < epochNumberExclusive }
        } yield AvailabilityStore.NumberOfRecords(
          batchesDeleted.toLong
        ),
      orderingStage = Some(functionFullName),
    )
}
