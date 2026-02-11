// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
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

  private val missingBatchAggregator = {
    val processor =
      new BatchAggregator.Processor[BatchId, Boolean] {
        override def kind: String = "availability-missing-batch"

        override def logger: TracedLogger = DbAvailabilityStore.this.logger
        override def executeBatch(items: NonEmpty[Seq[Traced[BatchId]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[Boolean]] =
          findIfMissingBatches(items.map(_.value))

        override def prettyItem: Pretty[BatchId] = {
          import com.digitalasset.canton.logging.pretty.PrettyUtil.*
          prettyOfClass[BatchId](
            param("batchId", _.hash)
          )
        }
      }

    BatchAggregator(processor, batchAggregatorConfig)
  }

  private val lookupBatchAggregator = {
    val processor =
      new BatchAggregator.Processor[BatchId, OrderingRequestBatch] {
        override def kind: String = "availability-lookup-batch"

        override def logger: TracedLogger = DbAvailabilityStore.this.logger
        override def executeBatch(items: NonEmpty[Seq[Traced[BatchId]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[OrderingRequestBatch]] =
          lookupBatches(items.map(_.value)).map { result =>
            items.map(item => result(item.value))
          }

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
  private val lookupBatchCache
      : TracedAsyncLoadingCache[FutureUnlessShutdown, BatchId, OrderingRequestBatch] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, BatchId, OrderingRequestBatch](
      cache = cachingConfigs.bftOrderingBatchCache
        .buildScaffeine(loggerFactory)
        .weigher((_: Any, v: Any) =>
          v.asInstanceOf[OrderingRequestBatch].requests.map(_.value.payload.size()).sum
        ),
      loader = implicit traceContext => batchId => lookupBatchAggregator.run(batchId),
      metrics = Some(bftOrderingMetrics.availability.dissemination.batchCache),
    )(logger, "batchCache")

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
      () => addBatchBatchAggregator.run((batchId, batch)),
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

  private def findIfMissingBatches(batches: NonEmpty[Seq[BatchId]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[Boolean]] = {
    import DbStorage.Implicits.BuilderChain.*
    storage.synchronizeWithClosing("find-if-missing-batches") {
      val query =
        sql"""select id from ord_availability_batch where """ ++ DbStorage.toInClause("id", batches)
      storage
        .query(
          query.as[BatchId],
          functionFullName,
        )
        .map { existingBatches =>
          val existingBatchesAsSet = existingBatches.toSet
          batches.toSeq.map(existingBatchesAsSet.contains)
        }
    }
  }

  private def lookupBatches(batches: NonEmpty[Seq[BatchId]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[BatchId, OrderingRequestBatch]] = {
    import DbStorage.Implicits.BuilderChain.*
    storage.synchronizeWithClosing("find-if-missing-batches") {
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
    }
  }

  override def fetchBatches(batches: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] = {
    val name = fetchBatchesActionName

    NonEmpty.from(batches) match {
      case None =>
        PekkoFutureUnlessShutdown(
          name,
          () => FutureUnlessShutdown.pure(AvailabilityStore.AllBatches(Seq.empty)),
        )
      case Some(oneOrMoreBatchIds) =>
        val future: () => FutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] =
          () =>
            missingBatchAggregator.runMany(oneOrMoreBatchIds.map(Traced(_))).flatMap {
              batchesThatWeHave =>
                val missing =
                  oneOrMoreBatchIds.zip(batchesThatWeHave).collect { case (batchId, false) =>
                    batchId
                  }
                if (missing.nonEmpty) {
                  FutureUnlessShutdown.pure(AvailabilityStore.MissingBatches(missing.toSet))
                } else {
                  lookupBatchCache.getAll(oneOrMoreBatchIds).map { retrievedBatches =>
                    AvailabilityStore.AllBatches(batches.map(i => i -> retrievedBatches(i)))
                  }
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
            storage.update_(
              (sql"""delete from ord_availability_batch where """ ++ DbStorage
                .toInClause("id", oneOrMoreBatchIds)).asUpdate,
              functionFullName,
            )
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
        } yield AvailabilityStore.NumberOfRecords(
          batchesDeleted.toLong
        ),
      orderingStage = Some(functionFullName),
    )
}
