// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.db

import cats.syntax.parallel.*
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore.BatchIdAndEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.updateBulk
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
      new BatchAggregator.Processor[(BatchId, OrderingRequestBatch), Boolean] {

        override val kind: String = "Add availability batches"

        override val logger: TracedLogger = DbAvailabilityStore.this.logger

        override def executeBatch(
            items: NonEmpty[Seq[Traced[(BatchId, OrderingRequestBatch)]]]
        )(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[Boolean]] =
          // Sorting should prevent deadlocks in Postgres when using concurrent clashing batched inserts
          //  with idempotency "on conflict do nothing" clauses.
          runAddBatches(items.sortBy { case Traced(batchId -> _) => batchId }.map(_.value))

        override def prettyItem: Pretty[(BatchId, OrderingRequestBatch)] = {
          import com.digitalasset.canton.logging.pretty.PrettyUtil.*
          prettyOfClass[(BatchId, OrderingRequestBatch)](
            param("batchId", { case (batchId, _) => batchId.hash })
          )
        }
      }

    BatchAggregator(processor, batchAggregatorConfig)
  }

  private val fetchBatchAggregator = {
    val processor =
      new BatchAggregator.Processor[BatchIdAndEpochNumber, Option[OrderingRequestBatch]] {
        override def kind: String = "availability-lookup-batch"

        override def logger: TracedLogger = DbAvailabilityStore.this.logger
        override def executeBatch(items: NonEmpty[Seq[Traced[BatchIdAndEpochNumber]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[Option[OrderingRequestBatch]]] =
          lookupBatches(items.map(_.value))

        override def prettyItem: Pretty[BatchIdAndEpochNumber] = {
          import com.digitalasset.canton.logging.pretty.PrettyUtil.*
          prettyOfClass[BatchIdAndEpochNumber](
            param("batchId", _.batchId.hash),
            param("epochNumber", _.epochNumber),
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
  ): PekkoFutureUnlessShutdown[Boolean] = {
    val name = addBatchActionName(batchId)
    PekkoFutureUnlessShutdown(
      name,
      () =>
        lookupBatchCache.getIfPresent(batchId) match {
          case Some(_) =>
            // batch already in cache we don't need to do add it again
            FutureUnlessShutdown.pure(false)
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
  ): FutureUnlessShutdown[Seq[Boolean]] =
    storage.synchronizeWithClosing("add-batches") {
      val insertSql =
        profile match {
          case _: Postgres =>
            """insert into ord_availability_batch
                      values (?, ?, ?)
                      on conflict (epoch_number, id) do nothing"""
          case _: H2 =>
            """merge into ord_availability_batch using dual
                     on (id = ?1 and epoch_number = ?3)
                     when not matched then
                       insert (id, batch, epoch_number)
                       values (?1, ?2, ?3)"""
        }

      // Batch insertion is not strictly idempotent, because it returns whether the batch was actually inserted or
      //  was already present, but in the worst case (e.g. connection broken after insert successful),
      //  when retried, it will return that no batch has been updated, which in this case is acceptable,
      //  as it would only cause the batch quota checks to allow for a bit more batches than they should
      //  and an attacker doesn't typically control the connection to the database.
      updateBulk(
        storage,
        DbStorage
          .bulkOperation(
            insertSql,
            // Sorting should prevent deadlocks in Postgres when using concurrent clashing batched inserts
            //  with idempotency "on conflict do nothing" clauses.
            batches.sortBy { case (batchId, _) => batchId },
            storage.profile,
          ) { pp => msg =>
            val (batchId, orderingRequestBatch) = msg
            pp >> batchId
            pp >> orderingRequestBatch
            pp >> orderingRequestBatch.epochNumber
          },
        functionFullName,
      )
        .map { results =>
          batches.view
            .zip(results)
            .map { case ((batchId, orderingRequestBatch), numRowsUpdated) =>
              val didUpdate = numRowsUpdated != 0
              if (didUpdate) {
                lookupBatchCache.put(batchId, orderingRequestBatch)
              }
              didUpdate
            }
            .toSeq
        }
    }

  private def lookupBatches(batches: NonEmpty[Seq[BatchIdAndEpochNumber]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[Option[OrderingRequestBatch]]] =
    storage.synchronizeWithClosing("lookup-batches") {
      import DbStorage.Implicits.BuilderChain.*

      val (batchsWithEpoch0, otherBatches) = batches.partition(_.epochNumber == EpochNumber(0))

      // If a batch is requested with epoch number 0, it can be one of the following cases:
      // - It was remotely requested by a node on an older version, whose remote batch request did not yet include an
      //    epoch number, in which case the protobuf request by default deserealizes the empty epoch number field as 0.
      //    In that case, we don't want to use the epoch number in the query, just the batch id.
      // - The epoch number 0 was deliberately requested. Because we cannot differentiate between the 2 cases, we will
      //    still perform this query without the epoch number, which will be of worse performance, but hopefully not that
      //    bad considering that at that point, we probably don't have too much data in this table yet.
      val query1 = NonEmpty
        .from(batchsWithEpoch0.map(_.batchId))
        .map(batchIds =>
          (sql"""select id, batch from ord_availability_batch where """ ++ DbStorage
            .toInClause("id", batchIds)).as[(BatchId, OrderingRequestBatch)]
        )

      // Otherwise if a non zero epoch number is given, this is included in the query, which improves its performance
      // significantly, especially in the presence of many partitions.
      val query2 = NonEmpty
        .from(otherBatches)
        .map(batchInfos =>
          sql"""select id, batch from ord_availability_batch where (id, epoch_number) IN (#${batchInfos
              .map(p => s"('${batchIdToPrimitive.toDbPrimitive(p.batchId)}', ${p.epochNumber})")
              .mkString(", ")})""".as[(BatchId, OrderingRequestBatch)]
        )

      import cats.syntax.parallel.*

      (query1.toList ++ query2.toList)
        .parTraverse(query => storage.query(query, functionFullName).map(_.toMap))
        .map(maps => maps.foldLeft(Map[BatchId, OrderingRequestBatch]())(_ ++ _))
        .map(result => batches.toSeq.map(_.batchId).map(result.get))
    }

  override def fetchBatches(batches: Seq[BatchIdAndEpochNumber])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] = {
    val name = fetchBatchesActionName

    val (missingBatches, presentInCacheBatches) = batches.partitionMap { batch =>
      lookupBatchCache.getIfPresent(batch.batchId) match {
        case Some(value) => Right(batch -> value)
        case None => Left(batch)
      }
    }

    val presentCache = presentInCacheBatches.toMap

    NonEmpty.from(missingBatches) match {
      case None =>
        // All batches are already in cache no lookup required

        PekkoFutureUnlessShutdown(
          name,
          () =>
            FutureUnlessShutdown.pure(
              AvailabilityStore.AllBatches(
                batches.map(batchInfo => batchInfo.batchId -> presentCache(batchInfo))
              )
            ),
        )
      case Some(oneOrMoreBatchesMissing) =>
        val future: () => FutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] =
          () =>
            fetchBatchAggregator.runMany(oneOrMoreBatchesMissing.map(Traced(_))).map {
              batchesThatWeHave =>
                val (stillMissing, newBatchMappingsNotInCache) =
                  oneOrMoreBatchesMissing.zip(batchesThatWeHave).partitionMap {
                    case (batchInfo, None) =>
                      Left(batchInfo)
                    case (batchInfo, Some(value)) =>
                      Right(batchInfo -> value)
                  }
                val resultMap = newBatchMappingsNotInCache.toMap
                lookupBatchCache.putAll(resultMap.map { case (poa, requestBatch) =>
                  poa.batchId -> requestBatch
                })
                if (stillMissing.nonEmpty) {
                  AvailabilityStore.MissingBatches(stillMissing.toSet)
                } else {
                  AvailabilityStore.AllBatches(batches.map { batchInfo =>
                    batchInfo.batchId -> presentCache.getOrElse(batchInfo, resultMap(batchInfo))
                  })
                }
            }
        PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
    }

  }

  override def gc(staleBatchIds: Map[EpochNumber, Set[BatchId]])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    PekkoFutureUnlessShutdown(
      gcName,
      () =>
        staleBatchIds.toSeq
          .parTraverse { case (epochNumber, batchIds) =>
            NonEmpty.from(batchIds) match {
              case Some(oneOrMoreBatchIds) =>
                import DbStorage.Implicits.BuilderChain.*
                storage
                  .update_(
                    (sql"""delete from ord_availability_batch where epoch_number = $epochNumber and """ ++ DbStorage
                      .toInClause("id", oneOrMoreBatchIds)).asUpdate,
                    functionFullName,
                  )
                  .map(_ => lookupBatchCache.invalidateAll(batchIds))
              case None => FutureUnlessShutdown.unit
            }
          }
          .map(_ => ()),
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
