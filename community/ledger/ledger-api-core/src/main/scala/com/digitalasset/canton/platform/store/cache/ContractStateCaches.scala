// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.{
  Active,
  Archived,
  ExistingContractStatus,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.ContractKeyStateValue.{Empty, Last}
import com.digitalasset.canton.platform.store.cache.ContractStateCaches.computeKeyStateChange
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Encapsulates the contract and key state caches with operations for mutating them. The caches are
  * used for serving contract activeness and key lookups for command interpretation performed during
  * command submission.
  *
  * @param keyState
  *   The contract key state cache.
  * @param contractState
  *   The contract state cache.
  * @param loggerFactory
  *   The logger factory.
  */
class ContractStateCaches(
    private[cache] val keyState: StateCache[GlobalKey, ContractKeyStateValue],
    private[cache] val contractState: StateCache[ContractId, ContractStateStatus],
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def push(
      eventsBatch: NonEmptyVector[ContractStateEvent],
      lastEventSeqId: Long,
  )(implicit traceContext: TraceContext): Unit = {
    val contractMappingsBuilder = Map.newBuilder[ContractId, ExistingContractStatus]
    val keyEventsBuilder = mutable.Map.empty[Key, VectorBuilder[ContractStateEvent]]

    eventsBatch.toVector.foreach {
      case activated: ContractStateEvent.Activated =>
        if (activated.isInitial) {
          contractMappingsBuilder.addOne(activated.contractId -> Active)
        }
        activated.globalKey.foreach(key =>
          keyEventsBuilder
            .getOrElseUpdate(key, new VectorBuilder[ContractStateEvent])
            .addOne(activated)
        )
      case deactivated: ContractStateEvent.Deactivated =>
        if (deactivated.isFinal) {
          contractMappingsBuilder.addOne(deactivated.contractId -> Archived)
        }
        deactivated.globalKey.foreach(key =>
          keyEventsBuilder
            .getOrElseUpdate(key, new VectorBuilder[ContractStateEvent])
            .addOne(deactivated)
        )
    }

    val validAt = lastEventSeqId
    val keyEvents = keyEventsBuilder.view.mapValues(_.result()).toMap

    keyState.putBatchCond(
      validAtEventSeqId = validAt,
      keys = keyEvents.keySet,
      change = computeKeyStateChange(keyEvents, _, logger),
    )
    contractState.putBatch(validAt, contractMappingsBuilder.result())
  }

  /** Reset the contract and key state caches to the specified offset. */
  def reset(lastPersistedLedgerEnd: Option[LedgerEnd]): Unit = {
    val index = lastPersistedLedgerEnd.map(_.lastEventSeqId).getOrElse(0L)
    keyState.reset(index)
    contractState.reset(index)
  }
}

object ContractStateCaches {

  /** Computes the upserts and invalidations to apply to the key state cache for a batch of per-key
    * event vectors, given the currently cached values for those keys.
    *
    * For each key, folds its events through [[ContractStateCaches.resolveKeyUpdatesFor]] starting
    * from the cached value (or `None` on cache miss). The final per-key result is routed into:
    *   - `upserts` when `Some(value)` is produced, or
    *   - `invalidations` when `None` is produced.
    */
  private[cache] def computeKeyStateChange(
      keyEvents: Map[Key, Vector[ContractStateEvent]],
      cached: Map[Key, ContractKeyStateValue],
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): (Map[Key, ContractKeyStateValue], Set[Key]) = {
    val upsertsBuilder = Map.newBuilder[Key, ContractKeyStateValue]
    val invalidationsBuilder = Set.newBuilder[Key]
    keyEvents.foreach { case (key, events) =>
      resolveKeyUpdatesFor(events, cached.get(key), logger) match {
        case Some(value) => discard(upsertsBuilder.addOne(key -> value))
        case None => discard(invalidationsBuilder.addOne(key))
      }
    }
    (upsertsBuilder.result(), invalidationsBuilder.result())
  }

  private[cache] def resolveKeyUpdatesFor(
      events: Vector[ContractStateEvent],
      initial: Option[ContractKeyStateValue],
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Option[ContractKeyStateValue] =
    events.foldLeft(initial)(applyContractStateEvent(_, _, logger))

  /** Applies a single [[ContractStateEvent]] to the current cached value for a key. Returns the new
    * cached value, or `None` to signal an invalidation.
    */
  private[cache] def applyContractStateEvent(
      current: Option[ContractKeyStateValue],
      event: ContractStateEvent,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Option[ContractKeyStateValue] =
    event match {
      case activated: ContractStateEvent.Activated =>
        val cid = activated.contractId
        val eventSeqId = activated.eventSequentialId
        current match {
          case None =>
            // Cache miss: we don't know whether other contracts exist for this key in the DB
            Some(Last(contractId = cid, eventSequentialId = eventSeqId, thereMightBeMore = true))
          case Some(Empty) =>
            // The cache knows that the key had no other contract activated.
            Some(Last(cid, eventSeqId, thereMightBeMore = false))
          case Some(Last(_, cachedSeqId, _)) =>
            if (cachedSeqId == eventSeqId) {
              // A contract that the cache already records as the last contract for the key is activated
              // twice. The entry is being removed from the cache.
              logger.warn(
                s"Key cache already contains the same contract $cid with eventSequientialId=$cachedSeqId, skipping update"
              )
              None
            } else {
              // The cache held a different contract as "last" for this key, the newest takes its place.
              Some(Last(cid, eventSeqId, thereMightBeMore = true))
            }
        }

      case deactivated: ContractStateEvent.Deactivated =>
        val deactivatedEventSequentialId = deactivated.deactivatedEventSequentialId
        current match {
          // Cache miss: the current state of the key is unknown. The next lookup will read through.
          case None => None
          case Some(Empty) =>
            // The cache claims the key has no contracts, yet we received a deactivation for it.
            logger.warn(
              s"Deactivation with deactivatedEventSequentialId=$deactivatedEventSequentialId encountered, but this entry is already deactivated in the cache"
            )
            Some(Empty)
          case Some(cached @ Last(cid, eventSeqId, thereMightBeMore)) =>
            if (eventSeqId == deactivatedEventSequentialId) {
              // The deactivation targets the exact contract recorded in the cache.
              if (!thereMightBeMore) {
                // This was the only contract for the key, so no contracts for the key exist.
                Some(Empty)
              } else {
                // There might be other contracts in the DB we don't know about.
                // Invalidate so the next lookup reads through.
                None
              }
            } else {
              // The deactivation targets a different contract than the one in the cache.
              // The cached "last" entry is still the most recent one known. Cache stays unchanged.
              if (!thereMightBeMore) {
                // Cache is not aware of any other contract for this key, yet we received a deactivation for a different contract.
                logger.warn(
                  s"Deactivation with deactivatedEventSequentialId=$deactivatedEventSequentialId on cache entry assigned to " +
                    s"contract $cid with sequentialId=$eventSeqId while thereMightBeMore was false"
                )
              }
              Some(cached)
            }
        }
    }

  def build(
      initialCacheEventSeqIdIndex: Long,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ContractStateCaches =
    new ContractStateCaches(
      contractState = ContractsStateCache(
        initialCacheEventSeqIdIndex,
        maxContractsCacheSize,
        metrics,
        loggerFactory,
      ),
      keyState =
        ContractKeyStateCache(initialCacheEventSeqIdIndex, maxKeyCacheSize, metrics, loggerFactory),
      loggerFactory = loggerFactory,
    )
}
