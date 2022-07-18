// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.MediatorDeduplicationStore.DeduplicationData
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

trait MediatorDeduplicationStore extends NamedLogging {

  /** Stores deduplication data for a given uuid.
    */
  private val dataByUuid: TrieMap[UUID, NonEmpty[Set[DeduplicationData]]] = TrieMap()

  /** Stores deduplication data by expiration time (for efficient pruning). */
  private val uuidByExpiration: ConcurrentNavigableMap[CantonTimestamp, NonEmpty[Set[UUID]]] =
    new ConcurrentSkipListMap()

  protected val initialized: AtomicBoolean = new AtomicBoolean()

  /** Clients must call this method before any other method.
    *
    * The method populates in-memory caches and
    * deletes all data with `timestamp` greater than or equal to `deleteFromInclusive`.
    */
  def initialize(deleteFromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  private def requireInitialized()(implicit traceContext: TraceContext): Unit =
    ErrorUtil.requireState(initialized.get(), "The initialize method needs to be called first.")

  /** Yields the data stored for a given `uuid` that is not expired at `timestamp`.
    */
  def findUuid(uuid: UUID, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Set[DeduplicationData] = {
    requireInitialized()
    dataByUuid
      .get(uuid)
      .fold(Set.empty[DeduplicationData])(_.forgetNE)
      .filter(_.expireAfter >= timestamp)
  }

  @VisibleForTesting
  def allData()(implicit traceContext: TraceContext): Set[DeduplicationData] = {
    requireInitialized()
    dataByUuid.values.toSet.flatten
  }

  /** See the documentation of the other `store` method.
    */
  def store(data: DeduplicationData)(implicit traceContext: TraceContext): Future[Unit] = {
    requireInitialized()

    // Updating in-memory state synchronously, so that changes are effective when the method returns,
    // as promised in the scaladoc.

    dataByUuid.updateWith(data.uuid) {
      case None => Some(NonEmpty(Set, data))
      case Some(existing) => Some(existing.incl(data))
    }

    // The map uuidByExpiration is updated second, so that a concurrent call to prune
    // won't leave behind orphaned data in dataByUuid.
    uuidByExpiration.asScala.updateWith(data.expireAfter) {
      case None => Some(NonEmpty(Set, data.uuid))
      case Some(existing) => Some(existing.incl(data.uuid))
    }

    persist(data)
  }

  /** Persist data to the database. */
  protected def persist(data: DeduplicationData)(implicit traceContext: TraceContext): Future[Unit]

  /** Stores the given `uuid` together with `requestTime` and `expireAfter`.
    *
    * This method supports concurrent invocations.
    * Changes are effective as soon as this method returns.
    * If the store supports persistence, changes are persistent as soon as the returned future completes.
    */
  def store(uuid: UUID, requestTime: CantonTimestamp, expireAfter: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = store(DeduplicationData(uuid, requestTime, expireAfter))

  /** Delete all data with `expireAt` before than or equal to `upToInclusive`.
    *
    * If some data is concurrently stored and pruned, some data may remain in the in-memory caches and / or in the database.
    * Such data will be deleted by a subsequent call to `prune`.
    */
  def prune(upToInclusive: CantonTimestamp)(implicit traceContext: TraceContext): Future[Unit] = {
    requireInitialized()

    // Take a defensive copy so that we can safely iterate over it.
    val uuidByExpirationToPrune =
      uuidByExpiration.subMap(CantonTimestamp.MinValue, true, upToInclusive, true).asScala.to(Map)

    for ((expireAt, uuids) <- uuidByExpirationToPrune) {
      // Delete this first so that a concurrent call to `store` does not leave behind orphaned data.
      // Delete this only if `uuids` hasn't changed concurrently.
      uuidByExpiration.remove(expireAt, uuids)

      for (uuid <- uuids) {
        dataByUuid.updateWith(uuid) {
          case None => None
          case Some(existing) => NonEmpty.from(existing.filter(_.expireAfter > upToInclusive))
        }
      }
    }

    prunePersistentData(upToInclusive)
  }

  /** Delete all persistent data with `expireAt` before than or equal to `upToInclusive`.
    */
  protected def prunePersistentData(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

object MediatorDeduplicationStore {
  def apply(storage: Storage, loggerFactory: NamedLoggerFactory): MediatorDeduplicationStore =
    new InMemoryMediatorDeduplicationStore(loggerFactory)
  // TODO(i8900): support persistence
  // TODO(i8900): make sure to implement batching

  case class DeduplicationData(
      uuid: UUID,
      requestTime: CantonTimestamp,
      expireAfter: CantonTimestamp,
  )
}

class InMemoryMediatorDeduplicationStore(override protected val loggerFactory: NamedLoggerFactory)
    extends MediatorDeduplicationStore
    with NamedLogging {

  override def initialize(
      deleteFromInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    ErrorUtil.requireState(!initialized.get(), "The store most not be initialized more than once!")
    initialized.set(true)
    Future.unit
  }

  override protected def persist(data: DeduplicationData)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.unit

  override protected def prunePersistentData(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.unit
}
