// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.db.DbMultiDomainCausalityStore
import com.digitalasset.canton.participant.store.memory.InMemoryMultiDomainCausalityStore
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}

trait MultiDomainCausalityStore extends AutoCloseable { self: NamedLogging =>

  implicit val ec: ExecutionContext

  /** The map from domains to the highest timestamp that has been published on that domain
    */
  @VisibleForTesting val highestSeen: TrieMap[DomainId, CantonTimestamp] = new TrieMap()

  // A cross-domain store used by transferring participants to propagate causality information from transfer-out to transfer-in
  // This can be accessed concurrently across different domains
  // TODO(i9515): Retrieve this state from the database instead of keeping it in memory
  protected val transferStore: TrieMap[TransferId, Map[LfPartyId, VectorClock]] =
    new TrieMap()

  // Use to synchronize between a transfer's source domain and target domain on a transferring participant
  protected val transferOutPromises: TrieMap[TransferId, Promise[Unit]] =
    new TrieMap()

  def awaitTransferOutRegistered(id: TransferId, parties: Set[LfPartyId])(implicit
      tc: TraceContext
  ): Future[Map[LfPartyId, VectorClock]] = {
    val storedPromise = transferOutPromises.getOrElseUpdate(id, Promise())
    val fetchStoredTransferOutState = transferOutState(id, parties).map(seen =>
      if (seen.isDefined) {
        val keys = seen.fold(Set.empty[LfPartyId])(m => m.keySet)
        ErrorUtil.requireState(
          parties.subsetOf(keys),
          s"Stored transfer-out state for $id misses parties ${parties.diff(keys)}",
        )
        storedPromise.trySuccess(())
      }
    )
    FutureUtil.doNotAwait(
      fetchStoredTransferOutState,
      failureMessage = s"Fetch stored transfer out state for $id",
    )

    storedPromise.future.map { case () =>
      val mapO = transferStore.get(id)
      ErrorUtil.requireState(mapO.isDefined, s"No causal state for transfer $id")
      transferOutPromises.remove(id)
      mapO.fold(Map.empty[LfPartyId, VectorClock])(m => m)
    }
  }

  /** Register that a transfer-out has completed with the provided causal state. This unblocks the corresponding
    * transfer-in.
    */
  def registerTransferOut(id: TransferId, vectorClocks: Set[VectorClock])(implicit
      tc: TraceContext
  ): Unit = {
    transferStore.updateWith(id) {
      case Some(value) =>
        val newMap = vectorClocks.map(v => v.partyId -> v).toMap
        ErrorUtil.requireState(
          value == newMap,
          s"Processed multiple TransferOut causal updates for the transfer $id. This is not expected to happen and is likely a bug. The two values are $value and $newMap.",
        )
        Some(value)
      case None => Some(vectorClocks.map(v => v.partyId -> v).toMap)
    }
    transferOutPromises.get(id).foreach { p =>
      p.trySuccess(())
    }
  }

  /** Register causality messages corresponding to transfer-out events on unconnected domains */
  def registerCausalityMessages(
      causalityMessages: List[CausalityMessage]
  )(implicit tc: TraceContext): Future[Unit] = {
    if (causalityMessages.nonEmpty) {
      val transferIds = causalityMessages.map(m => m.transferId).toSet

      //TODO(M40): Don't crash if there are causality messages for several transfers
      ErrorUtil.requireState(
        transferIds.sizeCompare(1) == 0,
        s"Received causality messages for several transfers at once." +
          s" The different transfer ids are $transferIds.",
      )

      val id = transferIds.headOption.getOrElse(
        ErrorUtil.internalError(new RuntimeException(s"Could not get head of list with length 1"))
      )

      for {
        persisted <- persistCausalityMessageState(id, causalityMessages.map(m => m.clock))
      } yield {

        // Add the state to our in-memory store
        causalityMessages.foreach { message =>
          val vc = message.clock

          transferStore.updateWith(id) {
            case Some(value) => Some(value + (vc.partyId -> vc))
            case None => Some(Map(vc.partyId -> vc))
          }
        }

        transferOutPromises.get(id).foreach(p => p.trySuccess(()))
      }

    } else Future.unit
  }

  def highestSeenOn(domain: DomainId): Option[CantonTimestamp] = highestSeen.get(domain)

  def registerSeen(domain: DomainId, timestamp: CantonTimestamp): Unit = {
    highestSeen.updateWith(domain) {
      case Some(value) =>
        // The existing highest seen timestamp might be higher than the timestamp we are observing
        // For example, this can happen during the replay of events after a shutdown
        Some(CantonTimestamp.max(timestamp, value))
      case None => Some(timestamp)
    }
    ()
  }

  /** Lookup the causal state at the transfer-out. Query it from the db if necessary.
    */
  private def transferOutState(id: TransferId, parties: Set[LfPartyId])(implicit
      tc: TraceContext
  ): Future[Option[Map[LfPartyId, VectorClock]]] = {
    transferStore.get(id) match {
      case Some(value) => Future.successful(Some(value))
      case None =>
        for {
          clocks <- loadTransferOutStateFromPersistentStore(id, parties)
        } yield {
          clocks.map { case (value) =>
            // Store all the transfer-out state in memory for now
            transferStore.put(id, value)
          }
          clocks
        }
    }
  }

  /** * Load the causal state at a transfer-out into memory.
    * @return Whether the causal state for the transfer-out was found.
    */
  protected def loadTransferOutStateFromPersistentStore(
      transferId: TransferId,
      parties: Set[LfPartyId],
  )(implicit tc: TraceContext): Future[Option[Map[LfPartyId, VectorClock]]]

  /** Write the causal state from a causality message to a persistent store */
  protected def persistCausalityMessageState(id: TransferId, vectorClocks: List[VectorClock])(
      implicit tc: TraceContext
  ): Future[Unit]

  @VisibleForTesting def inspectTransferStoreForTesting(
      id: TransferId
  ): Option[Map[LfPartyId, VectorClock]] =
    transferStore.get(id)

}

object MultiDomainCausalityStore {

  def apply(
      storage: Storage,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, tc: TraceContext): Future[MultiDomainCausalityStore] = {
    storage match {
      case _: MemoryStorage =>
        Future.successful(new InMemoryMultiDomainCausalityStore(loggerFactory))
      case storage: DbStorage =>
        DbMultiDomainCausalityStore(storage, indexedStringStore, timeouts, loggerFactory)
    }
  }
}
