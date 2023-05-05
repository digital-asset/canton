// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  SignedTopologyTransactionX,
  TopologyChangeOp,
  TopologyChangeOpX,
  TopologyMappingX,
}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, blocking}

/** simple topology store holders
  *
  * domain stores need the domain-id, but we only know it after init.
  * therefore, we need some data structure to manage the store
  */
abstract class DomainTopologyStoreBase[
    ValidTx,
    StoredTx,
    SignedTx,
    T <: TopologyStoreCommon[DomainStore, ValidTx, StoredTx, SignedTx],
](
    storage: Storage,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
) extends AutoCloseable {
  private val store = new AtomicReference[Option[T]](None)
  def initOrGet(
      storeId: DomainStore
  )(implicit ec: ExecutionContext): T = blocking {
    synchronized {
      store.get() match {
        case None =>
          val item = createTopologyStore(storeId)
          store.set(Some(item))
          item
        case Some(value) =>
          if (storeId != value.storeId) {
            loggerFactory
              .getLogger(getClass)
              .error("Duplicate init of domain topology store with different domain-id!")
          }
          value
      }
    }
  }

  protected def createTopologyStore(
      storeId: DomainStore
  )(implicit ec: ExecutionContext): T

  def get(): Option[T] = store.get()

  override def close(): Unit = {
    store.getAndSet(None).foreach(_.close())
  }

}

class DomainTopologyStore(
    storage: Storage,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
) extends DomainTopologyStoreBase[ValidatedTopologyTransaction, StoredTopologyTransaction[
      TopologyChangeOp
    ], SignedTopologyTransaction[
      TopologyChangeOp
    ], TopologyStore[DomainStore]](
      storage,
      timeouts,
      loggerFactory,
      futureSupervisor,
    ) {
  override protected def createTopologyStore(
      storeId: DomainStore
  )(implicit ec: ExecutionContext): TopologyStore[DomainStore] =
    TopologyStore(storeId, storage, timeouts, loggerFactory, futureSupervisor)

}

class DomainTopologyStoreX(
    storage: Storage,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
) extends DomainTopologyStoreBase[
      GenericValidatedTopologyTransactionX,
      StoredTopologyTransactionX[TopologyChangeOpX, TopologyMappingX],
      SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX],
      TopologyStoreX[
        DomainStore
      ],
    ](
      storage,
      timeouts,
      loggerFactory,
      futureSupervisor,
    ) {
  override protected def createTopologyStore(
      storeId: DomainStore
  )(implicit ec: ExecutionContext): TopologyStoreX[DomainStore] =
    TopologyStoreX(storeId, storage, timeouts, loggerFactory)

}
