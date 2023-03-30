// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, blocking}

/** simple topology store holders
  *
  * domain stores need the domain-id, but we only know it after init.
  * therefore, we need some data structure to manage the store
  */
abstract class DomainTopologyStoreBase[ValidTx, T <: BaseTopologyStore[DomainStore, ValidTx]](
    storage: Storage,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
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
) extends DomainTopologyStoreBase[ValidatedTopologyTransaction, TopologyStore[DomainStore]](
      storage,
      timeouts,
      loggerFactory,
    ) {
  override protected def createTopologyStore(
      storeId: DomainStore
  )(implicit ec: ExecutionContext): TopologyStore[DomainStore] =
    TopologyStore(storeId, storage, timeouts, loggerFactory)

}

class DomainTopologyStoreX(
    storage: Storage,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends DomainTopologyStoreBase[GenericValidatedTopologyTransactionX, TopologyStoreX[
      DomainStore
    ]](
      storage,
      timeouts,
      loggerFactory,
    ) {
  override protected def createTopologyStore(
      storeId: DomainStore
  )(implicit ec: ExecutionContext): TopologyStoreX[DomainStore] =
    TopologyStoreX(storeId, storage, timeouts, loggerFactory)

}
