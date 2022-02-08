// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainAliasResolution
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.{DomainAlias, DomainId}

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap

/** Read-only interface to the [[SyncDomainPersistentStateManager]] */
trait SyncDomainPersistentStateLookup {
  def getAll: Map[DomainId, SyncDomainPersistentState]
}

/** Manages domain state that needs to survive reconnects
  */
class SyncDomainPersistentStateManager(
    aliasResolution: DomainAliasResolution,
    protected val loggerFactory: NamedLoggerFactory,
) extends SyncDomainPersistentStateLookup
    with NamedLogging
    with AutoCloseable {

  private val domainStates: concurrent.Map[DomainId, SyncDomainPersistentState] =
    TrieMap[DomainId, SyncDomainPersistentState]()

  def put(state: SyncDomainPersistentState): Unit = {
    val domainId = state.domainId
    val previous = domainStates.putIfAbsent(domainId.item, state)
    if (previous.isDefined)
      throw new IllegalArgumentException(s"domain state already exists for $domainId")
  }

  def putIfAbsent(state: SyncDomainPersistentState): Unit = {
    val _ = domainStates.putIfAbsent(state.domainId.item, state)
  }

  def get(domainId: DomainId): Option[SyncDomainPersistentState] = domainStates.get(domainId)

  override def getAll: Map[DomainId, SyncDomainPersistentState] = domainStates.toMap

  def getByAlias(domainAlias: DomainAlias): Option[SyncDomainPersistentState] =
    for {
      domainId <- domainIdForAlias(domainAlias)
      res <- get(domainId)
    } yield res

  def domainIdForAlias(domainAlias: DomainAlias): Option[DomainId] =
    aliasResolution.domainIdForAlias(domainAlias)
  def aliasForDomainId(domainId: DomainId): Option[DomainAlias] =
    aliasResolution.aliasForDomainId(domainId)

  override def close(): Unit = Lifecycle.close(domainStates.values.toSeq: _*)(logger)
}
