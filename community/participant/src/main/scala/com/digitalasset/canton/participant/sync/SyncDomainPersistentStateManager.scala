// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.syntax.traverse._
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainAliasResolution
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

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
  private val protocolVersionCache: TrieMap[DomainId, ProtocolVersion] = TrieMap()

  def protocolVersionFor(
      domainId: DomainId
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Option[ProtocolVersion]] = {
    protocolVersionCache.get(domainId) match {
      case Some(protocolVersion) => Future.successful(Some(protocolVersion))
      case None =>
        get(domainId)
          .map(_.parameterStore)
          .traverse(_.lastParameters)
          .map(_.flatten)
          .map {
            case Some(staticDomainParameters) =>
              protocolVersionCache
                .put(domainId, staticDomainParameters.protocolVersion)
                .discard[Option[ProtocolVersion]]
              Some(staticDomainParameters.protocolVersion)

            case None => None
          }
    }
  }

  private val domainStates: concurrent.Map[DomainId, SyncDomainPersistentState] =
    TrieMap[DomainId, SyncDomainPersistentState]()

  def put(state: SyncDomainPersistentState): Unit = {
    val domainId = state.domainId
    val previous = domainStates.putIfAbsent(domainId.item, state)
    if (previous.isDefined)
      throw new IllegalArgumentException(s"domain state already exists for $domainId")
  }

  def putIfAbsent(state: SyncDomainPersistentState): Unit =
    domainStates.putIfAbsent(state.domainId.item, state).discard

  def get(domainId: DomainId): Option[SyncDomainPersistentState] = domainStates.get(domainId)

  override def getAll: Map[DomainId, SyncDomainPersistentState] = domainStates.toMap

  def getStatusOf(domainId: DomainId): Option[DomainConnectionConfigStore.Status] =
    this.aliasResolution.connectionStateForDomain(domainId)

  def getByAlias(domainAlias: DomainAlias): Option[SyncDomainPersistentState] =
    for {
      domainId <- domainIdForAlias(domainAlias)
      res <- get(domainId)
    } yield res

  def domainIdForAlias(domainAlias: DomainAlias): Option[DomainId] =
    aliasResolution.domainIdForAlias(domainAlias)
  def aliasForDomainId(domainId: DomainId): Option[DomainAlias] =
    aliasResolution.aliasForDomainId(domainId)

  override def close(): Unit =
    Lifecycle.close(domainStates.values.toSeq :+ aliasResolution: _*)(logger)
}
