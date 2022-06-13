// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import java.util.concurrent.atomic.AtomicReference
import cats.data.EitherT
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.{
  DomainAliasAndIdStore,
  DomainConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownDomain
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.topology.DomainId
import com.google.common.collect.{BiMap, HashBiMap}

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}

trait DomainAliasResolution extends AutoCloseable {
  def domainIdForAlias(alias: DomainAlias): Option[DomainId]
  def aliasForDomainId(id: DomainId): Option[DomainAlias]
  def connectionStateForDomain(id: DomainId): Option[DomainConnectionConfigStore.Status]
}

class DomainAliasManager private (
    configStore: DomainConnectionConfigStore,
    domainAliasAndIdStore: DomainAliasAndIdStore,
    initialDomainAliasMap: Map[DomainAlias, DomainId],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends NamedLogging
    with DomainAliasResolution {

  private val domainAliasMap =
    new AtomicReference[BiMap[DomainAlias, DomainId]](
      HashBiMap.create[DomainAlias, DomainId](initialDomainAliasMap.asJava)
    )

  def processHandshake(domainAlias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasManager.Error, Unit] =
    domainIdForAlias(domainAlias) match {
      // if a domain with this alias is restarted with new id, a different alias should be used to connect to it, since it is considered a new domain
      case Some(previousId) if previousId != domainId =>
        EitherT.leftT[Future, Unit](
          DomainAliasManager.DomainAliasDuplication(domainId, domainAlias, previousId)
        )
      case None => addMapping(domainAlias, domainId)
      case _ => EitherT.rightT[Future, DomainAliasManager.Error](())
    }

  def domainIdForAliasE(alias: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[SyncServiceError, DomainId] =
    domainIdForAlias(alias).toRight(SyncServiceUnknownDomain.Error(alias))

  def domainIdForAlias(alias: String): Option[DomainId] =
    DomainAlias.create(alias).toOption.flatMap(al => Option(domainAliasMap.get().get(al)))
  override def domainIdForAlias(alias: DomainAlias): Option[DomainId] = Option(
    domainAliasMap.get().get(alias)
  )
  override def aliasForDomainId(id: DomainId): Option[DomainAlias] = Option(
    domainAliasMap.get().inverse().get(id)
  )

  override def connectionStateForDomain(
      domainId: DomainId
  ): Option[DomainConnectionConfigStore.Status] = for {
    alias <- aliasForDomainId(domainId)
    conf <- configStore.get(alias).toOption
  } yield conf.status

  def aliases: Set[DomainAlias] = Set(domainAliasMap.get().keySet().asScala.toSeq: _*)
  def ids: Set[DomainId] = Set(domainAliasMap.get().values().asScala.toSeq: _*)

  private def addMapping(domainAlias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasManager.Error, Unit] =
    for {
      _ <- domainAliasAndIdStore
        .addMapping(domainAlias, domainId)
        .leftMap(error => DomainAliasManager.GenericError(error.toString))
      _ <- EitherT.right[DomainAliasManager.Error](updateCaches)
    } yield ()

  private def updateCaches(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- domainAliasAndIdStore.aliasToDomainIdMap.map(map =>
        domainAliasMap.set(HashBiMap.create[DomainAlias, DomainId](map.asJava))
      )
    } yield ()

  override def close(): Unit = Lifecycle.close(domainAliasAndIdStore)(logger)
}

object DomainAliasManager {
  def apply(
      configStore: DomainConnectionConfigStore,
      domainAliasAndIdStore: DomainAliasAndIdStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContextExecutor, traceContext: TraceContext): Future[DomainAliasManager] =
    for {
      domainAliasMap <- domainAliasAndIdStore.aliasToDomainIdMap
    } yield new DomainAliasManager(
      configStore,
      domainAliasAndIdStore,
      domainAliasMap,
      loggerFactory,
    )

  sealed trait Error
  final case class GenericError(reason: String) extends Error
  final case class DomainAliasDuplication(
      domainId: DomainId,
      alias: DomainAlias,
      previousDomainId: DomainId,
  ) extends Error {
    val message: String =
      s"Will not connect to domain $domainId using alias ${alias.unwrap}. The alias was previously used by another domain $previousDomainId, so please choose a new one."
  }
}
