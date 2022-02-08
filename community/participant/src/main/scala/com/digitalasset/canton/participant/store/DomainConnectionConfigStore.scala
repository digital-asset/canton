// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.db.DbDomainConnectionConfigStore
import com.digitalasset.canton.participant.store.memory.InMemoryDomainConnectionConfigStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** The configured domains and their connection configuration
  */
trait DomainConnectionConfigStore {

  /** Stores a domain connection config. Primary identifier is the domain alias.
    * Will return an [[DomainConnectionConfigStore.AlreadyAddedForAlias]] error if a config for that alias already exists.
    */
  def put(config: DomainConnectionConfig)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AlreadyAddedForAlias, Unit]

  /** Replaces the config for the given alias.
    * Will return an [[DomainConnectionConfigStore.MissingConfigForAlias]] error if there is no config for the alias.
    */
  def replace(config: DomainConnectionConfig)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit]

  /** Retrieves the config for a given alias.
    * Will return an [[DomainConnectionConfigStore.MissingConfigForAlias]] error if there is no config for the alias.
    */
  def get(alias: DomainAlias): Either[MissingConfigForAlias, DomainConnectionConfig]

  /** Retrieves all configured domains connection configs
    */
  def getAll(): Seq[DomainConnectionConfig]

  /** Dump and refresh all connection configs.
    * Used when a warm participant replica becomes active to ensure it has accurate configs cached.
    */
  def refreshCache()(implicit traceContext: TraceContext): Future[Unit]
}

object DomainConnectionConfigStore {
  sealed trait Error extends Serializable with Product
  case class AlreadyAddedForAlias(alias: DomainAlias) extends Error
  case class MissingConfigForAlias(alias: DomainAlias) extends Error {
    override def toString: String = s"$alias is unknown. Has the domain been registered?"
  }

  def apply(storage: Storage, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainConnectionConfigStore] =
    storage match {
      case _: MemoryStorage =>
        Future.successful(new InMemoryDomainConnectionConfigStore(loggerFactory))
      case dbStorage: DbStorage =>
        new DbDomainConnectionConfigStore(dbStorage, loggerFactory).initialize()
    }
}
