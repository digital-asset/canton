// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import java.util.concurrent.ConcurrentHashMap

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class InMemoryDomainConnectionConfigStore(protected override val loggerFactory: NamedLoggerFactory)
    extends DomainConnectionConfigStore
    with NamedLogging {
  private implicit val ec: ExecutionContext = DirectExecutionContext(logger)

  private val configuredDomainMap =
    new ConcurrentHashMap[DomainAlias, DomainConnectionConfig].asScala

  override def put(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, AlreadyAddedForAlias, Unit] =
    EitherT.fromEither[Future](
      configuredDomainMap
        .putIfAbsent(config.domain, config)
        .fold[Either[AlreadyAddedForAlias, Unit]](Right(()))(existingConfig =>
          Either.cond(config == existingConfig, (), AlreadyAddedForAlias(config.domain))
        )
    )

  override def replace(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, MissingConfigForAlias, Unit] =
    configuredDomainMap.synchronized {
      EitherT.fromEither[Future](configuredDomainMap.get(config.domain) match {
        case Some(_) =>
          configuredDomainMap.put(config.domain, config)
          Right(())
        case None =>
          Left(MissingConfigForAlias(config.domain))
      })
    }

  override def get(alias: DomainAlias): Either[MissingConfigForAlias, DomainConnectionConfig] =
    configuredDomainMap.get(alias).toRight(MissingConfigForAlias(alias))

  override def getAll(): Seq[DomainConnectionConfig] = configuredDomainMap.values.toSeq

  /** We have no cache so is effectively a noop. */
  override def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  override def close(): Unit = ()
}
