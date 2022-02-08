// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.DomainAliasAndIdStore.{
  DomainAliasAlreadyAdded,
  DomainIdAlreadyAdded,
  Error,
}
import com.digitalasset.canton.{DomainAlias, DomainId}
import com.digitalasset.canton.participant.store.RegisteredDomainsStore
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import scala.concurrent.{ExecutionContext, Future}

class InMemoryRegisteredDomainsStore(override protected val loggerFactory: NamedLoggerFactory)
    extends RegisteredDomainsStore
    with NamedLogging {

  private val domainAliasMap: BiMap[DomainAlias, DomainId] =
    HashBiMap.create[DomainAlias, DomainId]()

  private val lock = new Object()

  private implicit val ec: ExecutionContext = DirectExecutionContext(logger)

  override def addMapping(alias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Unit] = lock.synchronized {
    val swapped = for {
      _ <- Option(domainAliasMap.get(alias)).fold(Either.right[Either[Error, Unit], Unit](())) {
        oldDomainId =>
          Left(
            Either.cond(oldDomainId == domainId, (), DomainAliasAlreadyAdded(alias, oldDomainId))
          )
      }
      _ <- Option(domainAliasMap.inverse.get(domainId))
        .fold(Either.right[Either[Error, Unit], Unit](())) { oldAlias =>
          Left(Either.cond(oldAlias == alias, (), DomainIdAlreadyAdded(domainId, oldAlias)))
        }
    } yield {
      val _ = domainAliasMap.put(alias, domainId)
    }
    EitherT.fromEither[Future](swapped.swap.getOrElse(Right(())))
  }

  override def aliasToDomainIdMap(implicit
      traceContext: TraceContext
  ): Future[Map[DomainAlias, DomainId]] =
    lock.synchronized {
      import scala.jdk.CollectionConverters._
      Future.successful(Map(domainAliasMap.asScala.toSeq: _*))
    }

}
