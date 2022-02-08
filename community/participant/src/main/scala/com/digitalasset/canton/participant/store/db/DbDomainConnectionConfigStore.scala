// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import cats.implicits._
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import io.functionmeta.functionFullName

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DbDomainConnectionConfigStore private[store] (
    storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DomainConnectionConfigStore
    with NamedLogging {
  import storage.api._
  import storage.converters._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("domain-connection-config-store")

  // Eagerly maintained cache of domain config indexed by DomainAlias
  private val domainConfigCache = TrieMap.empty[DomainAlias, DomainConnectionConfig]

  // Load all configs from the DB into the cache
  private[store] def initialize()(implicit
      traceContext: TraceContext
  ): Future[DomainConnectionConfigStore] =
    for {
      configs <- getAllInternal
      _ = configs.foreach(s => domainConfigCache.put(s.domain, s))
    } yield this

  private def getInternal(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, DomainConnectionConfig] = {
    processingTime.metric.eitherTEvent {
      EitherT {
        storage
          .query(
            sql"""select config from participant_domain_connection_configs where domain_alias = $domainAlias"""
              .as[DomainConnectionConfig]
              .headOption,
            functionFullName,
          )
          .map(_.toRight(MissingConfigForAlias(domainAlias)))
      }
    }
  }

  private def getAllInternal(implicit
      traceContext: TraceContext
  ): Future[Seq[DomainConnectionConfig]] =
    processingTime.metric.event {
      storage.query(
        sql"""select config from participant_domain_connection_configs"""
          .as[DomainConnectionConfig],
        functionFullName,
      )
    }

  def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = {
    domainConfigCache.clear()
    initialize().map(_ => ())
  }

  override def put(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, AlreadyAddedForAlias, Unit] = {

    val domainAlias = config.domain

    val insertAction: DbAction.WriteOnly[Int] = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert 
               /*+ IGNORE_ROW_ON_DUPKEY_INDEX ( PARTICIPANT_DOMAIN_CONNECTION_CONFIGS ( domain_alias ) ) */
               into participant_domain_connection_configs(domain_alias, config)
               values ($domainAlias, $config)"""
      case _ =>
        sqlu"""insert 
               into participant_domain_connection_configs(domain_alias, config)
               values ($domainAlias, $config)
               on conflict do nothing"""
    }

    for {
      nrRows <- EitherT.right(
        processingTime.metric.event(storage.update(insertAction, functionFullName))
      )
      _ <- nrRows match {
        case 1 => EitherTUtil.unit[AlreadyAddedForAlias]
        case 0 =>
          // If no rows were updated (due to conflict on alias), check if the existing config matches
          EitherT {
            getInternal(config.domain)
              .valueOr { err =>
                ErrorUtil.internalError(
                  new IllegalStateException(
                    s"No existing domain connection config found but failed to insert: $err"
                  )
                )
              }
              .map { existingConfig =>
                Either.cond(existingConfig == config, (), AlreadyAddedForAlias(domainAlias))
              }
          }
        case _ =>
          ErrorUtil.internalError(
            new IllegalStateException(s"Updated more than 1 row for connection configs: $nrRows")
          )
      }
    } yield {
      // Eagerly update cache
      val _ = domainConfigCache.put(config.domain, config)
    }
  }

  override def replace(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, MissingConfigForAlias, Unit] = {
    val domainAlias = config.domain
    val updateAction = sqlu"""update participant_domain_connection_configs
                              set config=$config
                              where domain_alias=$domainAlias"""
    for {
      _ <- getInternal(domainAlias) // Make sure an existing config exists for the alias
      _ <- EitherT.right(
        processingTime.metric.event(storage.update_(updateAction, functionFullName))
      )
    } yield {
      // Eagerly update cache
      val _ = domainConfigCache.put(config.domain, config)
    }
  }

  override def get(alias: DomainAlias): Either[MissingConfigForAlias, DomainConnectionConfig] =
    domainConfigCache.get(alias).toRight(MissingConfigForAlias(alias))

  override def getAll(): Seq[DomainConnectionConfig] = domainConfigCache.values.toSeq
}
