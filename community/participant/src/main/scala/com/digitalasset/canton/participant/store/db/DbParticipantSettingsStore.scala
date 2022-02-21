// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.store.ParticipantSettingsStore
import com.digitalasset.canton.participant.store.ParticipantSettingsStore.Settings
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, SetParameter}
import slick.sql.SqlAction

import scala.concurrent.{ExecutionContext, Future}

class DbParticipantSettingsStore(
    storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ParticipantSettingsStore
    with NamedLogging {

  private val client = 0 // dummy field used to enforce at most one row in the db

  private val processingTime = storage.metrics.loadGaugeM("participant-settings-store")

  private val executionQueue = new SimpleExecutionQueue()

  import storage.api._
  import storage.converters._

  private implicit val readSettings: GetResult[Settings] = GetResult { r =>
    val maxDirtyRequests = r.<<[Option[NonNegativeInt]]
    val maxRate = r.<<[Option[NonNegativeInt]]
    val maxDedupDuration = r.<<[Option[NonNegativeFiniteDuration]]
    val uniqueContractKeys = r.<<[Option[Boolean]]
    Settings(
      ResourceLimits(maxDirtyRequests = maxDirtyRequests, maxRate = maxRate),
      maxDedupDuration,
      uniqueContractKeys,
    )
  }

  override def refreshCache()(implicit traceContext: TraceContext): Future[Unit] =
    executionQueue.execute(
      processingTime.metric.event {
        for {
          settings <- storage.query(
            sql"select max_dirty_requests, max_rate, max_deduplication_duration, unique_contract_keys from participant_settings"
              .as[Settings]
              .headOption
              .map(_.getOrElse(Settings())),
            functionFullName,
          )

        } yield cache.set(Some(settings))
      },
      functionFullName,
    )

  override def writeResourceLimits(
      resourceLimits: ResourceLimits
  )(implicit traceContext: TraceContext): Future[Unit] = {
    processingTime.metric.event {
      // Put the new value into the cache right away so that changes become effective immediately.
      // This also ensures that value meets the object invariant of Settings.
      cache.updateAndGet(_.map(_.copy(resourceLimits = resourceLimits)))

      val ResourceLimits(maxDirtyRequests, maxRate) = resourceLimits

      val query = storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sqlu"""insert into participant_settings(max_dirty_requests, max_rate, client) values($maxDirtyRequests, $maxRate, $client)
                   on conflict(client) do update set max_dirty_requests = $maxDirtyRequests, max_rate = $maxRate"""

        case _: DbStorage.Profile.Oracle | _: DbStorage.Profile.H2 =>
          sqlu"""merge into participant_settings using dual on (1 = 1)
                 when matched then
                   update set max_dirty_requests = $maxDirtyRequests, max_rate = $maxRate
                 when not matched then
                   insert (max_dirty_requests, max_rate, client) values ($maxDirtyRequests, $maxRate, $client)"""
      }
      runQueryAndRefreshCache(query, functionFullName)
    }
  }

  override def insertMaxDeduplicationDuration(maxDeduplicationDuration: NonNegativeFiniteDuration)(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    insertOrUpdateIfNull("max_deduplication_duration", maxDeduplicationDuration)

  override def insertUniqueContractKeysMode(uniqueContractKeys: Boolean)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    insertOrUpdateIfNull("unique_contract_keys", uniqueContractKeys)

  private def insertOrUpdateIfNull[A: SetParameter](columnName: String, newValue: A)(implicit
      traceContext: TraceContext
  ): Future[Unit] = processingTime.metric.event {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into participant_settings(#$columnName, client) values ($newValue, $client)
               on conflict(client) do
                 update set #$columnName = $newValue where participant_settings.#$columnName is null 
              """
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into participant_settings using dual on (1 = 1)
               when matched and #$columnName is null then
                 update set #$columnName = $newValue
               when not matched then
                 insert (#$columnName, client) values ($newValue, $client)"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into participant_settings using dual on (1 = 1)
               when matched then
                 update set #$columnName = $newValue where #$columnName is null 
               when not matched then
                 insert (#$columnName, client) values ($newValue, $client)"""
    }
    runQueryAndRefreshCache(query, functionFullName)
  }

  private def runQueryAndRefreshCache(
      query: SqlAction[Int, NoStream, Effect.Write],
      operationName: String,
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage.update_(query, functionFullName).transformWith { res =>
      // Reload cache to make it consistent with the DB. Particularly important in case of concurrent writes.
      FutureUtil
        .logOnFailure(
          refreshCache(),
          s"An exception occurred while refreshing the cache. Keeping old value $settings.",
        )
        .transform(_ => res)
    }
}
