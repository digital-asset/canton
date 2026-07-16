// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerConnectivityStatusStore
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import slick.jdbc.SetParameter

import scala.concurrent.ExecutionContext

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class DbSynchronizerConnectivityStatusStore(
    psid: PhysicalSynchronizerId,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SynchronizerConnectivityStatusStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  private implicit val setParameterStaticSynchronizerParameters
      : SetParameter[StaticSynchronizerParameters] =
    StaticSynchronizerParameters.getVersionedSetParameter

  def setParameters(
      newParameters: StaticSynchronizerParameters
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // We do not check equality of the parameters on the serialized format in the DB query because serialization may
    // be different even though the parameters are the same
    val query =
      sqlu"""insert into par_synchronizer_connectivity_status(physical_synchronizer_id, params)
             values ($psid, $newParameters)
             on conflict do nothing"""

    storage.update(query, functionFullName).flatMap { rowCount =>
      if (rowCount == 1) FutureUnlessShutdown.unit
      else
        lastParameters.flatMap {
          case None =>
            FutureUnlessShutdown.failed(
              new IllegalStateException(
                "Insertion of synchronizer parameters failed even though no synchronizer parameters are present"
              )
            )
          case Some(old) if old == newParameters => FutureUnlessShutdown.unit
          case Some(old) =>
            FutureUnlessShutdown.failed(
              new IllegalArgumentException(
                s"Cannot overwrite old synchronizer parameters $old with $newParameters."
              )
            )
        }
    }
  }

  def lastParameters(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StaticSynchronizerParameters]] =
    storage
      .query(
        sql"select params from par_synchronizer_connectivity_status where physical_synchronizer_id=$psid"
          .as[StaticSynchronizerParameters]
          .headOption,
        functionFullName,
      )

  @volatile
  private var isTopologyInitializedCache = false
  @volatile
  private var hasStoreBeenInitialized = false

  override def setTopologyInitialized()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage
      .update_(
        sqlu"""update par_synchronizer_connectivity_status
                set is_topology_initialized = true
                where physical_synchronizer_id = $psid""",
        functionFullName,
      )
      .map(_ => isTopologyInitializedCache = true)

  override def isTopologyInitialized(implicit traceContext: TraceContext): Boolean = {
    ErrorUtil.requireState(
      hasStoreBeenInitialized,
      s"Invalid read access to ${getClass.getSimpleName} before it has been initialized.",
    )
    isTopologyInitializedCache
  }

  private def readIsTopologyInitializedFromStore()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    storage
      .query(
        sql"""select is_topology_initialized
              from par_synchronizer_connectivity_status
              where physical_synchronizer_id = $psid"""
          .as[Boolean]
          .headOption,
        functionFullName,
      )
      .map(_.getOrElse(false))

  def initialize()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    readIsTopologyInitializedFromStore().map { isTopoInitialized =>
      isTopologyInitializedCache = isTopoInitialized
      hasStoreBeenInitialized = true
    }
}
