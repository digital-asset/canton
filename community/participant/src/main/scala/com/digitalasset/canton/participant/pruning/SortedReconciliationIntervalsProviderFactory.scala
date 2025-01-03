// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.StoreBasedDomainTopologyClient
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class SortedReconciliationIntervalsProviderFactory(
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def get(synchronizerId: SynchronizerId, subscriptionTs: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SortedReconciliationIntervalsProvider] =
    for {
      syncDomainPersistentState <- EitherT.fromEither[Future](
        syncDomainPersistentStateManager
          .get(synchronizerId)
          .toRight(s"Unable to get sync domain persistent state for domain $synchronizerId")
      )

      staticDomainParameters <- EitherT(
        syncDomainPersistentState.parameterStore.lastParameters.map(
          _.toRight(s"Unable to fetch static domain parameters for domain $synchronizerId")
        )
      )
      topologyFactory <- syncDomainPersistentStateManager
        .topologyFactoryFor(synchronizerId, staticDomainParameters.protocolVersion)
        .toRight(s"Can not obtain topology factory for $synchronizerId")
        .toEitherT[Future]
    } yield {
      val topologyClient = topologyFactory.createTopologyClient(
        StoreBasedDomainTopologyClient.NoPackageDependencies
      )
      topologyClient.updateHead(
        SequencedTime(subscriptionTs),
        EffectiveTime(subscriptionTs),
        ApproximateTime(subscriptionTs),
        potentialTopologyChange = true,
      )

      new SortedReconciliationIntervalsProvider(
        topologyClient,
        futureSupervisor,
        loggerFactory,
      )
    }

}
