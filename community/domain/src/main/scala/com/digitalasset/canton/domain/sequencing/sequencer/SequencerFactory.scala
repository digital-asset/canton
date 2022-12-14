// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.Materializer
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

trait SequencerFactory {
  def create(
      domainId: DomainId,
      storage: Storage,
      clock: Clock,
      topologyClientMember: Member,
      domainSyncCryptoApi: DomainSyncCryptoClient,
      futureSupervisor: FutureSupervisor,
      initialState: Option[SequencerInitialState],
      localNodeParameters: CantonNodeParameters,
      protocolVersion: ProtocolVersion,
  )(implicit ec: ExecutionContext, tracer: Tracer, actorMaterializer: Materializer): Sequencer
}

object SequencerFactory {
  def database(
      config: DatabaseSequencerConfig,
      loggerFactory: NamedLoggerFactory,
  ): SequencerFactory =
    new SequencerFactory {
      override def create(
          domainId: DomainId,
          storage: Storage,
          clock: Clock,
          topologyClientMember: Member,
          domainSyncCryptoApi: DomainSyncCryptoClient,
          futureSupervisor: FutureSupervisor,
          initialState: Option[SequencerInitialState],
          localNodeParameters: CantonNodeParameters,
          sequencerProtocolVersion: ProtocolVersion,
      )(implicit
          ec: ExecutionContext,
          tracer: Tracer,
          actorMaterializer: Materializer,
      ): Sequencer = {
        val sequencer = DatabaseSequencer.single(
          config,
          localNodeParameters.processingTimeouts,
          storage,
          clock,
          domainId,
          topologyClientMember,
          sequencerProtocolVersion,
          domainSyncCryptoApi,
          loggerFactory,
        )

        config.testingInterceptor.map(_(clock)(sequencer)(ec)).getOrElse(sequencer)
      }
    }
}
