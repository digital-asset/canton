// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.health.HealthQuasiComponent
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.pool.Connection.ConnectionConfig
import com.digitalasset.canton.sequencing.client.pool.SequencerConnectionPool.{
  SequencerConnectionPoolConfig,
  SequencerConnectionPoolError,
  SequencerConnectionPoolHealth,
}
import com.digitalasset.canton.sequencing.client.pool.{SequencerConnection, SequencerConnectionPool}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}

import scala.concurrent.ExecutionContextExecutor

/** This connection pool is meant to be used to create a sequencer client that connects directly to
  * an in-process sequencer. Needed for cases when the sequencer node itself needs to listen to
  * specific events such as identity events.
  */
class DirectSequencerConnectionPool(
    sequencer: Sequencer,
    mySynchronizerId: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticParameters: StaticSynchronizerParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionPool {
  import DirectSequencerConnectionPool.*

  val directConnection = new DirectSequencerConnection(
    directConnectionDummyConfig,
    sequencer,
    mySynchronizerId,
    sequencerId,
    staticParameters,
    timeouts,
    loggerFactory,
  )

  override def physicalSynchronizerIdO: Option[PhysicalSynchronizerId] = Some(mySynchronizerId)

  override def staticSynchronizerParametersO: Option[StaticSynchronizerParameters] = Some(
    staticParameters
  )

  override def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionPoolError, Unit] =
    EitherTUtil.unitUS

  override val config: SequencerConnectionPoolConfig = directPoolConfig

  override def updateConfig(newConfig: SequencerConnectionPoolConfig)(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionPoolError, Unit] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"$functionFullName is not implemented for DirectSequencerConnectionPool"
      )
    )

  override val health: SequencerConnectionPoolHealth =
    new SequencerConnectionPoolHealth.AlwaysHealthy("direct-pool-health", logger)

  override def getConnectionsHealthStatus: Seq[HealthQuasiComponent] = Seq(
    directConnection.health
  )

  override def nbSequencers: NonNegativeInt = NonNegativeInt.one

  override def nbConnections: NonNegativeInt = NonNegativeInt.one

  override def getConnections(requester: String, nb: PositiveInt, exclusions: Set[SequencerId])(
      implicit traceContext: TraceContext
  ): Set[SequencerConnection] = Set(directConnection)

  override def getOneConnectionPerSequencer(requester: String)(implicit
      traceContext: TraceContext
  ): Map[SequencerId, SequencerConnection] = Map(sequencerId -> directConnection)

  override def getAllConnections()(implicit traceContext: TraceContext): Seq[SequencerConnection] =
    Seq(directConnection)

  override val contents: Map[SequencerId, Set[SequencerConnection]] = Map(
    sequencerId -> Set(directConnection)
  )

  override def isThresholdStillReachable(
      threshold: PositiveInt,
      ignored: Set[ConnectionConfig],
      extraUndecided: NonNegativeInt,
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionPoolError.ThresholdUnreachableError, Unit] = Either.unit
}

object DirectSequencerConnectionPool {
  private val directConnectionDummyConfig = ConnectionConfig(
    name = "direct-connection",
    endpoint = Endpoint("dummy-endpoint-direct-connection", Port.tryCreate(0)),
    transportSecurity = false,
    customTrustCertificates = None,
    expectedSequencerIdO = None,
  )

  private val directPoolConfig = SequencerConnectionPoolConfig(
    connections = NonEmpty(Seq, directConnectionDummyConfig),
    trustThreshold = PositiveInt.one,
    // Not relevant for the direct pool
    minRestartConnectionDelay = NonNegativeFiniteDuration.Zero,
    maxRestartConnectionDelay = NonNegativeFiniteDuration.Zero,
    warnConnectionValidationDelay = NonNegativeFiniteDuration.Zero,
  )
}
