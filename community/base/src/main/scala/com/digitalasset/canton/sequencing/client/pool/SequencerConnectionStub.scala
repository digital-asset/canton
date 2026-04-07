// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContextExecutor

/** A generic stub to interact with a sequencer. This trait attempts to be independent of the
  * underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait SequencerConnectionStub {
  import SequencerConnectionStub.*

  def getApiName(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError.ConnectionError, String]

  def performHandshake(
      clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
      minimumProtocolVersion: Option[ProtocolVersion],
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, HandshakeResponse]

  def getSynchronizerAndSequencerIds(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    (PhysicalSynchronizerId, SequencerId),
  ]

  def getStaticSynchronizerParameters(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, StaticSynchronizerParameters]
}

object SequencerConnectionStub {
  sealed trait SequencerConnectionStubError extends Product with Serializable
  object SequencerConnectionStubError {

    /** An error happened with the underlying connection.
      */
    final case class ConnectionError(error: Connection.ConnectionError)
        extends SequencerConnectionStubError

    /** A deserialization error happened when decoding a response.
      */
    final case class DeserializationError(message: String) extends SequencerConnectionStubError
  }
}

trait SequencerConnectionStubFactory {
  def createStub(connection: Connection, metricsContext: MetricsContext)(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectionStub

  def createUserStub(
      connection: Connection,
      clientAuth: GrpcSequencerClientAuth,
      metricsContext: MetricsContext,
      timeouts: ProcessingTimeout,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
  ): UserSequencerConnectionStub
}
