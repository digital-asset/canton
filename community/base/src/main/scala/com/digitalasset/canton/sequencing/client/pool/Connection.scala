// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import com.digitalasset.canton.health.AtomicHealthElement
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

/** A generic connection. This trait attempts to be independent of the underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait Connection extends FlagCloseable with NamedLogging {
  import Connection.*

  def name: String

  def health: ConnectionHealth

  /** Prepare the underlying transport so that it can be used to make calls. This must be called
    * before sending any command over the connection.
    *
    * The connection to the endpoint is not necessarily established immediately: for example, for
    * gRPC, this call opens a channel, but the connection is only established the first time it is
    * used.
    */
  def start()(implicit traceContext: TraceContext): Unit

  /** Stop the connection by closing the underlying transport's means of communication. Commands
    * cannot be sent after this call until the connection is [[start]]'ed again.
    *
    * For example, for gRPC, this closes the channel.
    */
  def stop()(implicit traceContext: TraceContext): Unit
}

object Connection {

  /** A connection represents just a single endpoint. To provide HA on a logical sequencer, the
    * operator can define multiple connections with the different endpoints. These connections will
    * then be handled in a round-robin load balancing way.
    *
    * @param name
    *   An identifier for this connection.
    * @param endpoint
    *   Connection endpoint (host and port).
    * @param transportSecurity
    *   Whether the connection uses TLS.
    * @param customTrustCertificates
    *   Custom X.509 certificates in PEM format, defined if using TLS.
    * @param expectedSequencerIdO
    *   If provided, defines the sequencer ID that the connected sequencer is expected to report. If
    *   empty, any sequencer ID will be accepted.
    */
  final case class ConnectionConfig(
      name: String,
      endpoint: Endpoint,
      transportSecurity: Boolean,
      customTrustCertificates: Option[ByteString],
      expectedSequencerIdO: Option[SequencerId],
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ConnectionConfig] = prettyOfClass(
      param("name", _.name.singleQuoted),
      param("endpoint", _.endpoint.toURI(transportSecurity)),
      param("transportSecurity", _.transportSecurity),
      param("customTrustCertificates", _.customTrustCertificates.nonEmpty),
      paramIfDefined("expectedSequencerId", _.expectedSequencerIdO),
    )
  }

  class ConnectionHealth(
      override val name: String,
      override val associatedHasRunOnClosing: HasRunOnClosing,
      protected override val logger: TracedLogger,
  ) extends AtomicHealthElement {
    override type State = ConnectionState

    override protected def prettyState: Pretty[State] = Pretty[State]

    override protected def initialHealthState: State = ConnectionState.Stopped

    override protected def closingState: State = ConnectionState.Stopped
  }

  sealed trait ConnectionError extends Product with Serializable
  object ConnectionError {

    /** An error happened in the underlying transport.
      */
    final case class TransportError(error: GrpcError) extends ConnectionError

    /** The connection is in an invalid state.
      */
    final case class InvalidStateError(message: String) extends ConnectionError
  }

  sealed trait ConnectionState extends Product with Serializable with PrettyPrinting
  object ConnectionState {

    /** The connection has started.
      */
    case object Started extends ConnectionState {
      override protected def pretty: Pretty[Started.type] = prettyOfObject[Started.type]
    }

    /** The connection has stopped.
      */
    case object Stopped extends ConnectionState {
      override protected def pretty: Pretty[Stopped.type] = prettyOfObject[Stopped.type]
    }
  }
}
