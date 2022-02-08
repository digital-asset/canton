// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either._
import cats.syntax.traverse._
import cats.data.NonEmptyList
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.version.VersionedSequencerConnection
import com.digitalasset.canton.sequencing.client.http.HttpSequencerEndpoints
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, HasVersionedWrapperCompanion}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.grpc.netty.NettyChannelBuilder

import java.net.URI
import java.util.concurrent.Executor

/** Our [[com.digitalasset.canton.config.SequencerConnectionConfig]] provides a flexible structure for configuring how
  * the domain and its members talk to a sequencer. It however leaves much information intentionally optional so it can
  * be inferred at runtime based on information that may only be available at the point of creating a sequencer
  * connection (for instance defaulting to domain connection information that a user has provided in an admin command).
  * At this point these structures can then be constructed which contain all the mandatory details that sequencer clients
  * need to actually connect.
  */
sealed trait SequencerConnection
    extends Product
    with Serializable
    with HasVersionedWrapper[VersionedSequencerConnection]
    with HasProtoV0[v0.SequencerConnection]
    with PrettyPrinting {
  override def toProtoVersioned(version: ProtocolVersion): VersionedSequencerConnection =
    VersionedSequencerConnection(VersionedSequencerConnection.Version.V0(toProtoV0))

  override def toProtoV0: v0.SequencerConnection
}

case class HttpSequencerConnection(urls: HttpSequencerEndpoints, certificate: X509CertificatePem)
    extends SequencerConnection {

  override def toProtoV0: v0.SequencerConnection =
    v0.SequencerConnection(
      v0.SequencerConnection.Type.Http(
        v0.SequencerConnection.Http(
          urls.write.getHost,
          urls.write.getPort,
          Some(certificate.unwrap),
          urls.read.getHost,
          urls.read.getPort,
        )
      )
    )

  override def pretty: Pretty[HttpSequencerConnection] =
    prettyOfClass(
      param("urls", _.urls),
      param("certificate", _.certificate.unwrap),
    )
}

final case class GrpcSequencerConnection(
    endpoints: NonEmptyList[Endpoint],
    transportSecurity: Boolean,
    customTrustCertificates: Option[ByteString],
) extends SequencerConnection {
  def mkChannelBuilder(clientChannelBuilder: ClientChannelBuilder, tracePropagation: Propagation)(
      implicit executor: Executor
  ): NettyChannelBuilder =
    clientChannelBuilder
      .create(endpoints, transportSecurity, executor, customTrustCertificates, tracePropagation)

  override def toProtoV0: v0.SequencerConnection =
    v0.SequencerConnection(
      v0.SequencerConnection.Type.Grpc(
        v0.SequencerConnection.Grpc(
          endpoints.map(_.toURI(transportSecurity).toString).toList,
          transportSecurity,
          customTrustCertificates,
        )
      )
    )

  override def pretty: Pretty[GrpcSequencerConnection] =
    prettyOfClass(
      param("endpoints", _.endpoints.map(_.toURI(transportSecurity)).toList),
      param("transportSecurity", _.transportSecurity),
      param("customTrustCertificates", _.customTrustCertificates),
    )
}

object GrpcSequencerConnection {
  def create(
      connection: String,
      customTrustCertificates: Option[ByteString] = None,
  ): Either[String, GrpcSequencerConnection] =
    for {
      endpointsWithTlsFlag <- Endpoint.fromUris(NonEmptyList.one(new URI(connection)))
      (endpoints, useTls) = endpointsWithTlsFlag
    } yield GrpcSequencerConnection(endpoints, useTls, customTrustCertificates)

  def tryCreate(
      connection: String,
      customTrustCertificates: Option[ByteString] = None,
  ): GrpcSequencerConnection =
    create(connection, customTrustCertificates) match {
      case Left(err) => throw new IllegalArgumentException(s"Invalid connection $connection : $err")
      case Right(es) => es
    }
}

object SequencerConnection
    extends HasVersionedWrapperCompanion[VersionedSequencerConnection, SequencerConnection] {
  override protected def ProtoClassCompanion: VersionedSequencerConnection.type =
    VersionedSequencerConnection
  override protected def name: String = "sequencer connection"

  override def fromProtoVersioned(
      configP: VersionedSequencerConnection
  ): ParsingResult[SequencerConnection] =
    configP.version match {
      case VersionedSequencerConnection.Version.Empty =>
        Left(FieldNotSet("VersionedSequencerConnection.version"))
      case VersionedSequencerConnection.Version.V0(config) => fromProtoV0(config)
    }

  def fromProtoV0(
      configP: v0.SequencerConnection
  ): ParsingResult[SequencerConnection] =
    configP.`type` match {
      case v0.SequencerConnection.Type.Empty => Left(ProtoDeserializationError.FieldNotSet("type"))
      case v0.SequencerConnection.Type.Http(http) => fromHttpProto(http)
      case v0.SequencerConnection.Type.Grpc(grpc) => fromGrpcProto(grpc)
    }

  // https can be safely assumed
  private def url(host: String, port: Port) = s"https://$host:$port"

  private def fromHttpProto(
      httpP: v0.SequencerConnection.Http
  ): ParsingResult[SequencerConnection] =
    for {
      port <- Port.create(httpP.port)
      readPort <- Port.create(httpP.readPort)
      certificate <- ProtoConverter.parseRequired[X509CertificatePem, ByteString](
        bytes =>
          X509CertificatePem
            .fromBytes(bytes)
            .leftMap(err => ProtoDeserializationError.ValueConversionError("certificate", err)),
        "certificate",
        httpP.certificate,
      )
      urls <- HttpSequencerEndpoints
        .create(
          writeUrl = url(httpP.host, port),
          readUrl = url(httpP.readHost, readPort),
        )
        .leftMap(ProtoDeserializationError.StringConversionError)
    } yield HttpSequencerConnection(urls, certificate)

  private def fromGrpcProto(
      grpcP: v0.SequencerConnection.Grpc
  ): ParsingResult[SequencerConnection] =
    for {
      uris <- NonEmptyList
        .fromList(grpcP.connections.toList.map(new URI(_)))
        .toRight(ProtoDeserializationError.FieldNotSet("connections"))
      endpoints <- Endpoint
        .fromUris(uris)
        .leftMap(err => ProtoDeserializationError.ValueConversionError("connections", err))
    } yield GrpcSequencerConnection(
      endpoints._1,
      grpcP.transportSecurity,
      grpcP.customTrustCertificates,
    )

  def merge(connections: Seq[SequencerConnection]): Either[String, SequencerConnection] =
    for {
      connectionsNel <- NonEmptyList
        .fromList(connections.toList)
        .toRight("There must be at least one sequencer connection defined")
      conn <- connectionsNel.head match {
        case grpc @ GrpcSequencerConnection(endpoints, _, _) =>
          for {
            allMergedEndpoints <- connectionsNel.tail.flatTraverse {
              case grpc: GrpcSequencerConnection => Right(grpc.endpoints.toList)
              case _ => Left("Cannot merge grpc and http sequencer connections")
            }
          } yield grpc.copy(endpoints = endpoints ++ allMergedEndpoints)
        case http: HttpSequencerConnection =>
          Either.cond(
            connectionsNel.tail.isEmpty,
            http,
            "http connection currently only supports one endpoint",
          )
      }
    } yield conn
}
