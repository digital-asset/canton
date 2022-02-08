// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.NonEmptyList
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, InvariantViolation}
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.admin.v0
import com.digitalasset.canton.participant.admin.version.VersionedDomainConnectionConfig
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  HttpSequencerConnection,
  SequencerConnection,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{DomainTimeTrackerConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.util.{
  HasProtoV0,
  HasVersionedWrapper,
  HasVersionedWrapperCompanion,
  OptionUtil,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, DomainId}
import com.google.protobuf.ByteString

import java.net.URI

case class DomainConnectionConfig(
    domain: DomainAlias,
    sequencerConnection: SequencerConnection,
    manualConnect: Boolean = false,
    domainId: Option[DomainId] = None,
    priority: Int = 0,
    initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
    maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
    timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
) extends HasVersionedWrapper[VersionedDomainConnectionConfig]
    with HasProtoV0[v0.DomainConnectionConfig]
    with PrettyPrinting {

  /** Helper methods to avoid having to use NonEmptyList in the console */
  def addConnection(connection: String, additionalConnections: String*): DomainConnectionConfig =
    addConnection(new URI(connection), additionalConnections.map(new URI(_)): _*)
  def addConnection(connection: URI, additionalConnections: URI*): DomainConnectionConfig =
    sequencerConnection match {
      case GrpcSequencerConnection(endpoints, transportSecurity, customTrustCertificates) =>
        val newEndpoints = Endpoint
          .fromUris(NonEmptyList(connection, additionalConnections.toList)) match {
          case Left(err) =>
            throw new IllegalArgumentException(s"invalid connection $connection : $err")
          case Right(es) => es
        }
        copy(
          sequencerConnection = GrpcSequencerConnection(
            endpoints.concatNel(newEndpoints._1),
            transportSecurity,
            customTrustCertificates,
          )
        )
      case _: HttpSequencerConnection =>
        sys.error("Http sequencer does not support multiple connections")
    }

  def certificates: Option[ByteString] = sequencerConnection match {
    case grpcConnection: GrpcSequencerConnection =>
      grpcConnection.customTrustCertificates
    case httpConnection: HttpSequencerConnection =>
      Some(httpConnection.certificate.unwrap)
  }

  def withCertificates(certificates: ByteString): DomainConnectionConfig =
    sequencerConnection match {
      case grpcConnection: GrpcSequencerConnection =>
        copy(sequencerConnection =
          grpcConnection.copy(customTrustCertificates = Some(certificates))
        )
      case httpConnection: HttpSequencerConnection =>
        copy(sequencerConnection =
          httpConnection.copy(certificate = X509CertificatePem.tryFromBytes(certificates))
        )
    }

  override def pretty: Pretty[DomainConnectionConfig] =
    prettyOfClass(
      param("domain", _.domain),
      param("sequencerConnection", _.sequencerConnection),
      param("manualConnect", _.manualConnect),
      param("domainId", _.domainId),
      param("priority", _.priority),
      param("initialRetryDelay", _.initialRetryDelay),
      param("maxRetryDelay", _.maxRetryDelay),
    )

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedDomainConnectionConfig =
    VersionedDomainConnectionConfig(VersionedDomainConnectionConfig.Version.V0(toProtoV0))

  override def toProtoV0: v0.DomainConnectionConfig =
    v0.DomainConnectionConfig(
      domainAlias = domain.unwrap,
      sequencerConnection = sequencerConnection.toProtoV0.some,
      manualConnect = manualConnect,
      domainId = domainId.fold("")(_.toProtoPrimitive),
      priority = priority,
      initialRetryDelay = initialRetryDelay.map(_.toProtoPrimitive),
      maxRetryDelay = maxRetryDelay.map(_.toProtoPrimitive),
      timeTracker = timeTracker.toProtoV0.some,
    )
}

object DomainConnectionConfig
    extends HasVersionedWrapperCompanion[VersionedDomainConnectionConfig, DomainConnectionConfig] {
  override protected def ProtoClassCompanion: VersionedDomainConnectionConfig.type =
    VersionedDomainConnectionConfig
  override protected def name: String = "domain connection config"

  def grpc(
      domainAlias: DomainAlias,
      connection: String,
      manualConnect: Boolean = false,
      domainId: Option[DomainId] = None,
      certificates: Option[ByteString] = None,
      priority: Int = 0,
      initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
      maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
      timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
  ): DomainConnectionConfig =
    DomainConnectionConfig(
      domainAlias,
      GrpcSequencerConnection.tryCreate(connection, certificates),
      manualConnect,
      domainId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
    )

  override def fromProtoVersioned(
      domainConnectionConfigP: VersionedDomainConnectionConfig
  ): ParsingResult[DomainConnectionConfig] =
    domainConnectionConfigP.version match {
      case VersionedDomainConnectionConfig.Version.Empty =>
        Left(FieldNotSet("VersionedDomainConnectionConfig.version"))
      case VersionedDomainConnectionConfig.Version.V0(config) => fromProtoV0(config)
    }

  def fromProtoV0(
      domainConnectionConfigP: v0.DomainConnectionConfig
  ): ParsingResult[DomainConnectionConfig] = {
    val v0.DomainConnectionConfig(
      domainAlias,
      sequencerConnectionP,
      manualConnect,
      domainId,
      priority,
      initialRetryDelayP,
      maxRetryDelayP,
      timeTrackerP,
    ) =
      domainConnectionConfigP
    for {
      alias <- DomainAlias
        .create(domainAlias)
        .leftMap(err => InvariantViolation(s"DomainConnectionConfig.DomainAlias: $err"))
      sequencerConnection <- ProtoConverter.parseRequired(
        SequencerConnection.fromProtoV0,
        "sequencerConnection",
        sequencerConnectionP,
      )
      domainId <- OptionUtil
        .emptyStringAsNone(domainId)
        .traverse(DomainId.fromProtoPrimitive(_, "domain_id"))
      initialRetryDelay <- initialRetryDelayP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay")
      )
      maxRetryDelay <- maxRetryDelayP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxRetryDelay")
      )
      timeTracker <- ProtoConverter.parseRequired(
        DomainTimeTrackerConfig.fromProto,
        "timeTracker",
        timeTrackerP,
      )
    } yield DomainConnectionConfig(
      alias,
      sequencerConnection,
      manualConnect,
      domainId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
    )
  }
}
