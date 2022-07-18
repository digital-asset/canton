// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.admin.v0
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  HttpSequencerConnection,
  SequencerConnection,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{DomainTimeTrackerConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString

import java.net.URI

/** The domain connection configuration object
  *
  * @param domain alias to be used internally to refer to this domain connection
  * @param sequencerConnection the host and port to the sequencer(s).
  *                            multiple can be given by building the [[com.digitalasset.canton.sequencing.SequencerConnection]] object explicitly.
  * @param manualConnect if set to true (default false), the domain is not connected automatically on startup.
  * @param domainId if the domain-id is known, then it can be passed as an argument. during the handshake, the
  *                 participant will check that the domain-id on the remote port is indeed the one given
  *                 in the configuration. the domain-id can not be faked by a domain. therefore, this additional
  *                 check can be used to really ensure that you are talking to the right domain.
  * @param priority the priority of this domain connection. if there are more than one domain connections, the [[com.digitalasset.canton.participant.sync.DomainRouter]]
  *                 will pick the domain connection with the highest priority if possible.
  * @param initialRetryDelay domain connections are "resilient". i.e. if a connection is lost, the system will keep
  *                          trying to reconnect to a domain.
  * @param maxRetryDelay control the backoff parameter such that the retry interval does not grow above this value
  * @param timeTracker the domain time tracker settings. don't change it unless you know what you are doing.
  */
case class DomainConnectionConfig(
    domain: DomainAlias,
    sequencerConnection: SequencerConnection,
    manualConnect: Boolean = false,
    domainId: Option[DomainId] = None,
    priority: Int = 0,
    initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
    maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
    timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
) extends HasVersionedWrapper[VersionedMessage[DomainConnectionConfig]]
    with HasProtoV0[v0.DomainConnectionConfig]
    with PrettyPrinting {

  /** Helper methods to avoid having to use NonEmpty[Seq in the console */
  def addConnection(connection: String, additionalConnections: String*): DomainConnectionConfig =
    addConnection(new URI(connection), additionalConnections.map(new URI(_)): _*)
  def addConnection(connection: URI, additionalConnections: URI*): DomainConnectionConfig =
    copy(sequencerConnection =
      sequencerConnection
        .addConnection(connection, additionalConnections: _*)
    )

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
  ): VersionedMessage[DomainConnectionConfig] =
    VersionedMessage(toProtoV0.toByteString, 0)

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

object DomainConnectionConfig extends HasVersionedMessageCompanion[DomainConnectionConfig] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.DomainConnectionConfig)(fromProtoV0)
  )
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
