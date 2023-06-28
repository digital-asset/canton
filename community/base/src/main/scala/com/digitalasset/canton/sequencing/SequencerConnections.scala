// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.domain.api.{v0, v1}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequiredNonEmpty}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionedCompanionDbHelpers,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{ProtoDeserializationError, SequencerAlias}
import com.google.protobuf.ByteString

import java.net.URI

final case class SequencerConnections private (
    aliasToConnection: NonEmpty[Map[SequencerAlias, SequencerConnection]]
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerConnections.type
    ]
) extends PrettyPrinting
    with HasRepresentativeProtocolVersion
    with HasProtocolVersionedWrapper[SequencerConnections] {

  aliasToConnection.foreach { case (alias, connection) =>
    require(
      alias == connection.sequencerAlias,
      "SequencerAlias in the Map must match SequencerConnection.sequencerAlias",
    )
  }

  if (nonBftSetup) {
    require(
      aliasToConnection.sizeIs == 1,
      "Only a single connection is supported in case of non-BFT support",
    )
  }

  def default: SequencerConnection =
    aliasToConnection.head1._2

  // In case of BFT domain - multiple sequencers are required for proper functioning.
  // Some functionalities are only available in non-bft domain.
  def nonBftSetup: Boolean = {
    import scala.math.Ordered.orderingToOrdered
    representativeProtocolVersion < SequencerConnections.protocolVersionRepresentativeFor(
      ProtocolVersion.dev // TODO(i12373): Change to a specific ProtocolVersion when `dev` is released.
    )
  }

  def connections: NonEmpty[Seq[SequencerConnection]] = aliasToConnection.map(_._2).toSeq

  def modify(
      sequencerAlias: SequencerAlias,
      m: SequencerConnection => SequencerConnection,
  ): SequencerConnections =
    aliasToConnection
      .get(sequencerAlias)
      .map { connection =>
        SequencerConnections(
          aliasToConnection.updated(
            sequencerAlias,
            m(connection),
          )
        )(representativeProtocolVersion)
      }
      .getOrElse(this)

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: URI,
      additionalConnections: URI*
  ): SequencerConnections =
    (Seq(connection) ++ additionalConnections).foldLeft(this) { case (acc, elem) =>
      acc.modify(sequencerAlias, _.addEndpoints(elem))
    }

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: SequencerConnection,
      additionalConnections: SequencerConnection*
  ): SequencerConnections =
    (Seq(connection) ++ additionalConnections).foldLeft(this) { case (acc, elem) =>
      acc.modify(sequencerAlias, _.addEndpoints(elem))
    }

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): SequencerConnections =
    modify(sequencerAlias, _.withCertificates(certificates))

  override def pretty: Pretty[SequencerConnections] = prettyOfParam(_.aliasToConnection.forgetNE)

  def toProtoV0: Seq[v0.SequencerConnection] = connections.map(_.toProtoV0)

  @transient override protected lazy val companionObj: SequencerConnections.type =
    SequencerConnections

  def toProtoV1: v1.SequencerConnections = new v1.SequencerConnections(connections.map(_.toProtoV0))
}

object SequencerConnections
    extends HasProtocolVersionedCompanion[SequencerConnections]
    with ProtocolVersionedCompanionDbHelpers[SequencerConnections] {
  def single(connection: SequencerConnection): SequencerConnections =
    new SequencerConnections(NonEmpty.mk(Seq, (connection.sequencerAlias, connection)).toMap)(
      protocolVersionRepresentativeFor(ProtocolVersion.v3)
    )

  def many(connections: NonEmpty[Seq[SequencerConnection]]): SequencerConnections = {
    if (connections.size == 1) {
      SequencerConnections.single(connections.head1)
    } else
      new SequencerConnections(connections.map(conn => (conn.sequencerAlias, conn)).toMap)(
        protocolVersionRepresentativeFor(ProtocolVersion.dev)
      )
  }

  def tryMany(connections: Seq[SequencerConnection]): SequencerConnections = {
    require(
      connections.map(_.sequencerAlias).toSet.size == connections.size,
      "Non-unique sequencer aliases detected",
    )
    many(NonEmptyUtil.fromUnsafe(connections))
  }

  private def fromProtoV0V1(
      fieldName: String,
      connections: Seq[v0.SequencerConnection],
  ): ParsingResult[SequencerConnections] = for {
    sequencerConnectionsNes <- parseRequiredNonEmpty(
      SequencerConnection.fromProtoV0,
      fieldName,
      connections,
    )
    _ <- Either.cond(
      sequencerConnectionsNes.map(_.sequencerAlias).toSet.size == sequencerConnectionsNes.size,
      (),
      ProtoDeserializationError.ValueConversionError(
        fieldName,
        "Every sequencer connection must have a unique sequencer alias",
      ),
    )
  } yield many(sequencerConnectionsNes)

  def fromProtoV0(
      sequencerConnection: v0.SequencerConnection
  ): ParsingResult[SequencerConnections] =
    fromProtoV0V1("sequencer_connection", Seq(sequencerConnection))

  def fromProtoV0(
      sequencerConnection: Seq[v0.SequencerConnection]
  ): ParsingResult[SequencerConnections] =
    fromProtoV0V1("sequencer_connections", sequencerConnection)

  def fromProtoV1(
      sequencerConnections: v1.SequencerConnections
  ): ParsingResult[SequencerConnections] =
    fromProtoV0V1("sequencer_connections", sequencerConnections.sequencerConnections)

  override protected def name: String = "sequencer connections"

  override def supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter
      .storage[v0.SequencerConnection](
        ReleaseProtocolVersion(ProtocolVersion.v3),
        v0.SequencerConnection,
      )(
        supportedProtoVersion(_)(fromProtoV0),
        element => element.default.toProtoV0.toByteString,
      ),
    ProtoVersion(1) -> VersionedProtoConverter
      .storage(ReleaseProtocolVersion(ProtocolVersion.dev), v1.SequencerConnections)(
        supportedProtoVersion(_)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
  )
}
