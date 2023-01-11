// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import java.util.UUID

/** Information concerning every '''member''' involved in the underlying transaction.
  *
  * @param confirmationPolicy determines who must confirm the request
  */
final case class CommonMetadata private (
    confirmationPolicy: ConfirmationPolicy,
    domainId: DomainId,
    mediatorId: MediatorId,
    salt: Salt,
    uuid: UUID,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[CommonMetadata],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[CommonMetadata](hashOps)
    with HasProtocolVersionedWrapper[CommonMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.CommonMetadata

  override def pretty: Pretty[CommonMetadata] = prettyOfClass(
    param("confirmation policy", _.confirmationPolicy),
    param("domain id", _.domainId),
    param("mediator id", _.mediatorId),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )

  override def companionObj = CommonMetadata

  private[CommonMetadata] def toProtoV0: v0.CommonMetadata = v0.CommonMetadata(
    confirmationPolicy = confirmationPolicy.toProtoPrimitive,
    domainId = domainId.toProtoPrimitive,
    salt = Some(salt.toProtoV0),
    uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    mediatorId = mediatorId.toProtoPrimitive,
  )
}

object CommonMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      CommonMetadata,
      HashOps,
    ] {
  override val name: String = "CommonMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersionMemoized(v0.CommonMetadata)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      hashOps: HashOps
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediatorId: MediatorId,
      salt: Salt,
      uuid: UUID,
      protocolVersion: ProtocolVersion,
  ): CommonMetadata = CommonMetadata(confirmationPolicy, domain, mediatorId, salt, uuid)(
    hashOps,
    protocolVersionRepresentativeFor(protocolVersion),
    None,
  )

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] =
    for {
      confirmationPolicy <- ConfirmationPolicy
        .fromProtoPrimitive(metaDataP.confirmationPolicy)
        .leftMap(e =>
          ProtoDeserializationError.ValueDeserializationError("confirmationPolicy", e.show)
        )
      v0.CommonMetadata(saltP, _confirmationPolicyP, domainIdP, uuidP, mediatorIdP) = metaDataP
      domainUid <- UniqueIdentifier
        .fromProtoPrimitive_(domainIdP)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("domainId", _))
      mediatorId <- MediatorId
        .fromProtoPrimitive(mediatorIdP, "CommonMetadata.mediator_id")
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
    } yield CommonMetadata(confirmationPolicy, DomainId(domainUid), mediatorId, salt, uuid)(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
}
