// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.implicits.toTraverseOps
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.v30.ExternalPartyAuthorization
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.*

final case class ExternalAuthorization(
    signatures: Map[PartyId, Seq[Signature]],
    hashingSchemeVersion: HashingSchemeVersion,
    maxRecordTime: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ExternalAuthorization.type
    ]
) extends HasProtocolVersionedWrapper[ExternalAuthorization]
    with PrettyPrintingFromCompanion {

  override def prettyCompanion: PrettyPrintingCompanion[ExternalAuthorization] =
    ExternalAuthorization

  private def authenticationsV30: Seq[ExternalPartyAuthorization] =
    signatures.map { case (party, partySignatures) =>
      v30.ExternalPartyAuthorization(party.toProtoPrimitive, partySignatures.map(_.toProtoV30))
    }.toSeq

  private[canton] def toProtoV31: v31.ExternalAuthorization =
    v31.ExternalAuthorization(
      authentications = authenticationsV30,
      hashingSchemeVersion = hashingSchemeVersion.toProtoV31,
      maxRecordTime = maxRecordTime.map(_.toProtoPrimitive),
    )

  private[canton] def toProtoV32: v32.ExternalAuthorization =
    v32.ExternalAuthorization(
      authentications = authenticationsV30,
      hashingSchemeVersion = hashingSchemeVersion.toProtoV32,
      maxRecordTime = maxRecordTime.map(_.toProtoPrimitive),
    )

  @transient override protected lazy val companionObj: ExternalAuthorization.type =
    ExternalAuthorization

}

object ExternalAuthorization
    extends VersioningCompanion[ExternalAuthorization]
    with ProtocolVersionedCompanionDbHelpers[ExternalAuthorization]
    with PrettyPrintingCompanion[ExternalAuthorization] {

  override protected val pretty: Pretty[ExternalAuthorization] = prettyOfClass(
    param("signatures", _.signatures)
  )

  def create(
      signatures: Map[PartyId, Seq[Signature]],
      hashingSchemeVersion: HashingSchemeVersion,
      maxRecordTime: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): ExternalAuthorization =
    ExternalAuthorization(signatures, hashingSchemeVersion, maxRecordTime)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "ExternalAuthorization"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(protoCompanion =
      v31.ExternalAuthorization
    )(supportedProtoVersionPVV(_)(fromProtoV31), _.toProtoV31),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(protoCompanion =
      v32.ExternalAuthorization
    )(supportedProtoVersionPVV(_)(fromProtoV32), _.toProtoV32),
  )

  private def fromProtoV30(
      pvv: ProtocolVersionValidation,
      proto: v30.ExternalPartyAuthorization,
  ): ParsingResult[(PartyId, Seq[Signature])] = {
    val v30.ExternalPartyAuthorization(partyP, signaturesP) = proto
    for {
      partyId <- ProtoValidation.validateThen(partyP, "party", pvv)(
        PartyId.fromProtoPrimitive
      )
      partySignatures <- ProtoValidation
        .validateLengthThen(
          signaturesP,
          "signatures",
          pvv,
          ProtoValidation.MaxCollectionSize,
        )((element, _) => Signature.fromProtoV30(element))
    } yield partyId -> partySignatures
  }

  def fromProtoV31(
      pvv: ProtocolVersionValidation,
      proto: v31.ExternalAuthorization,
  ): ParsingResult[ExternalAuthorization] = {
    val v31.ExternalAuthorization(signaturesP, hashingSchemeVersionP, maxRecordTimeP) = proto
    for {
      signatures <- ProtoValidation
        .validateLengthThen(
          signaturesP,
          "signatures",
          pvv,
          ProtoValidation.MaxCollectionSize,
        )((element, _) => fromProtoV30(pvv, element))
      hashingSchemeVersion <- HashingSchemeVersion.fromProtoV31(hashingSchemeVersionP)
      maxRecordTime <- maxRecordTimeP.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
    } yield ExternalAuthorization(signatures.toMap, hashingSchemeVersion, maxRecordTime)(rpv)
  }

  def fromProtoV32(
      pvv: ProtocolVersionValidation,
      proto: v32.ExternalAuthorization,
  ): ParsingResult[ExternalAuthorization] = {
    val v32.ExternalAuthorization(signaturesP, hashingSchemeVersionP, maxRecordTimeP) = proto
    for {
      signatures <- ProtoValidation
        .validateLengthThen(
          signaturesP,
          "signatures",
          pvv,
          ProtoValidation.MaxCollectionSize,
        )((element, _) => fromProtoV30(pvv, element))
      hashingSchemeVersion <- HashingSchemeVersion.fromProtoV32(hashingSchemeVersionP)
      maxRecordTime <- maxRecordTimeP.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
    } yield ExternalAuthorization(signatures.toMap, hashingSchemeVersion, maxRecordTime)(rpv)
  }

}
