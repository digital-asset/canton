// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.protocol.{v30, v32}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.digitalasset.nonempty.NonEmpty

/** INTERNAL protocol message type used for ACS commitments in transit starting with PV35. Do NOT
  * USE this message outside of envelope transit (e.g.,
  * `ClosedUncompressedEnvelope.tryFromProtocolMessage` and
  * `ClosedUncompressedEnvelope.toOpenEnvelope`).
  *
  * This type allows us to hide the signatures from the sequencer so that they are not verified when
  * inspecting the closed envelope. It is converted to a `SignedProtocolMessage[AcsCommitment]`
  * immediately upon deserialization when the envelope is opened.
  *
  * TODO(#30888): Make `AcsCommitmentProtocolMessage` the default and get rid of
  * `SignedProtocolMessage[AcsCommitment]`
  */
final case class LegacyAcsCommitmentProtocolMessage private (
    acsCommitment: LegacyAcsCommitment,
    signatures: NonEmpty[Seq[Signature]],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      LegacyAcsCommitmentProtocolMessage.type
    ]
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[LegacyAcsCommitmentProtocolMessage] {

  @transient override protected lazy val companionObj: LegacyAcsCommitmentProtocolMessage.type =
    LegacyAcsCommitmentProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitment.psid

  protected def toProtoV30: v30.AcsCommitmentProtocolMessage =
    v30.AcsCommitmentProtocolMessage(
      acsCommitment = acsCommitment.toByteString,
      signatures = signatures.map(_.toProtoV30),
    )

  override protected[messages] def toProtoSomeEnvelopeContentV32
      : v32.EnvelopeContent.SomeEnvelopeContent =
    v32.EnvelopeContent.SomeEnvelopeContent.LegacyAcsCommitmentProtocolMessage(toProtoV30)
}

object LegacyAcsCommitmentProtocolMessage
    extends VersioningCompanion[LegacyAcsCommitmentProtocolMessage] {

  override def name: String = "LegacyAcsCommitmentProtocolMessage"

  def apply(
      acsCommitment: LegacyAcsCommitment,
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): LegacyAcsCommitmentProtocolMessage =
    LegacyAcsCommitmentProtocolMessage(
      acsCommitment,
      signatures,
    )(LegacyAcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(protocolVersion))

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v30.AcsCommitmentProtocolMessage
    )(
      supportedProtoVersionPVV(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[messages] def fromProtoV30(
      pvv: ProtocolVersionValidation,
      message: v30.AcsCommitmentProtocolMessage,
  ): ParsingResult[LegacyAcsCommitmentProtocolMessage] = {
    val v30.AcsCommitmentProtocolMessage(acsCommitmentP, signaturesP) = message
    for {
      acsCommitment <- LegacyAcsCommitment.fromByteString(
        pvv,
        acsCommitmentP,
      )
      signaturesSeqP <- ProtoValidation
        .validateLength(
          signaturesP,
          "signatures",
          pvv,
          ProtoValidation.MaxCollectionSize,
        )
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesSeqP,
      )
      rpv <- LegacyAcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield LegacyAcsCommitmentProtocolMessage(acsCommitment, signatures)(rpv)
  }

}
