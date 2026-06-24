// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.protocol.{v30, v31, v32}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanionContext,
}

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
final case class LegacyAcsCommitmentProtocolMessage(
    acsCommitment: LegacyAcsCommitment,
    signatures: NonEmpty[Seq[Signature]],
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[LegacyAcsCommitmentProtocolMessage] {

  @transient override protected lazy val companionObj: LegacyAcsCommitmentProtocolMessage.type =
    LegacyAcsCommitmentProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitment.psid

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    LegacyAcsCommitmentProtocolMessage.type
  ] = LegacyAcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  protected def toProtoV30: v30.AcsCommitmentProtocolMessage =
    v30.AcsCommitmentProtocolMessage(
      acsCommitment = acsCommitment.toByteString,
      signatures = signatures.map(_.toProtoV30),
    )

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} cannot be serialized to envelope content v30"
    )

  override protected[messages] def toProtoSomeEnvelopeContentV31
      : v31.EnvelopeContent.SomeEnvelopeContent =
    v31.EnvelopeContent.SomeEnvelopeContent.AcsCommitmentProtocolMessage(toProtoV30)

  override protected[messages] def toProtoSomeEnvelopeContentV32
      : v32.EnvelopeContent.SomeEnvelopeContent =
    v32.EnvelopeContent.SomeEnvelopeContent.LegacyAcsCommitmentProtocolMessage(toProtoV30)
}

object LegacyAcsCommitmentProtocolMessage
    extends VersioningCompanionContext[
      LegacyAcsCommitmentProtocolMessage,
      ProtocolVersion,
    ] {

  override def name: String = "LegacyAcsCommitmentProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v35)(
      v30.AcsCommitmentProtocolMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  private[messages] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.AcsCommitmentProtocolMessage,
  ): ParsingResult[LegacyAcsCommitmentProtocolMessage] = {
    val v30.AcsCommitmentProtocolMessage(acsCommitmentP, signaturesP) = message
    for {
      acsCommitment <- LegacyAcsCommitment.fromByteString(expectedProtocolVersion, acsCommitmentP)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesP,
      )
    } yield LegacyAcsCommitmentProtocolMessage(acsCommitment, signatures)
  }

}
