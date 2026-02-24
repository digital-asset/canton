// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.protocol.{v30, v31}
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
final case class AcsCommitmentProtocolMessage(
    acsCommitment: AcsCommitment,
    signatures: NonEmpty[Seq[Signature]],
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[AcsCommitmentProtocolMessage] {

  @transient override protected lazy val companionObj: AcsCommitmentProtocolMessage.type =
    AcsCommitmentProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitment.psid

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    AcsCommitmentProtocolMessage.type
  ] = AcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

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

}

object AcsCommitmentProtocolMessage
    extends VersioningCompanionContext[
      AcsCommitmentProtocolMessage,
      ProtocolVersion,
    ] {

  override def name: String = "AcsCommitmentProtocolMessage"

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
  ): ParsingResult[AcsCommitmentProtocolMessage] = {
    val v30.AcsCommitmentProtocolMessage(acsCommitmentP, signaturesP) = message
    for {
      acsCommitment <- AcsCommitment.fromByteString(expectedProtocolVersion, acsCommitmentP)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesP,
      )
    } yield AcsCommitmentProtocolMessage(acsCommitment, signatures)
  }

}
