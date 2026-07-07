// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.option.*
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

final case class AcsCommitmentProtocolMessage(
    acsCommitment: AcsCommitment,
    signature: Signature,
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[AcsCommitmentProtocolMessage] {

  @transient override protected lazy val companionObj: AcsCommitmentProtocolMessage.type =
    AcsCommitmentProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitment.psid

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    AcsCommitmentProtocolMessage.type
  ] = AcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  protected def toProtoV32: v32.AcsCommitmentProtocolMessage =
    v32.AcsCommitmentProtocolMessage(
      acsCommitment = acsCommitment.toByteString,
      signature = signature.toProtoV30.some,
    )

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} cannot be serialized to envelope content v30"
    )

  override protected[messages] def toProtoSomeEnvelopeContentV31
      : v31.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} cannot be serialized to envelope content v31"
    )

  override protected[messages] def toProtoSomeEnvelopeContentV32
      : v32.EnvelopeContent.SomeEnvelopeContent =
    v32.EnvelopeContent.SomeEnvelopeContent.AcsCommitmentProtocolMessage(toProtoV32)
}

object AcsCommitmentProtocolMessage
    extends VersioningCompanionContext[
      AcsCommitmentProtocolMessage,
      ProtocolVersion,
    ] {

  override def name: String = "AcsCommitmentProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.AcsCommitmentProtocolMessage
    )(
      supportedProtoVersion(_)(fromProtoV32),
      _.toProtoV32,
    ),
  )

  private[messages] def fromProtoV32(
      expectedProtocolVersion: ProtocolVersion,
      message: v32.AcsCommitmentProtocolMessage,
  ): ParsingResult[AcsCommitmentProtocolMessage] = {
    val v32.AcsCommitmentProtocolMessage(acsCommitmentP, signaturesP) = message
    for {
      acsCommitment <- AcsCommitment.fromByteString(expectedProtocolVersion, acsCommitmentP)
      signatures <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        signaturesP,
      )
    } yield AcsCommitmentProtocolMessage(acsCommitment, signatures)
  }

}
