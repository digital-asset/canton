// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EnvelopeContent,
  ProtocolMessage,
}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import monocle.Lens

/** A [[ClosedEnvelope]]'s contents are serialized as a [[com.google.protobuf.ByteString]]. */
case class ClosedEnvelope(
    bytes: ByteString,
    override val recipients: Recipients,
) extends Envelope[ByteString] {
  override protected def contentAsByteString: ByteString = bytes
  override protected def content: ByteString = bytes

  def openEnvelope[M <: ProtocolMessage](
      protocolMessageDeserializer: ByteString => ParsingResult[M],
      protocolVersion: ProtocolVersion,
  ): ParsingResult[OpenEnvelope[M]] =
    protocolMessageDeserializer(bytes).map(protocolMessage =>
      OpenEnvelope(protocolMessage, recipients, protocolVersion)
    )

  override def pretty: Pretty[ClosedEnvelope] = prettyOfClass(param("recipients", _.recipients))

  override def forRecipient(member: Member): Option[ClosedEnvelope] =
    recipients.forMember(member).map(r => ClosedEnvelope(bytes, r))
}

object ClosedEnvelope {
  def fromProtoV0(envelopeP: v0.Envelope): ParsingResult[ClosedEnvelope] = {
    val v0.Envelope(contentP, recipientsP) = envelopeP
    for {
      tree <- ProtoConverter.required("recipients", recipientsP)
      recipients <- Recipients.fromProtoV0(tree)
    } yield ClosedEnvelope(contentP, recipients)
  }

  def tryDefaultOpenEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): Lens[ClosedEnvelope, DefaultOpenEnvelope] =
    Lens[ClosedEnvelope, DefaultOpenEnvelope](
      _.openEnvelope(
        EnvelopeContent.messageFromByteString(protocolVersion, hashOps),
        protocolVersion,
      ).valueOr(err => throw new IllegalArgumentException(s"Failed to open envelope: $err"))
    )(newOpenEnvelope => _ => newOpenEnvelope.closeEnvelope)
}
