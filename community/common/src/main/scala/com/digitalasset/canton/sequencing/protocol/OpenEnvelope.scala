// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

/** An [[OpenEnvelope]] contains a not serialized protocol message
  *
  * @tparam M The type of the protocol message
  */
case class OpenEnvelope[+M <: ProtocolMessage](
    protocolMessage: M,
    override val recipients: Recipients,
) extends Envelope[M] {

  override protected def content: M = protocolMessage

  /** Returns the serialized contents of the envelope */
  protected def contentAsByteString(version: ProtocolVersion): ByteString =
    ProtocolMessage.toEnvelopeContentByteString(protocolMessage, version)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], MM <: ProtocolMessage](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[OpenEnvelope[MM]] =
    F.map(f(protocolMessage)) { newProtocolMessage =>
      if (newProtocolMessage eq protocolMessage) this.asInstanceOf[OpenEnvelope[MM]]
      else this.copy(protocolMessage = newProtocolMessage)
    }

  override def pretty: Pretty[DefaultOpenEnvelope] =
    prettyOfClass(unnamedParam(_.protocolMessage), param("recipients", _.recipients))

  override def forRecipient(member: Member): Option[OpenEnvelope[M]] = {
    val subtrees = recipients.forMember(member)
    subtrees.map(s => OpenEnvelope(protocolMessage, s))
  }

  /** Closes the envelope by serializing the contents */
  def closeEnvelope(version: ProtocolVersion): ClosedEnvelope =
    ClosedEnvelope(contentAsByteString(version), recipients)
}

object OpenEnvelope {
  def fromProtoV0[M <: ProtocolMessage](
      protocolMessageDeserializer: ByteString => ParsingResult[M]
  )(envelopeP: v0.Envelope): ParsingResult[OpenEnvelope[M]] = {
    val v0.Envelope(contentsP, recipientsP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(Recipients.fromProtoV0, "recipients", recipientsP)
      protocolMessage <- protocolMessageDeserializer(contentsP)
    } yield OpenEnvelope(protocolMessage, recipients)
  }
}
