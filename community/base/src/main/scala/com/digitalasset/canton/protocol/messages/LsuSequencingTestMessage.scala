// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.crypto.verifier.SyncCryptoVerifier
import com.digitalasset.canton.crypto.{
  Hash,
  HashAlgorithm,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Used to test ordering and sequencing on the LSU successor, before upgrade time.
  */
final case class LsuSequencingTestMessage(
    content: LsuSequencingTestMessageContent,
    signature: Signature,
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[LsuSequencingTestMessage] {

  override val psid: PhysicalSynchronizerId = content.psid

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    LsuSequencingTestMessage.type
  ] = LsuSequencingTestMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  @transient override protected lazy val companionObj: LsuSequencingTestMessage.type =
    LsuSequencingTestMessage

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.LsuSequencingTestMessage(toProtoV30)

  override protected[messages] def toProtoSomeEnvelopeContentV31
      : v31.EnvelopeContent.SomeEnvelopeContent =
    v31.EnvelopeContent.SomeEnvelopeContent.LsuSequencingTestMessage(toProtoV30)

  def toProtoV30: v30.LsuSequencingTestMessage =
    v30.LsuSequencingTestMessage(content.toByteString, signature.toProtoV30.some)
}

object LsuSequencingTestMessage
    extends VersioningCompanionContext[LsuSequencingTestMessage, ProtocolVersion] {
  override def name: String = "LsuSequencingTestMessage"

  implicit val acceptedTestingLsuSequencingMessageMessageCast
      : ProtocolMessageContentCast[LsuSequencingTestMessage] =
    ProtocolMessageContentCast.create[LsuSequencingTestMessage](name) {
      case m: LsuSequencingTestMessage => Some(m)
      case _ => None
    }

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.LsuSequencingTestMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[messages] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.LsuSequencingTestMessage,
  ): ParsingResult[LsuSequencingTestMessage] = {
    val v30.LsuSequencingTestMessage(contentP, signatureP) = message

    for {
      content <- LsuSequencingTestMessageContent.fromByteString(expectedProtocolVersion, contentP)
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        signatureP,
      )

    } yield LsuSequencingTestMessage(content, signature)
  }

  def verifySignature(verifier: SyncCryptoVerifier, snapshot: TopologySnapshot)(
      msg: LsuSequencingTestMessage
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val hash = Hash.digest(
      HashPurpose.LsuSequencingTestMessageContent,
      msg.content.getCryptographicEvidence,
      HashAlgorithm.Sha256,
    )

    verifier
      .verifySignature(
        topologySnapshot = snapshot,
        hash = hash,
        signer = msg.content.sender,
        signature = msg.signature,
        usage = SigningKeyUsage.ProtocolOnly,
      )
  }
}

final case class LsuSequencingTestMessageContent private (
    psid: PhysicalSynchronizerId,
    sender: Member,
)(override val deserializedFrom: Option[ByteString])
    extends HasProtocolVersionedWrapper[LsuSequencingTestMessageContent]
    with ProtocolVersionedMemoizedEvidence {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    LsuSequencingTestMessageContent.type
  ] = LsuSequencingTestMessageContent.protocolVersionRepresentativeFor(psid.protocolVersion)

  @transient override protected lazy val companionObj: LsuSequencingTestMessageContent.type =
    LsuSequencingTestMessageContent

  private def toProtoV30: v30.LsuSequencingTestMessageContent =
    v30.LsuSequencingTestMessageContent(psid.toProtoPrimitive, sender.toProtoPrimitive)

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}

object LsuSequencingTestMessageContent
    extends VersioningCompanionMemoization[LsuSequencingTestMessageContent] {
  override def name: String = "LsuSequencingTestMessageContent"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.LsuSequencingTestMessageContent
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(psid: PhysicalSynchronizerId, sender: Member): LsuSequencingTestMessageContent =
    LsuSequencingTestMessageContent(psid, sender)(None)

  private[messages] def fromProtoV30(
      message: v30.LsuSequencingTestMessageContent
  )(
      bytes: ByteString
  ): ParsingResult[LsuSequencingTestMessageContent] = {
    val v30.LsuSequencingTestMessageContent(synchronizerP, senderP) = message

    for {
      psid <- PhysicalSynchronizerId
        .fromProtoPrimitive(synchronizerP, "physical_synchronizer_id")

      sender <- Member.fromProtoPrimitive(senderP, "sender")
    } yield LsuSequencingTestMessageContent(psid, sender)(Some(bytes))
  }
}
