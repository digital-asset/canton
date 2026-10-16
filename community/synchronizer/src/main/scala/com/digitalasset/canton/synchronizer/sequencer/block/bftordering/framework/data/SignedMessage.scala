// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.version.ProtoVersion
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.google.protobuf.ByteString

trait MessageFrom {
  def from: BftNodeId
}

final case class SignedMessage[+MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
    message: MessageT,
    signature: Signature,
) {
  def toProtoV1: v30.SignedMessage =
    v30.SignedMessage(
      message.getCryptographicEvidence,
      message.from,
      signature = Some(signature.toProtoV30),
    )

  def from: BftNodeId = message.from
}

object SignedMessage {
  def fromProto[
      MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom,
      Proto <: scalapb.GeneratedMessage,
  ](
      p: scalapb.GeneratedMessageCompanion[Proto],
      expectedProtoVersion: ProtoVersion,
  )(
      parse: Proto => ByteString => ParsingResult[MessageT]
  )(
      proto: v30.SignedMessage
  ): ParsingResult[SignedMessage[MessageT]] =
    fromProtoWithNodeId(p, expectedProtoVersion)(_ => parse)(proto)

  def fromProtoWithNodeId[
      A <: ProtocolVersionedMemoizedEvidence & MessageFrom,
      Proto <: scalapb.GeneratedMessage,
  ](
      p: scalapb.GeneratedMessageCompanion[Proto],
      expectedProtoVersion: ProtoVersion,
  )(
      parse: BftNodeId => Proto => ByteString => ParsingResult[A]
  )(proto: v30.SignedMessage): ParsingResult[SignedMessage[A]] = {
    val from = BftNodeId(proto.from)
    for {
      versionedMessage <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(
        proto.message
      )
      // Validate the embedded wire version against the expected one (derived from the runtime protocol version):
      //  selecting the decoder purely from the local protocol version would otherwise accept mismatched wire
      //  versions whenever the payloads happen to be wire-compatible (e.g. a v30-tagged payload decoded with the
      //  v31 decoder and mislabeled as such).
      _ <- Either.cond(
        versionedMessage.version == expectedProtoVersion.v,
        (),
        ProtoDeserializationError.OtherError(
          s"Wire protobuf version ${versionedMessage.version} does not match the expected version " +
            s"${expectedProtoVersion.v} for the runtime protocol version"
        ),
      )
      unVersionedBytes <- versionedMessage.wrapper.data.toRight(
        ProtoDeserializationError.OtherError("Missing data in UntypedVersionedMessage")
      )
      protoMessage <- ProtoConverter.protoParser(p.parseFrom)(unVersionedBytes)
      message <- parse(from)(protoMessage)(proto.message)
      signature <- Signature.fromProtoV30(proto.getSignature)
    } yield SignedMessage(message, signature)
  }

  implicit def ordering[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](implicit
      ordering: Ordering[A]
  ): Ordering[SignedMessage[A]] =
    ordering.on(_.message)
}
