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

/** The claimed BFT node that originated this protocol message.
  *
  * In serialized form, the origin is encoded either in the protocol message or in the outer
  * `v30.SignedMessage` wrapper, depending on the message format. The wrapper contains the
  * serialized message bytes, this `from` value, and the signature. The origin is not necessarily
  * the node that delivered the message: retransmission can make those nodes differ. Deserialization
  * must preserve the protocol's origin identity, and callers must not treat this value as
  * authenticated until the message signature has been verified.
  */
trait MessageFrom {
  def from: BftNodeId
}

/** A protocol message together with its signature.
  *
  * The serialized `v30.SignedMessage` wrapper carries `from` alongside the message bytes and
  * signature. `toProtoV1` writes `message.from` into the wrapper; after deserialization, `from`
  * delegates to the parsed message. This is a claimed signing identity, not proof of identity:
  * signature verification must check the signature against this node and the message. If the
  * message bytes also encode an origin, the parser must ensure it agrees with the wrapper's `from`
  * value.
  */
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

  /** Parses the message bytes without using the `from` field in the outer `v30.SignedMessage`
    * wrapper.
    *
    * The supplied parser must derive `MessageFrom.from` from the serialized protocol message
    * itself. Use this when decoding signed messages whose message format carries its own origin.
    * The wrapper's `from` is not compared with or substituted for that origin.
    */
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

  /** Parses the outer `v30.SignedMessage` wrapper's claimed origin and supplies it to the message
    * parser.
    *
    * Use this when the typed message's `from` is defined by the wrapper. The wrapper's `from` field
    * is untrusted input at this point: this method does not authenticate it or compare it with an
    * origin encoded in the message bytes. A parser for a format that encodes both must reject
    * mismatches; callers must verify the resulting signature before trusting `from`.
    */
  def fromProtoWithNodeId[
      A <: ProtocolVersionedMemoizedEvidence & MessageFrom,
      Proto <: scalapb.GeneratedMessage,
  ](
      p: scalapb.GeneratedMessageCompanion[Proto],
      expectedProtoVersion: ProtoVersion,
  )(
      parse: BftNodeId => Proto => ByteString => ParsingResult[A]
  )(proto: v30.SignedMessage): ParsingResult[SignedMessage[A]] = {
    val claimedFrom = BftNodeId(proto.from)
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
      message <- parse(claimedFrom)(protoMessage)(proto.message)
      signature <- Signature.fromProtoV30(proto.getSignature)
    } yield SignedMessage(message, signature)
  }

  implicit def ordering[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](implicit
      ordering: Ordering[A]
  ): Ordering[SignedMessage[A]] =
    ordering.on(_.message)
}
