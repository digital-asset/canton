// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol.channel

import com.digitalasset.canton.crypto.{AsymmetricEncrypted, Signature}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

/** Session key message sent by the session key owner to the other channel member.
  *
  * @param encryptedSessionKey
  *   The session key and its context hash, encrypted to the receiver.
  * @param signature
  *   Signature of the session key owner over the channel context and the encrypted session key.
  */
final case class SequencerChannelSessionKey(
    encryptedSessionKey: AsymmetricEncrypted[SequencerChannelSessionKeyContent],
    signature: Signature,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerChannelSessionKey.type
    ]
) extends HasProtocolVersionedWrapper[SequencerChannelSessionKey] {
  @transient override protected lazy val companionObj: SequencerChannelSessionKey.type =
    SequencerChannelSessionKey

  def toProtoV30: v30.SequencerChannelSessionKey =
    v30.SequencerChannelSessionKey(
      encryptedSessionKey = Some(encryptedSessionKey.toProtoV30),
      sessionKeySignature = Some(signature.toProtoV30),
    )
}

object SequencerChannelSessionKey extends VersioningCompanion[SequencerChannelSessionKey] {
  override val name: String = "SequencerChannelSessionKey"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.sequencerChannel)(
      v30.SequencerChannelSessionKey
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def apply(
      encryptedSessionKey: AsymmetricEncrypted[SequencerChannelSessionKeyContent],
      signature: Signature,
      protocolVersion: ProtocolVersion,
  ): SequencerChannelSessionKey =
    SequencerChannelSessionKey(encryptedSessionKey, signature)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      sessionKeyP: v30.SequencerChannelSessionKey
  ): ParsingResult[SequencerChannelSessionKey] = {
    val v30.SequencerChannelSessionKey(encryptedSessionKeyP, signatureP) = sessionKeyP
    for {
      encryptedSessionKey <- ProtoConverter.parseRequired(
        AsymmetricEncrypted.fromProtoV30[SequencerChannelSessionKeyContent],
        "encrypted_session_key",
        encryptedSessionKeyP,
      )
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "session_key_signature",
        signatureP,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencerChannelSessionKey(encryptedSessionKey, signature)(rpv)
  }
}
