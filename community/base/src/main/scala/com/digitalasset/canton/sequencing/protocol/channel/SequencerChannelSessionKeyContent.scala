// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol.channel

import com.digitalasset.canton.crypto.{Hash, SymmetricKey}
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

/** Plaintext of [[SequencerChannelSessionKey.encryptedSessionKey]].
  *
  * @param sessionKey
  *   The symmetric key that secures the channel.
  * @param contextHash
  *   Hash of the physical synchronizer id, the channel id, the sender and the receiver. It binds
  *   the session key to the member that encrypted it, so that the receiver can reject an encrypted
  *   key that another member signed as its own.
  */
final case class SequencerChannelSessionKeyContent(
    sessionKey: SymmetricKey,
    contextHash: Hash,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerChannelSessionKeyContent.type
    ]
) extends HasProtocolVersionedWrapper[SequencerChannelSessionKeyContent]
    with PrettyPrintingFromCompanion {
  @transient override protected lazy val companionObj: SequencerChannelSessionKeyContent.type =
    SequencerChannelSessionKeyContent

  def toProtoV30: v30.SequencerChannelSessionKeyContent =
    v30.SequencerChannelSessionKeyContent(
      sessionKey = Some(sessionKey.toProtoV30),
      contextHash = contextHash.getCryptographicEvidence,
    )

  override def prettyCompanion: PrettyPrintingCompanion[SequencerChannelSessionKeyContent] =
    SequencerChannelSessionKeyContent
}

object SequencerChannelSessionKeyContent
    extends VersioningCompanion[SequencerChannelSessionKeyContent]
    with PrettyPrintingCompanion[SequencerChannelSessionKeyContent] {
  override val name: String = "SequencerChannelSessionKeyContent"

  // Never print the session key.
  override protected val pretty: Pretty[SequencerChannelSessionKeyContent] =
    prettyOfClass(param("contextHash", _.contextHash))

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.sequencerChannel)(
      v30.SequencerChannelSessionKeyContent
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def apply(
      sessionKey: SymmetricKey,
      contextHash: Hash,
      protocolVersion: ProtocolVersion,
  ): SequencerChannelSessionKeyContent =
    SequencerChannelSessionKeyContent(sessionKey, contextHash)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      contentP: v30.SequencerChannelSessionKeyContent
  ): ParsingResult[SequencerChannelSessionKeyContent] = {
    val v30.SequencerChannelSessionKeyContent(sessionKeyP, contextHashP) = contentP
    for {
      sessionKey <- ProtoConverter.parseRequired(
        SymmetricKey.fromProtoV30,
        "session_key",
        sessionKeyP,
      )
      contextHash <- Hash.fromProtoPrimitive(contextHashP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencerChannelSessionKeyContent(sessionKey, contextHash)(rpv)
  }
}
