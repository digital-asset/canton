// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.protocol.v0
import cats.syntax.traverse._
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0
import com.digitalasset.canton.version.ProtocolVersion

final case class HandshakeRequest(
    clientProtocolVersions: Seq[ProtocolVersion],
    minimumProtocolVersion: Option[ProtocolVersion],
) extends HasProtoV0[v0.Handshake.Request] {

  override def toProtoV0: v0.Handshake.Request =
    v0.Handshake.Request(
      clientProtocolVersions.map(_.fullVersion),
      minimumProtocolVersion.map(_.fullVersion),
    )

  /* We allow serializing this message to a ByteArray despite it not implementing HasVersionedWrapper because the serialization
   is (and should only be used) in the HttpSequencerClient.
  If you need to save this message in a database, please add a Versioned... message as documented in CONTRIBUTING.md  */
  def toByteArrayV0: Array[Byte] = toProtoV0.toByteArray

}

object HandshakeRequest {
  def fromProtoV0(
      requestP: v0.Handshake.Request
  ): ParsingResult[HandshakeRequest] =
    for {
      clientProtocolVersions <- requestP.clientProtocolVersions.traverse(version =>
        ProtocolVersion.fromProtoPrimitive(version)
      )
      minimumProtocolVersion <- requestP.minimumProtocolVersion.traverse(
        ProtocolVersion.fromProtoPrimitive(_)
      )
    } yield HandshakeRequest(clientProtocolVersions, minimumProtocolVersion)
}
