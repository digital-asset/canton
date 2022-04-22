// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{HasProtoV0, ProtocolVersion}

sealed trait HandshakeResponse extends HasProtoV0[v0.Handshake.Response] {
  val serverVersion: ProtocolVersion

  override def toProtoV0: v0.Handshake.Response
}

object HandshakeResponse {
  case class Success(serverVersion: ProtocolVersion) extends HandshakeResponse {
    override def toProtoV0: v0.Handshake.Response =
      v0.Handshake.Response(
        serverVersion.fullVersion,
        v0.Handshake.Response.Value.Success(v0.Handshake.Success()),
      )
  }
  case class Failure(serverVersion: ProtocolVersion, reason: String) extends HandshakeResponse {
    override def toProtoV0: v0.Handshake.Response =
      v0.Handshake
        .Response(
          serverVersion.fullVersion,
          v0.Handshake.Response.Value.Failure(v0.Handshake.Failure(reason)),
        )
  }

  def fromProtoV0(
      responseP: v0.Handshake.Response
  ): ParsingResult[HandshakeResponse] =
    responseP.value match {
      case v0.Handshake.Response.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("Handshake.Response.value"))
      case v0.Handshake.Response.Value.Success(success) =>
        ProtocolVersion.fromProtoPrimitive(responseP.serverVersion).map(Success)
      case v0.Handshake.Response.Value.Failure(failure) =>
        ProtocolVersion.fromProtoPrimitive(responseP.serverVersion).map(Failure(_, failure.reason))
    }
}
