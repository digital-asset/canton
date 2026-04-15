// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

final case class HandshakeResponse(serverProtocolVersion: ProtocolVersion)

object HandshakeResponse {

  def fromProtoV30(
      responseP: v30.SequencerConnect.HandshakeResponse
  ): ParsingResult[HandshakeResponse] =
    responseP.value match {
      case v30.SequencerConnect.HandshakeResponse.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("HandshakeResponse.value"))
      case v30.SequencerConnect.HandshakeResponse.Value.Success(_success) =>
        ProtocolVersion
          .fromProtoPrimitive(responseP.serverProtocolVersion)
          .map(HandshakeResponse.apply)
    }
}
