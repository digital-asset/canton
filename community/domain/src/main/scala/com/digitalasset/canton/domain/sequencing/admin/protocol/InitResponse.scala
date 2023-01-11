// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.protocol

import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class InitResponse(keyId: String, publicKey: SigningPublicKey, replicated: Boolean) {
  def toProtoV0: v0.InitResponse = v0.InitResponse(
    keyId = keyId,
    publicKey = Some(publicKey.toProtoV0),
    replicated = replicated,
  )
}

object InitResponse {
  def fromProtoV0(response: v0.InitResponse): ParsingResult[InitResponse] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV0,
        "public_key",
        response.publicKey,
      )
    } yield InitResponse(response.keyId, publicKey, response.replicated)
}
