// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfVersioned
import com.digitalasset.canton.data.KeyResolution
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.HasProtoV0

case class ResolvedKey(key: LfGlobalKey, resolution: KeyResolution)
    extends HasProtoV0[v0.ViewParticipantData.ResolvedKey] {
  override def toProtoV0: v0.ViewParticipantData.ResolvedKey =
    v0.ViewParticipantData.ResolvedKey(
      // oddity: pass the version from resolution to the proto-key
      key = Some(GlobalKeySerialization.assertToProto(LfVersioned(resolution.version, key))),
      resolution = resolution.toProtoOneOf,
    )
}

object ResolvedKey {
  def fromProtoV0(
      resolvedKeyP: v0.ViewParticipantData.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v0.ViewParticipantData.ResolvedKey(keyP, resolutionP) = resolvedKeyP
    for {
      keyWithVersion <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      LfVersioned(version, key) = keyWithVersion
      // oddity: pass the version from the proto-key to resolution
      resolution <- KeyResolution.fromProtoOneOf(resolutionP, version)
    } yield ResolvedKey(key, resolution)
  }
}
