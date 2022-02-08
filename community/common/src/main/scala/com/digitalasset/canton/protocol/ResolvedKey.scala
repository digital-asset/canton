// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.data.KeyResolution
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0

case class ResolvedKey(key: LfGlobalKey, resolution: KeyResolution)
    extends HasProtoV0[v0.ViewParticipantData.ResolvedKey] {
  override def toProtoV0: v0.ViewParticipantData.ResolvedKey =
    v0.ViewParticipantData.ResolvedKey(
      key = Some(GlobalKeySerialization.assertToProto(key)),
      resolution = resolution.toProtoOneOf,
    )
}

object ResolvedKey {
  def fromProtoV0(
      resolvedKeyP: v0.ViewParticipantData.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v0.ViewParticipantData.ResolvedKey(keyP, resolutionP) = resolvedKeyP
    for {
      key <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      resolution <- KeyResolution.fromProtoOneOf(resolutionP)
    } yield ResolvedKey(key, resolution)
  }
}
