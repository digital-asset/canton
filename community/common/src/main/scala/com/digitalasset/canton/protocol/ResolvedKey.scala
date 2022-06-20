// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse._
import com.digitalasset.canton.LfVersioned
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.{AssignedKey, FreeKey, SerializableKeyResolution}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.HasProtoV0

case class ResolvedKey(key: LfGlobalKey, resolution: SerializableKeyResolution)
    extends HasProtoV0[v0.ViewParticipantData.ResolvedKey] {
  override def toProtoV0: v0.ViewParticipantData.ResolvedKey =
    v0.ViewParticipantData.ResolvedKey(
      // oddity: pass the version from resolution to the proto-key
      key = Some(GlobalKeySerialization.assertToProto(LfVersioned(resolution.version, key))),
      resolution = resolution.toProtoOneOfV0,
    )

  def toProtoV1: v1.ResolvedKey =
    v1.ResolvedKey(
      // oddity: pass the version from resolution to the proto-key
      key = Some(GlobalKeySerialization.assertToProto(LfVersioned(resolution.version, key))),
      resolution = serializeKeyResolutionV1(resolution),
      rolledBack = resolution.rolledBack,
    )

  private def serializeKeyResolutionV1(
      resolution: SerializableKeyResolution
  ): v1.ResolvedKey.Resolution = {
    resolution match {
      case FreeKey(maintainers, rolledBack) =>
        v1.ResolvedKey.Resolution.Free(
          value = v0.ViewParticipantData.FreeKey(maintainers = maintainers.toSeq)
        )
      case AssignedKey(contractId, rolledBack) =>
        v1.ResolvedKey.Resolution
          .ContractId(value = contractId.toProtoPrimitive)
    }
  }
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
      resolution <- SerializableKeyResolution.fromProtoOneOfV0(resolutionP, version)
    } yield ResolvedKey(key, resolution)
  }

  def fromProtoV1(
      resolvedKeyP: v1.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v1.ResolvedKey(keyP, resolutionP, rolledBack) = resolvedKeyP
    for {
      keyWithVersion <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      LfVersioned(version, key) = keyWithVersion
      resolution <- resolutionP match {
        case v1.ResolvedKey.Resolution.ContractId(contractIdP) =>
          LfContractId.fromProtoPrimitive(contractIdP).map(AssignedKey(_, rolledBack)(version))
        case v1.ResolvedKey.Resolution
              .Free(v0.ViewParticipantData.FreeKey(maintainersP)) =>
          maintainersP
            .traverse(ProtoConverter.parseLfPartyId)
            .map(maintainers => FreeKey(maintainers.toSet, rolledBack)(version))
        case v1.ResolvedKey.Resolution.Empty =>
          Left(FieldNotSet("ViewParticipantData.ResolvedKey.resolution"))
      }
    } yield ResolvedKey(key, resolution)
  }
}
