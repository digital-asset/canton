// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

sealed trait KeyResolution extends Product with Serializable with PrettyPrinting {
  def resolution: Option[LfContractId]

  def toProtoOneOf: v0.ViewParticipantData.ResolvedKey.Resolution
}

object KeyResolution {
  def fromProtoOneOf(
      resolutionP: v0.ViewParticipantData.ResolvedKey.Resolution
  ): ParsingResult[KeyResolution] =
    resolutionP match {
      case v0.ViewParticipantData.ResolvedKey.Resolution.ContractId(contractIdP) =>
        LfContractId.fromProtoPrimitive(contractIdP).map(AssignedKey)
      case v0.ViewParticipantData.ResolvedKey.Resolution
            .Free(v0.ViewParticipantData.FreeKey(maintainersP)) =>
        maintainersP
          .traverse(ProtoConverter.parseLfPartyId)
          .map(maintainers => FreeKey(maintainers.toSet))
      case v0.ViewParticipantData.ResolvedKey.Resolution.Empty =>
        Left(FieldNotSet("ViewParticipantData.ResolvedKey.resolution"))
    }
}

case class AssignedKey(contractId: LfContractId) extends KeyResolution {
  override def pretty: Pretty[AssignedKey] = prettyNode("Assigned", unnamedParam(_.contractId))

  override def resolution: Option[LfContractId] = Some(contractId)

  override def toProtoOneOf: v0.ViewParticipantData.ResolvedKey.Resolution =
    v0.ViewParticipantData.ResolvedKey.Resolution.ContractId(value = contractId.toProtoPrimitive)
}

case class FreeKey(maintainers: Set[LfPartyId]) extends KeyResolution {
  override def pretty: Pretty[FreeKey] = prettyNode("Free", param("maintainers", _.maintainers))

  override def resolution: Option[LfContractId] = None

  override def toProtoOneOf: v0.ViewParticipantData.ResolvedKey.Resolution =
    v0.ViewParticipantData.ResolvedKey.Resolution.Free(
      value = v0.ViewParticipantData.FreeKey(maintainers = maintainers.toSeq)
    )
}
