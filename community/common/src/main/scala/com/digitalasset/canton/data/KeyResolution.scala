// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.{LfContractId, LfTransactionVersion, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

sealed trait KeyResolution extends Product with Serializable with PrettyPrinting {
  def resolution: Option[LfContractId]

  /** lf version of the key */
  def version: LfTransactionVersion

  /** Whether all usages of this key in this view (including subviews) have a different rollback scope than the view itself.
    * Introduced in [[com.digitalasset.canton.version.ProtocolVersion.v3_0_0]];
    * always `false` in earlier Protocol versions.
    */
  def rolledBack: Boolean

  def withRolledBack(rolledBack: Boolean): KeyResolution
}

sealed trait KeyResolutionWithMaintainers extends KeyResolution {
  def maintainers: Set[LfPartyId]

  def asSerializable: SerializableKeyResolution

  override def withRolledBack(rolledBack: Boolean): KeyResolutionWithMaintainers
}

sealed trait SerializableKeyResolution extends KeyResolution {
  def toProtoOneOfV0: v0.ViewParticipantData.ResolvedKey.Resolution

  override def withRolledBack(rolledBack: Boolean): SerializableKeyResolution
}

object SerializableKeyResolution {
  def fromProtoOneOfV0(
      resolutionP: v0.ViewParticipantData.ResolvedKey.Resolution,
      version: LfTransactionVersion,
  ): ParsingResult[SerializableKeyResolution] =
    resolutionP match {
      case v0.ViewParticipantData.ResolvedKey.Resolution.ContractId(contractIdP) =>
        LfContractId
          .fromProtoPrimitive(contractIdP)
          .map(AssignedKey(_, rolledBack = false)(version))
      case v0.ViewParticipantData.ResolvedKey.Resolution
            .Free(v0.ViewParticipantData.FreeKey(maintainersP)) =>
        maintainersP
          .traverse(ProtoConverter.parseLfPartyId)
          .map(maintainers => FreeKey(maintainers.toSet, rolledBack = false)(version))
      case v0.ViewParticipantData.ResolvedKey.Resolution.Empty =>
        Left(FieldNotSet("ViewParticipantData.ResolvedKey.resolution"))
    }
}

case class AssignedKey(contractId: LfContractId, override val rolledBack: Boolean)(
    override val version: LfTransactionVersion
) extends SerializableKeyResolution {
  override def pretty: Pretty[AssignedKey] =
    prettyNode("Assigned", unnamedParam(_.contractId))

  override def resolution: Option[LfContractId] = Some(contractId)

  override def toProtoOneOfV0: v0.ViewParticipantData.ResolvedKey.Resolution =
    v0.ViewParticipantData.ResolvedKey.Resolution.ContractId(value = contractId.toProtoPrimitive)

  override def withRolledBack(rolledBack: Boolean): AssignedKey =
    copy(rolledBack = rolledBack)(version = version)
}

case class FreeKey(override val maintainers: Set[LfPartyId], override val rolledBack: Boolean)(
    override val version: LfTransactionVersion
) extends SerializableKeyResolution
    with KeyResolutionWithMaintainers {
  override def pretty: Pretty[FreeKey] = prettyNode("Free", param("maintainers", _.maintainers))

  override def resolution: Option[LfContractId] = None

  override def toProtoOneOfV0: v0.ViewParticipantData.ResolvedKey.Resolution =
    v0.ViewParticipantData.ResolvedKey.Resolution.Free(
      value = v0.ViewParticipantData.FreeKey(maintainers = maintainers.toSeq)
    )

  override def asSerializable: SerializableKeyResolution = this

  override def withRolledBack(rolledBack: Boolean): FreeKey =
    copy(rolledBack = rolledBack)(version = version)
}

case class AssignedKeyWithMaintainers(
    contractId: LfContractId,
    override val maintainers: Set[LfPartyId],
    override val rolledBack: Boolean,
)(override val version: LfTransactionVersion)
    extends KeyResolutionWithMaintainers {
  override def resolution: Option[LfContractId] = Some(contractId)

  override def pretty: Pretty[AssignedKeyWithMaintainers] = prettyOfClass(
    unnamedParam(_.contractId),
    param("maintainers", _.maintainers),
  )

  override def asSerializable: SerializableKeyResolution =
    AssignedKey(contractId, rolledBack)(version)

  override def withRolledBack(rolledBack: Boolean): AssignedKeyWithMaintainers =
    copy(rolledBack = rolledBack)(version = version)
}
