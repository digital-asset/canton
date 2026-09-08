// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfContractId

sealed trait LegacyKeyResolution extends Product with Serializable with PrettyPrinting {
  def resolution: Option[LfContractId]
}

sealed trait LegacyKeyResolutionWithMaintainers extends LegacyKeyResolution {
  def maintainers: Set[LfPartyId]

  def asSerializable: LegacySerializableKeyResolution
}

object LegacyKeyResolutionWithMaintainers {
  def tryFromNextGen(krm: KeyResolutionWithMaintainers): LegacyKeyResolutionWithMaintainers =
    (krm.contracts, krm.maintainers) match {
      case (Seq(contractId), maintainers) if maintainers.isEmpty =>
        LegacyAssignedKeyWithMaintainers(contractId, Set.empty)
      case (Nil, maintainers) =>
        LegacyFreeKey(maintainers)
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot convert $krm to LegacyKeyResolutionWithMaintainers"
        )
    }
}

sealed trait LegacySerializableKeyResolution extends LegacyKeyResolution {
  def tryToNextGen(): KeyResolutionWithMaintainers = this match {
    case LegacyAssignedKey(contractId) =>
      KeyResolutionWithMaintainers(Seq(contractId), Set.empty)
    case LegacyFreeKey(maintainers) =>
      KeyResolutionWithMaintainers(Seq.empty, maintainers)
  }
}

final case class LegacyAssignedKey(contractId: LfContractId)
    extends LegacySerializableKeyResolution {
  override protected def pretty: Pretty[LegacyAssignedKey] =
    prettyNode("Assigned", unnamedParam(_.contractId))

  override def resolution: Option[LfContractId] = Some(contractId)
}

final case class LegacyFreeKey(override val maintainers: Set[LfPartyId])
    extends LegacySerializableKeyResolution
    with LegacyKeyResolutionWithMaintainers {
  override protected def pretty: Pretty[LegacyFreeKey] =
    prettyNode("Free", param("maintainers", _.maintainers))

  override def resolution: Option[LfContractId] = None

  override def asSerializable: LegacySerializableKeyResolution = this
}

final case class LegacyAssignedKeyWithMaintainers(
    contractId: LfContractId,
    override val maintainers: Set[LfPartyId],
) extends LegacyKeyResolutionWithMaintainers {
  override def resolution: Option[LfContractId] = Some(contractId)

  override protected def pretty: Pretty[LegacyAssignedKeyWithMaintainers] = prettyOfClass(
    unnamedParam(_.contractId),
    param("maintainers", _.maintainers),
  )

  override def asSerializable: LegacySerializableKeyResolution =
    LegacyAssignedKey(contractId)
}
