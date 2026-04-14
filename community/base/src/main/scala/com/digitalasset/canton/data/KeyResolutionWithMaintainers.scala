// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.implicits.toTraverseOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.LfContractIdSyntax
import com.digitalasset.canton.protocol.{GlobalKeySerialization, LfContractId, LfGlobalKey, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfPartyId, LfVersioned}

final case class KeyResolutionWithMaintainers(
    contracts: Seq[LfContractId],
    maintainers: Set[LfPartyId],
) extends PrettyPrinting {
  override protected def pretty: Pretty[KeyResolutionWithMaintainers.this.type] = prettyOfClass(
    param("contracts", _.contracts),
    param("maintainers", _.maintainers),
  )
}

object KeyResolutionWithMaintainers {

  def toProtoV31(
      key: LfGlobalKey,
      resolution: LfVersioned[KeyResolutionWithMaintainers],
  ): v31.ViewParticipantData.KeyResolutionWithMaintainers =
    v31.ViewParticipantData.KeyResolutionWithMaintainers(
      key = Some(GlobalKeySerialization.assertToProtoV31(resolution.map(_ => key))),
      maintainers = resolution.unversioned.maintainers.toSeq,
      contractIds = resolution.unversioned.contracts.map(_.toProtoPrimitive),
    )

  def fromProtoV31(
      resolutionP: v31.ViewParticipantData.KeyResolutionWithMaintainers
  ): ParsingResult[(LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers])] = {
    val v31.ViewParticipantData.KeyResolutionWithMaintainers(keyP, maintainersP, contractIdsP) =
      resolutionP
    for {
      key <- ProtoConverter
        .required("KeyResolutionWithMaintainers.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV31)
      contractIds <- contractIdsP.traverse(ProtoConverter.parseLfContractId)
      maintainers <- maintainersP.traverse(ProtoConverter.parseLfPartyId(_, "maintainers"))
    } yield (
      key.unversioned,
      LfVersioned(
        key.version,
        KeyResolutionWithMaintainers(contractIds.toVector, maintainers.toSet),
      ),
    )
  }

}
