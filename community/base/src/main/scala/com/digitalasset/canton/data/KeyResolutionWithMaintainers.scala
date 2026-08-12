// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.LfContractIdSyntax
import com.digitalasset.canton.protocol.{GlobalKeySerialization, LfContractId, LfGlobalKey, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation
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
      maintainers = resolution.unversioned.maintainers.toSeq.map(_.toProtoUnvalidated),
      contractIds = resolution.unversioned.contracts.map(_.toProtoPrimitive),
    )

  def fromProtoV31(
      pvv: ProtocolVersionValidation,
      resolutionP: v31.ViewParticipantData.KeyResolutionWithMaintainers,
  ): ParsingResult[(LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers])] = {
    val v31.ViewParticipantData.KeyResolutionWithMaintainers(keyP, maintainersP, contractIdsP) =
      resolutionP
    for {
      key <- ProtoConverter
        .required("KeyResolutionWithMaintainers.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV31(pvv, _))
      contractIds <- ProtoValidation
        .validateThen(contractIdsP, "contract_ids", pvv)(
          ProtoConverter.parseLfContractId
        )
      maintainers <- ProtoValidation.validateThen(
        maintainersP,
        "maintainers",
        pvv,
      )(ProtoConverter.parseLfPartyId)
    } yield (
      key.unversioned,
      LfVersioned(
        key.version,
        KeyResolutionWithMaintainers(contractIds.toVector, maintainers.toSet),
      ),
    )
  }

}
