// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service.PartyDetails as ProtoPartyDetails
import com.digitalasset.canton.LfPartyId

import scala.util.control.NoStackTrace

/** Represents a party details value exposed in the Canton console
  */
case class PartyDetails(
    party: LfPartyId,
    displayName: String,
    isLocal: Boolean,
    annotations: Map[String, String],
)

object PartyDetails {
  def fromProtoPartyDetails(details: ProtoPartyDetails): PartyDetails = PartyDetails(
    party = LfPartyId.assertFromString(details.party),
    displayName = details.displayName,
    isLocal = details.isLocal,
    annotations = details.localMetadata.fold(Map.empty[String, String])(_.annotations),
  )
  def toProtoPartyDetails(
      details: PartyDetails,
      resourceVersionO: Option[String],
  ): ProtoPartyDetails = ProtoPartyDetails(
    party = details.party.toString,
    displayName = details.displayName,
    isLocal = details.isLocal,
    localMetadata = Some(
      ProtoObjectMeta(
        resourceVersion = resourceVersionO.getOrElse(""),
        annotations = details.annotations,
      )
    ),
  )
}

case class ModifyingNonModifiablePartyDetailsPropertiesError()
    extends RuntimeException("MODIFYING_AN_UNMODIFIABLE_PARTY_DETAILS_PROPERTY_ERROR")
    with NoStackTrace
