// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PartyId, UniqueIdentifier}
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.google.protobuf.ByteString

/** A template-bound party mapping. The party's signing key is destroyed after
  * registration. The party can only act through the whitelisted templates.
  *
  * The hosting participant auto-confirms transactions where the party is
  * signatory/controller, as long as all actions are on allowed templates.
  *
  * See CIP-draft-template-bound-parties.
  *
  * @param partyId the party whose key is destroyed
  * @param hostingParticipantId the participant that auto-confirms on behalf of this party
  * @param allowedTemplateIds fully qualified Daml template IDs
  * @param signingKeyHash hash of the signing public key (proof the key existed)
  */
final case class TemplateBoundPartyMapping(
    partyId: PartyId,
    hostingParticipantId: ParticipantId,
    allowedTemplateIds: Set[String],
    signingKeyHash: ByteString,
) extends TopologyMapping {

  override def code: TopologyMapping.Code = TopologyMapping.Code.TemplateBoundParty

  override def namespace: com.digitalasset.canton.topology.Namespace = partyId.namespace

  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToDomain: Option[com.digitalasset.canton.topology.SynchronizerId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyMapping.Code]]
  ): TopologyMapping.RequiredAuth =
    // Registration must be authorized by the party's namespace (which owns the key
    // that signs this one transaction before being destroyed)
    TopologyMapping.RequiredAuth.NamespaceOnly(namespace)

  def toProtoV30: v30.TemplateBoundParty =
    v30.TemplateBoundParty(
      party = partyId.toProtoPrimitive,
      hostingParticipantUid = hostingParticipantId.uid.toProtoPrimitive,
      allowedTemplateIds = allowedTemplateIds.toSeq,
      signingKeyHash = signingKeyHash,
    )

  override def toProto: v30.TopologyMapping =
    v30.TopologyMapping(
      mapping = v30.TopologyMapping.Mapping.TemplateBoundParty(toProtoV30)
    )

  override def pretty: Pretty[TemplateBoundPartyMapping] = prettyOfClass(
    param("partyId", _.partyId),
    param("hostingParticipantId", _.hostingParticipantId),
    param("allowedTemplateIds", _.allowedTemplateIds.size),
  )

  /** Check whether a given template ID is in the allowed set. */
  def isTemplateAllowed(templateId: String): Boolean =
    allowedTemplateIds.contains(templateId)
}

object TemplateBoundPartyMapping {

  def fromProtoV30(
      proto: v30.TemplateBoundParty
  ): ParsingResult[TemplateBoundPartyMapping] =
    for {
      partyId <- PartyId.fromProtoPrimitive(proto.party, "party")
      participantId <- ParticipantId.fromProtoPrimitive(
        proto.hostingParticipantUid,
        "hosting_participant_uid",
      )
    } yield TemplateBoundPartyMapping(
      partyId = partyId,
      hostingParticipantId = participantId,
      allowedTemplateIds = proto.allowedTemplateIds.toSet,
      signingKeyHash = proto.signingKeyHash,
    )
}
