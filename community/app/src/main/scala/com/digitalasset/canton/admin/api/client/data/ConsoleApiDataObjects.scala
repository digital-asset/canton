// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import java.time.Instant
import cats.syntax.either._
import cats.syntax.traverse._

import com.daml.ledger.api.v1.admin.user_management_service.Right.Kind
import com.digitalasset.canton.admin.api.client.data.ListPartiesResult.ParticipantDomains
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.admin.v0
import com.digitalasset.canton.participant.admin.{v0 => participantAdminV0}
import com.daml.ledger.api.v1.admin.user_management_service.{
  ListUsersResponse => ProtoListUsersResponse,
  Right => ProtoUserRight,
  User => ProtoLedgerApiUser,
}
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportResponse,
  ParticipantMeteringReport,
  ApplicationMeteringReport => ProtoApplicationMeteringReport,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{DomainAlias, DomainId, LfPartyId, ProtoDeserializationError}
import com.google.protobuf.ByteString

/** A tagged class used to return exported private keys */
case class SerializedPrivateKey(payload: String)

case class ListPartiesResult(party: PartyId, participants: Seq[ParticipantDomains])

object ListPartiesResult {
  case class DomainPermission(domain: DomainId, permission: ParticipantPermission)
  case class ParticipantDomains(participant: ParticipantId, domains: Seq[DomainPermission])

  private def fromProtoV0(
      value: v0.ListPartiesResponse.Result.ParticipantDomains.DomainPermissions
  ): ParsingResult[DomainPermission] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      permission <- ParticipantPermission.fromProtoEnum(value.permission)
    } yield DomainPermission(domainId, permission)

  private def fromProtoV0(
      value: v0.ListPartiesResponse.Result.ParticipantDomains
  ): ParsingResult[ParticipantDomains] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      domains <- value.domains.traverse(fromProtoV0)
    } yield ParticipantDomains(participantId, domains)

  def fromProtoV0(
      value: v0.ListPartiesResponse.Result
  ): ParsingResult[ListPartiesResult] =
    for {
      partyUid <- UniqueIdentifier.fromProtoPrimitive(value.party, "party")
      participants <- value.participants.traverse(fromProtoV0)
    } yield ListPartiesResult(PartyId(partyUid), participants)
}

case class ListKeyOwnersResult(
    store: DomainId,
    owner: KeyOwner,
    signingKeys: Seq[SigningPublicKey],
    encryptionKeys: Seq[EncryptionPublicKey],
)

object ListKeyOwnersResult {
  def fromProtoV0(
      value: v0.ListKeyOwnersResponse.Result
  ): ParsingResult[ListKeyOwnersResult] =
    for {
      domain <- DomainId.fromProtoPrimitive(value.domain, "domain")
      owner <- KeyOwner.fromProtoPrimitive(value.keyOwner, "keyOwner")
      signingKeys <- value.signingKeys.traverse(SigningPublicKey.fromProtoV0)
      encryptionKeys <- value.encryptionKeys.traverse(EncryptionPublicKey.fromProtoV0)
    } yield ListKeyOwnersResult(domain, owner, signingKeys, encryptionKeys)
}

case class BaseResult(
    domain: String,
    validFrom: Instant,
    validUntil: Option[Instant],
    operation: TopologyChangeOp,
    serialized: ByteString,
    signedBy: Fingerprint,
)

object BaseResult {
  def fromProtoV0(value: v0.BaseResult): ParsingResult[BaseResult] =
    for {
      protoValidFrom <- ProtoConverter.required("valid_from", value.validFrom)
      validFrom <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoValidFrom)
      validUntil <- value.validUntil.traverse(ProtoConverter.InstantConverter.fromProtoPrimitive)
      operation <- TopologyChangeOp.fromProtoV0(value.operation)
      signedBy <- Fingerprint.fromProtoPrimitive(value.signedByFingerprint)

    } yield BaseResult(value.store, validFrom, validUntil, operation, value.serialized, signedBy)
}

case class ListPartyToParticipantResult(context: BaseResult, item: PartyToParticipant)

object ListPartyToParticipantResult {
  def fromProtoV0(
      value: v0.ListPartyToParticipantResult.Result
  ): ParsingResult[ListPartyToParticipantResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyToParticipant.fromProtoV0(itemProto)
    } yield ListPartyToParticipantResult(context, item)
}

case class ListOwnerToKeyMappingResult(
    context: BaseResult,
    item: OwnerToKeyMapping,
    key: Fingerprint,
)

object ListOwnerToKeyMappingResult {
  def fromProtoV0(
      value: v0.ListOwnerToKeyMappingResult.Result
  ): ParsingResult[ListOwnerToKeyMappingResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- OwnerToKeyMapping.fromProtoV0(itemProto)
      key <- Fingerprint.fromProtoPrimitive(value.keyFingerprint)
    } yield ListOwnerToKeyMappingResult(context, item, key)
}

case class ListNamespaceDelegationResult(
    context: BaseResult,
    item: NamespaceDelegation,
    targetKey: Fingerprint,
)

object ListNamespaceDelegationResult {
  def fromProtoV0(
      value: v0.ListNamespaceDelegationResult.Result
  ): ParsingResult[ListNamespaceDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- NamespaceDelegation.fromProtoV0(itemProto)
      targetKey <- Fingerprint.fromProtoPrimitive(value.targetKeyFingerprint)
    } yield ListNamespaceDelegationResult(context, item, targetKey)
}

case class ListIdentifierDelegationResult(
    context: BaseResult,
    item: IdentifierDelegation,
    targetKey: Fingerprint,
)

object ListIdentifierDelegationResult {
  def fromProtoV0(
      value: v0.ListIdentifierDelegationResult.Result
  ): ParsingResult[ListIdentifierDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- IdentifierDelegation.fromProtoV0(itemProto)
      targetKey <- Fingerprint.fromProtoPrimitive(value.targetKeyFingerprint)
    } yield ListIdentifierDelegationResult(context, item, targetKey)
}

case class ListSignedLegalIdentityClaimResult(context: BaseResult, item: LegalIdentityClaim)

object ListSignedLegalIdentityClaimResult {
  def fromProtoV0(
      value: v0.ListSignedLegalIdentityClaimResult.Result
  ): ParsingResult[ListSignedLegalIdentityClaimResult] = {
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SignedLegalIdentityClaim.fromProtoV0(itemProto)
      claim <- LegalIdentityClaim.fromByteString(item.claim)
    } yield ListSignedLegalIdentityClaimResult(context, claim)
  }
}

case class ListParticipantDomainStateResult(context: BaseResult, item: ParticipantState)

object ListParticipantDomainStateResult {
  def fromProtoV0(
      value: v0.ListParticipantDomainStateResult.Result
  ): ParsingResult[ListParticipantDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- ParticipantState.fromProtoV0(itemProto)
    } yield ListParticipantDomainStateResult(context, item)

}

case class ListMediatorDomainStateResult(context: BaseResult, item: MediatorDomainState)

object ListMediatorDomainStateResult {
  def fromProtoV0(
      value: v0.ListMediatorDomainStateResult.Result
  ): ParsingResult[ListMediatorDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- MediatorDomainState.fromProtoV0(itemProto)
    } yield ListMediatorDomainStateResult(context, item)

}

case class CertificateResult(certificateId: CertificateId, x509Pem: String)

object CertificateResult {
  def fromPem(x509Pem: String): Either[X509CertificateError, CertificateResult] = {
    for {
      pem <- X509CertificatePem
        .fromString(x509Pem)
        .leftMap(err => X509CertificateError.DecodingError(err))
      cert <- X509Certificate.fromPem(pem)
    } yield CertificateResult(cert.id, x509Pem)
  }
}

case class ListVettedPackagesResult(context: BaseResult, item: VettedPackages)

object ListVettedPackagesResult {
  def fromProtoV0(
      value: v0.ListVettedPackagesResult.Result
  ): ParsingResult[ListVettedPackagesResult] = {
    val v0.ListVettedPackagesResult.Result(contextPO, itemPO) = value
    for {
      contextProto <- ProtoConverter.required("context", contextPO)
      context <- BaseResult.fromProtoV0(contextProto)
      itemProto <- ProtoConverter.required("item", itemPO)
      item <- VettedPackages.fromProtoV0(itemProto)
    } yield ListVettedPackagesResult(context, item)
  }
}

case class ListDomainParametersChangeResult(context: BaseResult, item: DynamicDomainParameters)

object ListDomainParametersChangeResult {
  def fromProtoV0(
      value: v0.ListDomainParametersChangesResult.Result
  ): ParsingResult[ListDomainParametersChangeResult] = for {
    contextP <- value.context.toRight(ProtoDeserializationError.FieldNotSet("context"))
    context <- BaseResult.fromProtoV0(contextP)
    itemP <- value.item.toRight(ProtoDeserializationError.FieldNotSet("item"))
    item <- DynamicDomainParameters.fromProtoV0(itemP)
  } yield ListDomainParametersChangeResult(context, item)
}

case class ListConnectedDomainsResult(
    domainAlias: DomainAlias,
    domainId: DomainId,
    healthy: Boolean,
)

object ListConnectedDomainsResult {
  def fromProtoV0(
      value: participantAdminV0.ListConnectedDomainsResponse.Result
  ): ParsingResult[ListConnectedDomainsResult] = {
    val participantAdminV0.ListConnectedDomainsResponse.Result(domainAlias, domainId, healthy) =
      value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainId, "domainId")
      domainAlias <- DomainAlias.fromProtoPrimitive(domainAlias)

    } yield ListConnectedDomainsResult(
      domainAlias = domainAlias,
      domainId = domainId,
      healthy = healthy,
    )

  }
}

case class LedgerApiUser(id: String, primaryParty: Option[LfPartyId])

object LedgerApiUser {
  def fromProtoV0(
      value: ProtoLedgerApiUser
  ): ParsingResult[LedgerApiUser] = {
    val ProtoLedgerApiUser(id, primaryParty) = value
    Option
      .when(primaryParty.nonEmpty)(primaryParty)
      .traverse(LfPartyId.fromString)
      .leftMap { err =>
        ProtoDeserializationError.ValueConversionError("primaryParty", err)
      }
      .map { primaryPartyO =>
        LedgerApiUser(id, primaryPartyO)
      }
  }
}

case class UserRights(actAs: Set[LfPartyId], readAs: Set[LfPartyId], participantAdmin: Boolean)
object UserRights {
  def fromProtoV0(
      values: Seq[ProtoUserRight]
  ): ParsingResult[UserRights] = {
    Right(values.map(_.kind).foldLeft(UserRights(Set(), Set(), false)) {
      case (acc, Kind.Empty) => acc
      case (acc, Kind.ParticipantAdmin(value)) => acc.copy(participantAdmin = true)
      case (acc, Kind.CanActAs(value)) =>
        acc.copy(actAs = acc.actAs + LfPartyId.assertFromString(value.party))
      case (acc, Kind.CanReadAs(value)) =>
        acc.copy(readAs = acc.readAs + LfPartyId.assertFromString(value.party))
    })
  }
}

case class ListLedgerApiUsersResult(users: Seq[LedgerApiUser], nextPageToken: String)

object ListLedgerApiUsersResult {
  def fromProtoV0(
      value: ProtoListUsersResponse,
      filterUser: String,
  ): ParsingResult[ListLedgerApiUsersResult] = {
    val ProtoListUsersResponse(protoUsers, nextPageToken) = value
    protoUsers.traverse(LedgerApiUser.fromProtoV0).map { users =>
      ListLedgerApiUsersResult(users.filter(_.id.startsWith(filterUser)), nextPageToken)
    }
  }
}

case class ApplicationMeteringReport(applicationId: String, eventCount: Long)

object ApplicationMeteringReport {
  def fromProtoV0(
      value: ProtoApplicationMeteringReport
  ): ParsingResult[ApplicationMeteringReport] = {
    val ProtoApplicationMeteringReport(applicationId, eventCount) = value
    Right(ApplicationMeteringReport(applicationId, eventCount))
  }
}

case class LedgerMeteringReport(
    from: CantonTimestamp,
    to: CantonTimestamp,
    reports: Seq[ApplicationMeteringReport],
)

object LedgerMeteringReport {

  def fromProtoV0(
      value: GetMeteringReportResponse
  ): ParsingResult[LedgerMeteringReport] = {
    val GetMeteringReportResponse(requestO, participantReportO, reportGenerationTimeO) = value

    for {
      request <- ProtoConverter.required("request", requestO)
      from <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "requestfrom",
        request.from,
      )
      participantReport <- ProtoConverter.required("participantReport", participantReportO)
      ParticipantMeteringReport(
        _,
        toActualP,
        reportsP,
      ) = participantReport
      toActual <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "participantReport.toActual",
        toActualP,
      )
      reports <- reportsP.traverse(ApplicationMeteringReport.fromProtoV0)
    } yield LedgerMeteringReport(from, toActual, reports)

  }
}
