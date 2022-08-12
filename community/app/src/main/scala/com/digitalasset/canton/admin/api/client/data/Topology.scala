// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.console

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.console.ListPartiesResult.ParticipantDomains
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.protocol.DynamicDomainParameters.InvalidDomainParameters
import com.digitalasset.canton.protocol.{
  DynamicDomainParameters => DomainDynamicDomainParameters,
  v0 => protocolV0,
  v1 => protocolV1,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.admin.v0
import com.digitalasset.canton.topology.admin.v0.DomainParametersChangeAuthorization
import com.digitalasset.canton.topology.admin.v0.ListDomainParametersChangesResult.Result.Parameters
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl._

import java.time.Instant
import scala.Ordering.Implicits._

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
) {
  def keys(purpose: KeyPurpose): Seq[PublicKey] = purpose match {
    case KeyPurpose.Signing => signingKeys
    case KeyPurpose.Encryption => encryptionKeys
  }
}

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
    domainDynamicDomainParameters <- value.parameters match {
      case Parameters.Empty => Left(ProtoDeserializationError.FieldNotSet("parameters"))
      case Parameters.V0(v0) => DomainDynamicDomainParameters.fromProtoV0(v0)
      case Parameters.V1(v1) => DomainDynamicDomainParameters.fromProtoV1(v1)
    }
    item <- DynamicDomainParameters(domainDynamicDomainParameters)
  } yield ListDomainParametersChangeResult(context, item)
}

sealed trait DynamicDomainParameters {
  def participantResponseTimeout: TimeoutDuration
  def mediatorReactionTimeout: TimeoutDuration
  def transferExclusivityTimeout: TimeoutDuration
  def topologyChangeDelay: NonNegativeFiniteDuration
  def ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration

  /** Checks if it is safe to change the `ledgerTimeRecordTimeTolerance` to the given new value.
    */
  private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean

  /** Convert the parameters to the Protobuf representation
    * @param protocolVersion Protocol version used on the domain; used to ensure that we don't send parameters
    *                        that are not dynamic yet (e.g., reconciliation interval in a domain running an old
    *                        protocol version).
    */
  def toProto(
      protocolVersion: ProtocolVersion
  ): Either[String, v0.DomainParametersChangeAuthorization.Parameters]

  protected def protobufVersion(protocolVersion: ProtocolVersion): Int =
    DomainDynamicDomainParameters.protobufVersionFor(protocolVersion).v

  def update(
      participantResponseTimeout: TimeoutDuration = participantResponseTimeout,
      mediatorReactionTimeout: TimeoutDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: TimeoutDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
  ): DynamicDomainParameters
}

final case class DynamicDomainParametersV0(
    participantResponseTimeout: TimeoutDuration,
    mediatorReactionTimeout: TimeoutDuration,
    transferExclusivityTimeout: TimeoutDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
) extends DynamicDomainParameters {

  override private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean = true // always safe, because we don't have mediatorDeduplicationTimeout

  override def update(
      participantResponseTimeout: TimeoutDuration = participantResponseTimeout,
      mediatorReactionTimeout: TimeoutDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: TimeoutDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
  ): DynamicDomainParameters = DynamicDomainParametersV0(
    participantResponseTimeout = participantResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
  )

  override def toProto(
      protocolVersion: ProtocolVersion
  ): Either[String, DomainParametersChangeAuthorization.Parameters] =
    if (protobufVersion(protocolVersion) == 0)
      Right(
        protocolV0.DynamicDomainParameters(
          participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
          mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
          transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
          topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
          ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
        )
      ).map(DomainParametersChangeAuthorization.Parameters.ParametersV0)
    else
      Left(
        s"Cannot convert DynamicDomainParametersV0 to Protobuf when domain protocol version is $protocolVersion"
      )
}

final case class DynamicDomainParametersV1(
    participantResponseTimeout: TimeoutDuration,
    mediatorReactionTimeout: TimeoutDuration,
    transferExclusivityTimeout: TimeoutDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveSeconds,
) extends DynamicDomainParameters {

  if (ledgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2) > mediatorDeduplicationTimeout)
    throw new InvalidDomainParameters(
      s"The ledgerTimeRecordTimeTolerance ($ledgerTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
  override private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean = {
    // If false, a new request may receive the same ledger time as a previous request and the previous
    // request may be evicted too early from the mediator's deduplication store.
    // Thus, an attacker may assign the same UUID to both requests.
    // See i9028 for a detailed design. (This is the second clause of item 2 of Lemma 2).
    ledgerTimeRecordTimeTolerance + newLedgerTimeRecordTimeTolerance <= mediatorDeduplicationTimeout
  }

  override def update(
      participantResponseTimeout: TimeoutDuration = participantResponseTimeout,
      mediatorReactionTimeout: TimeoutDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: TimeoutDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
  ): DynamicDomainParameters = DynamicDomainParametersV1(
    participantResponseTimeout = participantResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
    reconciliationInterval = reconciliationInterval,
  )

  override def toProto(
      protocolVersion: ProtocolVersion
  ): Either[String, DomainParametersChangeAuthorization.Parameters] =
    if (protobufVersion(protocolVersion) == 1)
      Right(
        protocolV1.DynamicDomainParameters(
          participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
          mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
          transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
          topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
          ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
          mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
          reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
        )
      ).map(DomainParametersChangeAuthorization.Parameters.ParametersV1)
    else
      Left(
        s"Cannot convert DynamicDomainParametersV1 to Protobuf when domain protocol version is $protocolVersion"
      )
}

object DynamicDomainParameters {

  /** Default dynamic domain parameters for non-static clocks */
  def defaultValues(protocolVersion: ProtocolVersion): DynamicDomainParameters =
    DynamicDomainParameters(
      DomainDynamicDomainParameters.defaultValues(protocolVersion)
    ).fold(err => throw new RuntimeException(err.message), identity)

  def apply(
      domain: DomainDynamicDomainParameters
  ): Either[ProtoDeserializationError.VersionError, DynamicDomainParameters] = {
    val protobufVersion = domain.protobufVersion.v

    if (protobufVersion == 0)
      Right(domain.transformInto[DynamicDomainParametersV0])
    else if (protobufVersion == 1)
      Right(domain.transformInto[DynamicDomainParametersV1])
    else
      Left(ProtoDeserializationError.VersionError("DynamicDomainParameters", protobufVersion))
  }
}
