// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.data.{NonEmptyList, NonEmptySet}
import cats.instances.either._
import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.version.{
  VersionedDynamicDomainParameters,
  VersionedStaticDomainParameters,
}
import com.digitalasset.canton.protocol.{v0 => protoV0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, HasVersionedWrapperCompanion}
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration
import scala.Ordered.orderingToOrdered

final case class StaticDomainParameters(
    reconciliationInterval: PositiveSeconds,
    maxRatePerParticipant: NonNegativeInt,
    maxInboundMessageSize: NonNegativeInt,
    uniqueContractKeys: Boolean,
    requiredSigningKeySchemes: NonEmptySet[SigningKeyScheme],
    requiredEncryptionKeySchemes: NonEmptySet[EncryptionKeyScheme],
    requiredSymmetricKeySchemes: NonEmptySet[SymmetricKeyScheme],
    requiredHashAlgorithms: NonEmptySet[HashAlgorithm],
    requiredCryptoKeyFormats: NonEmptySet[CryptoKeyFormat],
    protocolVersion: ProtocolVersion,
) extends HasVersionedWrapper[VersionedStaticDomainParameters]
    with HasProtoV0[protoV0.StaticDomainParameters] {

  /** Compute the max size limit for the sum of the envelope payloads of a batch
    *
    * taking 0.9 of the max inbound size, assuming that this will be enough to accommodate any metadata
    * and the list of recipients.
    */
  def maxBatchMessageSize: NonNegativeInt =
    NonNegativeInt.tryCreate((0.9 * maxInboundMessageSize.unwrap).toInt)

  override def toProtoVersioned(version: ProtocolVersion): VersionedStaticDomainParameters =
    VersionedStaticDomainParameters(VersionedStaticDomainParameters.Version.V0(toProtoV0))

  override def toProtoV0: protoV0.StaticDomainParameters =
    protoV0.StaticDomainParameters(
      reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
      maxInboundMessageSize = maxInboundMessageSize.unwrap,
      maxRatePerParticipant = maxRatePerParticipant.unwrap,
      uniqueContractKeys = uniqueContractKeys,
      requiredSigningKeySchemes = requiredSigningKeySchemes.toSortedSet.toSeq.map(_.toProtoEnum),
      requiredEncryptionKeySchemes =
        requiredEncryptionKeySchemes.toSortedSet.toSeq.map(_.toProtoEnum),
      requiredSymmetricKeySchemes =
        requiredSymmetricKeySchemes.toSortedSet.toSeq.map(_.toProtoEnum),
      requiredHashAlgorithms = requiredHashAlgorithms.toSortedSet.toSeq.map(_.toProtoEnum),
      requiredCryptoKeyFormats = requiredCryptoKeyFormats.toSortedSet.toSeq.map(_.toProtoEnum),
      protocolVersion = protocolVersion.fullVersion,
    )
}

object StaticDomainParameters
    extends HasVersionedWrapperCompanion[VersionedStaticDomainParameters, StaticDomainParameters] {
  override protected def ProtoClassCompanion: VersionedStaticDomainParameters.type =
    VersionedStaticDomainParameters
  override protected def name: String = "static domain parameters"

  /*
   Set of default values used for configuration and tests
   Values should be synced with the CCF ones:
    enterprise/domain/src/main/cpp/canton/domain/canton_domain_parameters.hpp
   */
  val defaultMaxRatePerParticipant: NonNegativeInt =
    NonNegativeInt.tryCreate(1000000) // yeah, sure.
  val defaultMaxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(10 * 1024 * 1024)
  val defaultReconciliationInterval: PositiveSeconds = PositiveSeconds(
    Duration.ofSeconds(60)
  )

  override def fromProtoVersioned(
      domainParametersP: VersionedStaticDomainParameters
  ): ParsingResult[StaticDomainParameters] =
    domainParametersP.version match {
      case VersionedStaticDomainParameters.Version.Empty =>
        Left(FieldNotSet("VersionedStaticDomainParameters.version"))
      case VersionedStaticDomainParameters.Version.V0(parameters) => fromProtoV0(parameters)
    }

  def fromProtoV0(
      domainParametersP: protoV0.StaticDomainParameters
  ): ParsingResult[StaticDomainParameters] = {
    def requiredKeySchemes[P, A: Order](
        field: String,
        content: Seq[P],
        parse: (String, P) => ParsingResult[A],
    ): ParsingResult[NonEmptySet[A]] =
      for {
        contentList <- NonEmptyList
          .fromList(content.toList)
          .toRight(ProtoDeserializationError.OtherError(s"Sequence $field not set or empty"))
        parsed <- contentList.traverse(parse(field, _))
      } yield parsed.toNes

    val protoV0.StaticDomainParameters(
      reconciliationIntervalP,
      maxRatePerParticipantP,
      maxInboundMessageSizeP,
      uniqueContractKeys,
      requiredSigningKeySchemesP,
      requiredEncryptionKeySchemesP,
      requiredSymmetricKeySchemesP,
      requiredHashAlgorithmsP,
      requiredCryptoKeyFormatsP,
      protocolVersionP,
    ) = domainParametersP

    for {
      reconciliationInterval <- PositiveSeconds.fromProtoPrimitiveO("reconciliationInterval")(
        reconciliationIntervalP
      )
      maxRatePerParticipant <- NonNegativeInt.create(maxRatePerParticipantP)
      maxInboundMessageSize <- NonNegativeInt.create(maxInboundMessageSizeP)

      requiredSigningKeySchemes <- requiredKeySchemes(
        "requiredSigningKeySchemes",
        requiredSigningKeySchemesP,
        SigningKeyScheme.fromProtoEnum,
      )
      requiredEncryptionKeySchemes <- requiredKeySchemes(
        "requiredEncryptionKeySchemes",
        requiredEncryptionKeySchemesP,
        EncryptionKeyScheme.fromProtoEnum,
      )
      requiredSymmetricKeySchemes <- requiredKeySchemes(
        "requiredSymmetricKeySchemes",
        requiredSymmetricKeySchemesP,
        SymmetricKeyScheme.fromProtoEnum,
      )
      requiredHashAlgorithms <- requiredKeySchemes(
        "requiredHashAlgorithms",
        requiredHashAlgorithmsP,
        HashAlgorithm.fromProtoEnum,
      )
      requiredCryptoKeyFormats <- requiredKeySchemes(
        "requiredCryptoKeyFormats",
        requiredCryptoKeyFormatsP,
        CryptoKeyFormat.fromProtoEnum,
      )
      protocolVersion <- ProtocolVersion
        .create(protocolVersionP)
        .leftMap(err => ProtoDeserializationError.OtherError(err))
    } yield StaticDomainParameters(
      reconciliationInterval,
      maxRatePerParticipant,
      maxInboundMessageSize,
      uniqueContractKeys,
      requiredSigningKeySchemes,
      requiredEncryptionKeySchemes,
      requiredSymmetricKeySchemes,
      requiredHashAlgorithms,
      requiredCryptoKeyFormats,
      protocolVersion,
    )
  }
}

final case class DynamicDomainParameters(
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
) extends HasVersionedWrapper[VersionedDynamicDomainParameters]
    with HasProtoV0[protoV0.DynamicDomainParameters] {

  /** Computes the decision time for the given activeness time.
    *
    * Right inverse to [[activenessTimeForDecisionTime]].
    */
  def decisionTimeFor(activenessTime: CantonTimestamp): CantonTimestamp =
    activenessTime.add(participantResponseTimeout.unwrap).add(mediatorReactionTimeout.unwrap)

  /** Left inverse to [[decisionTimeFor]]. Gives the minimum timestamp value if the activeness time would be below this
    * value.
    */
  def activenessTimeForDecisionTime(decisionTime: CantonTimestamp): CantonTimestamp = {
    val activenessInstant =
      decisionTime.toInstant
        .minus(participantResponseTimeout.unwrap)
        .minus(mediatorReactionTimeout.unwrap)
    // The activenessInstant may have become smaller than a CantonTimestamp can represent
    CantonTimestamp.fromInstant(activenessInstant).getOrElse(CantonTimestamp.MinValue)
  }

  def transferExclusivityLimitFor(baseline: CantonTimestamp): CantonTimestamp =
    baseline.add(transferExclusivityTimeout.unwrap)

  def participantResponseDeadlineFor(timestamp: CantonTimestamp): CantonTimestamp =
    timestamp.add(participantResponseTimeout.unwrap)

  /** In some situations, the sequencer signs transaction with slightly outdated keys.
    * This is to allow recipients to verify sequencer signatures when the sequencer keys have been rolled over and
    * they have not yet received the new keys.
    * This parameter determines how much outdated a signing key can be.
    * Choose a higher value to avoid that the sequencer refuses to sign and send messages.
    * Choose a lower value to reduce the latency of sequencer key rollovers.
    * The sequencer signing tolerance must be at least `participantResponseTimeout + mediatorReactionTimeout`.
    */
  def sequencerSigningTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration(
      participantResponseTimeout.unwrap.plus(mediatorReactionTimeout.unwrap).multipliedBy(2)
    )

  def automaticTransferInEnabled: Boolean =
    transferExclusivityTimeout > NonNegativeFiniteDuration.Zero

  override def toProtoVersioned(version: ProtocolVersion): VersionedDynamicDomainParameters =
    VersionedDynamicDomainParameters(VersionedDynamicDomainParameters.Version.V0(toProtoV0))

  override def toProtoV0: protoV0.DynamicDomainParameters =
    protoV0.DynamicDomainParameters(
      participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
      mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
      transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
      topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
      ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
    )
}

object DynamicDomainParameters
    extends HasVersionedWrapperCompanion[
      VersionedDynamicDomainParameters,
      DynamicDomainParameters,
    ] {
  override protected def ProtoClassCompanion: VersionedDynamicDomainParameters.type =
    VersionedDynamicDomainParameters
  override protected def name: String = "dynamic domain parameters"

  final case class WithValidity(
      validFrom: CantonTimestamp,
      validUntil: Option[CantonTimestamp],
      parameters: DynamicDomainParameters,
  ) {
    def isValidAt(ts: CantonTimestamp) = validFrom < ts && validUntil.forall(ts <= _)
  }

  /*
   Set of default values used for configuration and tests
   Values should be synced with the CCF ones:
    enterprise/domain/src/main/cpp/canton/domain/canton_domain_parameters.hpp
   */
  val defaultParticipantResponseTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    Duration.ofSeconds(10)
  )
  val defaultMediatorReactionTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    Duration.ofSeconds(10)
  )
  val defaultTransferExclusivityTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    Duration.ofSeconds(60)
  )
  val defaultTopologyChangeDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(250)
  val defaultLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    Duration.ofSeconds(60)
  )

  // if there is no topology change delay defined (or not yet propagated), we'll use this one
  val topologyChangeDelayIfAbsent: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero

  override def fromProtoVersioned(
      domainParametersP: VersionedDynamicDomainParameters
  ): ParsingResult[DynamicDomainParameters] =
    domainParametersP.version match {
      case VersionedDynamicDomainParameters.Version.Empty =>
        Left(FieldNotSet("VersionedDynamicDomainParameters.version"))
      case VersionedDynamicDomainParameters.Version.V0(parameters) => fromProtoV0(parameters)
    }

  def fromProtoV0(
      domainParametersP: protoV0.DynamicDomainParameters
  ): ParsingResult[DynamicDomainParameters] = {
    def requiredNonNegativeDuration(
        field: String,
        content: Option[com.google.protobuf.duration.Duration],
    ): ParsingResult[NonNegativeFiniteDuration] =
      ProtoConverter
        .required(field, content)
        .flatMap(NonNegativeFiniteDuration.fromProtoPrimitive(field))

    val protoV0.DynamicDomainParameters(
      participantResponseTimeoutP,
      mediatorReactionTimeoutP,
      transferExclusivityTimeoutP,
      topologyChangeDelayP,
      ledgerTimeRecordTimeToleranceP,
    ) = domainParametersP

    for {
      participantResponseTimeout <- requiredNonNegativeDuration(
        "participantResponseTimeout",
        participantResponseTimeoutP,
      )
      mediatorReactionTimeout <- requiredNonNegativeDuration(
        "mediatorReactionTimeout",
        mediatorReactionTimeoutP,
      )
      transferExclusivityTimeout <- requiredNonNegativeDuration(
        "transferExclusivityTimeout",
        transferExclusivityTimeoutP,
      )
      topologyChangeDelay <- requiredNonNegativeDuration(
        "topologyChangeDelay",
        topologyChangeDelayP,
      )
      ledgerTimeRecordTimeTolerance <- requiredNonNegativeDuration(
        "ledgerTimeRecordTimeTolerance",
        ledgerTimeRecordTimeToleranceP,
      )
    } yield DynamicDomainParameters(
      participantResponseTimeout = participantResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      transferExclusivityTimeout = transferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    )
  }
}
