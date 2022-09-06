// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.either._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DynamicDomainParameters.InvalidDomainParameters
import com.digitalasset.canton.protocol.{v0 => protoV0, v1 => protoV1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration,
  PositiveSeconds,
  RemoteClock,
  SimClock,
}
import com.digitalasset.canton.version._
import com.digitalasset.canton.{ProtoDeserializationError, checked}

import scala.Ordering.Implicits._

object DomainParameters {

  /** This class is used to represent domain parameter(s) that can come from static
    * domain parameters or dynamic ones, depending on the protocol version.
    * @param validFrom If the parameter comes from dynamic parameters, exclusive
    *                  timestamp coming from the topology transaction, otherwise, CantonTimestamp.MinValue
    * @param validUntil If the parameter comes from dynamic parameters, timestamp
    *                   coming from the topology transaction, otherwise None
    */
  final case class WithValidity[+P](
      validFrom: CantonTimestamp,
      validUntil: Option[CantonTimestamp],
      parameter: P,
  ) {
    def map[T](f: P => T): WithValidity[T] = WithValidity(validFrom, validUntil, f(parameter))
    def isValidAt(ts: CantonTimestamp) = validFrom < ts && validUntil.forall(ts <= _)
  }
}

sealed abstract case class StaticDomainParameters(
    reconciliationInterval: PositiveSeconds, // TODO(#9800) Mark as deprecated, optional, indicate that it should be used only for V0
    maxRatePerParticipant: NonNegativeInt, //  TODO(#9800) Mark as deprecated, optional, indicate that it should be used only for V0
    maxInboundMessageSize: NonNegativeInt,
    uniqueContractKeys: Boolean,
    requiredSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]],
    requiredEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]],
    requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
    requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
    requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
    protocolVersion: ProtocolVersion,
)(val representativeProtocolVersion: RepresentativeProtocolVersion[StaticDomainParameters])
    extends HasProtocolVersionedWrapper[StaticDomainParameters] {

  val companionObj = StaticDomainParameters

  /** Compute the max size limit for the sum of the envelope payloads of a batch
    *
    * taking 0.9 of the max inbound size, assuming that this will be enough to accommodate any metadata
    * and the list of recipients.
    */
  def maxBatchMessageSize: NonNegativeInt =
    NonNegativeInt.tryCreate((0.9 * maxInboundMessageSize.unwrap).toInt)

  def toProtoV0: protoV0.StaticDomainParameters =
    protoV0.StaticDomainParameters(
      reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
      maxInboundMessageSize = maxInboundMessageSize.unwrap,
      maxRatePerParticipant = maxRatePerParticipant.unwrap,
      uniqueContractKeys = uniqueContractKeys,
      requiredSigningKeySchemes = requiredSigningKeySchemes.toSeq.map(_.toProtoEnum),
      requiredEncryptionKeySchemes = requiredEncryptionKeySchemes.toSeq.map(_.toProtoEnum),
      requiredSymmetricKeySchemes = requiredSymmetricKeySchemes.toSeq.map(_.toProtoEnum),
      requiredHashAlgorithms = requiredHashAlgorithms.toSeq.map(_.toProtoEnum),
      requiredCryptoKeyFormats = requiredCryptoKeyFormats.toSeq.map(_.toProtoEnum),
      protocolVersion = protocolVersion.toProtoPrimitiveS,
    )

  def toProtoV1: protoV1.StaticDomainParameters =
    protoV1.StaticDomainParameters(
      maxInboundMessageSize = maxInboundMessageSize.unwrap,
      uniqueContractKeys = uniqueContractKeys,
      requiredSigningKeySchemes = requiredSigningKeySchemes.toSeq.map(_.toProtoEnum),
      requiredEncryptionKeySchemes = requiredEncryptionKeySchemes.toSeq.map(_.toProtoEnum),
      requiredSymmetricKeySchemes = requiredSymmetricKeySchemes.toSeq.map(_.toProtoEnum),
      requiredHashAlgorithms = requiredHashAlgorithms.toSeq.map(_.toProtoEnum),
      requiredCryptoKeyFormats = requiredCryptoKeyFormats.toSeq.map(_.toProtoEnum),
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}

object StaticDomainParameters
    extends HasProtocolVersionedCompanion[StaticDomainParameters]
    with ProtocolVersionedCompanionDbHelpers[StaticDomainParameters] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(protoV0.StaticDomainParameters)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    // TODO(#9800) Move to stable protocol version
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.dev,
      supportedProtoVersion(protoV1.StaticDomainParameters)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  override protected def name: String = "static domain parameters"

  /*
   Set of default values used for configuration and tests
   Values should be synced with the CCF ones:
    enterprise/domain/src/main/cpp/canton/domain/canton_domain_parameters.hpp
   */
  val defaultMaxRatePerParticipant: NonNegativeInt =
    NonNegativeInt.tryCreate(1000000) // yeah, sure.
  val defaultMaxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(10 * 1024 * 1024)
  val defaultReconciliationInterval: PositiveSeconds = PositiveSeconds.ofSeconds(60)

  def create(
      maxInboundMessageSize: NonNegativeInt,
      uniqueContractKeys: Boolean,
      requiredSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]],
      requiredEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]],
      requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
      requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
      requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
      protocolVersion: ProtocolVersion,
      reconciliationInterval: PositiveSeconds =
        StaticDomainParameters.defaultReconciliationInterval,
      maxRatePerParticipant: NonNegativeInt = StaticDomainParameters.defaultMaxRatePerParticipant,
  ) = new StaticDomainParameters(
    reconciliationInterval = reconciliationInterval,
    maxRatePerParticipant = maxRatePerParticipant,
    maxInboundMessageSize = maxInboundMessageSize,
    uniqueContractKeys = uniqueContractKeys,
    requiredSigningKeySchemes = requiredSigningKeySchemes,
    requiredEncryptionKeySchemes = requiredEncryptionKeySchemes,
    requiredSymmetricKeySchemes = requiredSymmetricKeySchemes,
    requiredHashAlgorithms = requiredHashAlgorithms,
    requiredCryptoKeyFormats = requiredCryptoKeyFormats,
    protocolVersion = protocolVersion,
  )(protocolVersionRepresentativeFor(protocolVersion)) {}

  private def requiredKeySchemes[P, A: Order](
      field: String,
      content: Seq[P],
      parse: (String, P) => ParsingResult[A],
  ): ParsingResult[NonEmpty[Set[A]]] =
    ProtoConverter.pareRequiredNonEmpty(parse(field, _), field, content).map(_.toSet)

  def fromProtoV0(
      domainParametersP: protoV0.StaticDomainParameters
  ): ParsingResult[StaticDomainParameters] = {

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
    } yield new StaticDomainParameters(
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
    )(protocolVersionRepresentativeFor(ProtobufVersion(0))) {}
  }

  def fromProtoV1(
      domainParametersP: protoV1.StaticDomainParameters
  ): ParsingResult[StaticDomainParameters] = {
    val protoV1.StaticDomainParameters(
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
      protocolVersion = ProtocolVersion(protocolVersionP)
    } yield new StaticDomainParameters(
      StaticDomainParameters.defaultReconciliationInterval,
      StaticDomainParameters.defaultMaxRatePerParticipant,
      maxInboundMessageSize,
      uniqueContractKeys,
      requiredSigningKeySchemes,
      requiredEncryptionKeySchemes,
      requiredSymmetricKeySchemes,
      requiredHashAlgorithms,
      requiredCryptoKeyFormats,
      protocolVersion,
    )(protocolVersionRepresentativeFor(ProtobufVersion(1))) {}
  }
}

/** @param participantResponseTimeout the amount of time (w.r.t. the sequencer clock) that a participant may take
  *                                   to validate a command and send a response.
  *                                   Once the timeout has elapsed for a request,
  *                                   the mediator will discard all responses for that request.
  *                                   Choose a lower value to reduce the time to reject a command in case one of the
  *                                   involved participants has high load / operational problems.
  *                                   Choose a higher value to reduce the likelihood of commands being rejected
  *                                   due to timeouts.
  * @param mediatorReactionTimeout the maximum amount of time (w.r.t. the sequencer clock) that the mediator may take
  *                                to validate the responses for a request and broadcast the result message.
  *                                The mediator reaction timeout starts when the confirmation response timeout has elapsed.
  *                                If the mediator does not send a result message within that timeout,
  *                                participants must rollback the transaction underlying the request.
  *                                Chooses a lower value to reduce the time to learn whether a command
  *                                has been accepted.
  *                                Choose a higher value to reduce the likelihood of commands being rejected
  *                                due to timeouts.
  * @param transferExclusivityTimeout this timeout affects who can initiate a transfer-in.
  *                                   Before the timeout, only the submitter of the transfer-out can initiate the
  *                                   corresponding transfer-in.
  *                                   From the timeout onwards, every stakeholder of the contract can initiate a transfer-in,
  *                                   if it has not yet happened.
  *                                   Moreover, if this timeout is zero, no automatic transfer-ins will occur.
  *                                   Choose a low value, if you want to lower the time that contracts can be inactive
  *                                   due to ongoing transfers.
  *                                   TODO(andreas): Choosing a high value currently has no practical benefit, but
  *                                   will have benefits in a future version.
  * @param topologyChangeDelay determines the offset applied to the topology transactions before they become active,
  *                            in order to support parallel transaction processing
  * @param ledgerTimeRecordTimeTolerance the maximum absolute difference between the ledger time and the
  *                                      record time of a command.
  *                                      If the absolute difference would be larger for a command,
  *                                      then the command must be rejected.
  * @param mediatorDeduplicationTimeout the time for how long a request will be stored at the mediator for deduplication
  *                                     purposes. This must be at least twice the `ledgerTimeRecordTimeTolerance`.
  *                                     It is fine to choose the minimal value, unless you plan to subsequently
  *                                     increase `ledgerTimeRecordTimeTolerance.`
  * @param reconciliationInterval The size of the reconciliation interval (minimum duration between two ACS commitments).
  *                               Note: default to [[StaticDomainParameters.defaultReconciliationInterval]] for backward
  *                               compatibility.
  * @param maxRatePerParticipant maximum number of messages sent per participant per second
  * @throws DynamicDomainParameters$.InvalidDomainParameters
  *   if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
  */
sealed abstract case class DynamicDomainParameters(
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveSeconds,
    maxRatePerParticipant: NonNegativeInt,
)(val representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters])
    extends HasProtocolVersionedWrapper[DynamicDomainParameters]
    with PrettyPrinting {

  val companionObj = DynamicDomainParameters

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.jtqcu52qpf82
  if (ledgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2) > mediatorDeduplicationTimeout)
    throw new InvalidDomainParameters(
      s"The ledgerTimeRecordTimeTolerance ($ledgerTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

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

  def tryUpdate(
      participantResponseTimeout: NonNegativeFiniteDuration = participantResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration = mediatorDeduplicationTimeout,
      reconciliationInterval: PositiveSeconds = reconciliationInterval,
      maxRatePerParticipant: NonNegativeInt = maxRatePerParticipant,
  ): DynamicDomainParameters = DynamicDomainParameters.tryCreate(
    participantResponseTimeout = participantResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
    reconciliationInterval = reconciliationInterval,
    maxRatePerParticipant = maxRatePerParticipant,
  )(representativeProtocolVersion)

  def toProtoV0: protoV0.DynamicDomainParameters =
    protoV0.DynamicDomainParameters(
      participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
      mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
      transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
      topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
      ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
    )

  def toProtoV1: protoV1.DynamicDomainParameters =
    protoV1.DynamicDomainParameters(
      participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
      mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
      transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
      topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
      ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
      mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
      reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
      maxRatePerParticipant = maxRatePerParticipant.unwrap,
    )

  override def pretty: Pretty[DynamicDomainParameters] = {
    // TODO(#9800) Change reference to dev, remove preview
    if (representativeProtocolVersion.representative < ProtocolVersion.dev) {
      prettyOfClass(
        param("participant response timeout", _.participantResponseTimeout),
        param("mediator reaction timeout", _.mediatorReactionTimeout),
        param("transfer exclusivity timeout", _.transferExclusivityTimeout),
        param("topology change delay", _.topologyChangeDelay),
        param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
      )
    } else {
      prettyOfClass(
        param("participant response timeout", _.participantResponseTimeout),
        param("mediator reaction timeout", _.mediatorReactionTimeout),
        param("transfer exclusivity timeout", _.transferExclusivityTimeout),
        param("topology change delay", _.topologyChangeDelay),
        param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
        param("reconciliation interval", _.reconciliationInterval),
        param("max rate per participant", _.maxRatePerParticipant),
      )
    }
  }
}

object DynamicDomainParameters extends HasProtocolVersionedCompanion[DynamicDomainParameters] {

  /** Safely creates DynamicDomainParameters.
    * @return `Left(...)` if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
    */
  private def create(
      participantResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      transferExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      maxRatePerParticipant: NonNegativeInt,
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters]
  ): Either[InvalidDomainParameters, DynamicDomainParameters] =
    Either.catchOnly[InvalidDomainParameters](
      tryCreate(
        participantResponseTimeout,
        mediatorReactionTimeout,
        transferExclusivityTimeout,
        topologyChangeDelay,
        ledgerTimeRecordTimeTolerance,
        mediatorDeduplicationTimeout,
        reconciliationInterval,
        maxRatePerParticipant,
      )(representativeProtocolVersion)
    )

  /** Creates DynamicDomainParameters
    * @throws InvalidDomainParameters if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
    */
  def tryCreate(
      participantResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      transferExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      maxRatePerParticipant: NonNegativeInt,
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters]
  ): DynamicDomainParameters = new DynamicDomainParameters(
    participantResponseTimeout,
    mediatorReactionTimeout,
    transferExclusivityTimeout,
    topologyChangeDelay,
    ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout,
    reconciliationInterval,
    maxRatePerParticipant,
  )(representativeProtocolVersion) {}

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(protoV0.DynamicDomainParameters)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    // TODO(#9800) Move to stable protocol version
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.dev,
      supportedProtoVersion(protoV1.DynamicDomainParameters)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  override protected def name: String = "dynamic domain parameters"

  /*
   Set of default values used for configuration and tests
   Values should be synced with the CCF ones:
    enterprise/domain/src/main/cpp/canton/domain/canton_domain_parameters.hpp
   */
  private val defaultParticipantResponseTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(30)
  private val defaultMediatorReactionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(30)

  private val defaultTransferExclusivityTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(60)

  private val defaultTopologyChangeDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(250)
  private val defaultTopologyChangeDelayNonStandardClock: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.Zero // SimClock, RemoteClock

  private val defaultLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(60)

  private val defaultMediatorDeduplicationTimeout: NonNegativeFiniteDuration =
    defaultLedgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2)

  /** Default dynamic domain parameters for non-static clocks */
  def defaultValues(protocolVersion: ProtocolVersion): DynamicDomainParameters =
    initialValues(defaultTopologyChangeDelay, protocolVersion)

  def initialValues(
      topologyChangeDelay: NonNegativeFiniteDuration,
      protocolVersion: ProtocolVersion,
      maxRatePerParticipant: NonNegativeInt = StaticDomainParameters.defaultMaxRatePerParticipant,
  ) = checked( // safe because default values are safe
    DynamicDomainParameters.tryCreate(
      participantResponseTimeout = defaultParticipantResponseTimeout,
      mediatorReactionTimeout = defaultMediatorReactionTimeout,
      transferExclusivityTimeout = defaultTransferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = StaticDomainParameters.defaultReconciliationInterval,
      maxRatePerParticipant = maxRatePerParticipant,
    )(
      protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  def initialValues(clock: Clock, protocolVersion: ProtocolVersion): DynamicDomainParameters = {
    val topologyChangeDelay = clock match {
      case _: RemoteClock | _: SimClock => defaultTopologyChangeDelayNonStandardClock
      case _ => defaultTopologyChangeDelay
    }
    initialValues(topologyChangeDelay, protocolVersion)
  }

  // if there is no topology change delay defined (or not yet propagated), we'll use this one
  val topologyChangeDelayIfAbsent: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero

  def fromProtoV0(
      domainParametersP: protoV0.DynamicDomainParameters
  ): ParsingResult[DynamicDomainParameters] = {
    val protoV0.DynamicDomainParameters(
      participantResponseTimeoutP,
      mediatorReactionTimeoutP,
      transferExclusivityTimeoutP,
      topologyChangeDelayP,
      ledgerTimeRecordTimeToleranceP,
    ) = domainParametersP

    for {
      participantResponseTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "participantResponseTimeout"
      )(
        participantResponseTimeoutP
      )
      mediatorReactionTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "mediatorReactionTimeout"
      )(
        mediatorReactionTimeoutP
      )
      transferExclusivityTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "transferExclusivityTimeout"
      )(
        transferExclusivityTimeoutP
      )
      topologyChangeDelay <- NonNegativeFiniteDuration.fromProtoPrimitiveO("topologyChangeDelay")(
        topologyChangeDelayP
      )
      ledgerTimeRecordTimeTolerance <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "ledgerTimeRecordTimeTolerance"
      )(
        ledgerTimeRecordTimeToleranceP
      )
    } yield checked( // safe because value for mediatorDeduplicationTimeout is safe
      DynamicDomainParameters.tryCreate(
        participantResponseTimeout = participantResponseTimeout,
        mediatorReactionTimeout = mediatorReactionTimeout,
        transferExclusivityTimeout = transferExclusivityTimeout,
        topologyChangeDelay = topologyChangeDelay,
        ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
        reconciliationInterval = StaticDomainParameters.defaultReconciliationInterval,
        mediatorDeduplicationTimeout = ledgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2),
        maxRatePerParticipant = StaticDomainParameters.defaultMaxRatePerParticipant,
      )(protocolVersionRepresentativeFor(ProtobufVersion(0)))
    )
  }

  def fromProtoV1(
      domainParametersP: protoV1.DynamicDomainParameters
  ): ParsingResult[DynamicDomainParameters] = {
    val protoV1.DynamicDomainParameters(
      participantResponseTimeoutP,
      mediatorReactionTimeoutP,
      transferExclusivityTimeoutP,
      topologyChangeDelayP,
      ledgerTimeRecordTimeToleranceP,
      reconciliationIntervalP,
      mediatorDeduplicationTimeoutP,
      maxRatePerParticipantP,
    ) = domainParametersP

    for {
      reconciliationInterval <- PositiveSeconds.fromProtoPrimitiveO(
        "reconciliationInterval"
      )(
        reconciliationIntervalP
      )
      participantResponseTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "participantResponseTimeout"
      )(
        participantResponseTimeoutP
      )
      mediatorReactionTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "mediatorReactionTimeout"
      )(
        mediatorReactionTimeoutP
      )
      transferExclusivityTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "transferExclusivityTimeout"
      )(
        transferExclusivityTimeoutP
      )
      topologyChangeDelay <- NonNegativeFiniteDuration.fromProtoPrimitiveO("topologyChangeDelay")(
        topologyChangeDelayP
      )
      ledgerTimeRecordTimeTolerance <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "ledgerTimeRecordTimeTolerance"
      )(
        ledgerTimeRecordTimeToleranceP
      )
      mediatorDeduplicationTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "mediatorDeduplicationTimeout"
      )(
        mediatorDeduplicationTimeoutP
      )
      maxRatePerParticipant <- NonNegativeInt.create(maxRatePerParticipantP)
      domainParameters <-
        create(
          participantResponseTimeout = participantResponseTimeout,
          mediatorReactionTimeout = mediatorReactionTimeout,
          transferExclusivityTimeout = transferExclusivityTimeout,
          topologyChangeDelay = topologyChangeDelay,
          ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
          mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
          reconciliationInterval = reconciliationInterval,
          maxRatePerParticipant = maxRatePerParticipant,
        )(protocolVersionRepresentativeFor(ProtobufVersion(1)))
          .leftMap(_.toProtoDeserializationError)
    } yield domainParameters
  }

  class InvalidDomainParameters(message: String) extends RuntimeException(message) {
    lazy val toProtoDeserializationError: ProtoDeserializationError.InvariantViolation =
      ProtoDeserializationError.InvariantViolation(message)
  }
}
