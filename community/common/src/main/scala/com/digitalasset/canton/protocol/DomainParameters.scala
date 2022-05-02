// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.either._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{v0 => protoV0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration,
  PositiveSeconds,
  RemoteClock,
  SimClock,
}
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}

import scala.Ordered.orderingToOrdered

final case class StaticDomainParameters(
    reconciliationInterval: PositiveSeconds,
    maxRatePerParticipant: NonNegativeInt,
    maxInboundMessageSize: NonNegativeInt,
    uniqueContractKeys: Boolean,
    requiredSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]],
    requiredEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]],
    requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
    requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
    requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
    protocolVersion: ProtocolVersion,
) extends HasVersionedWrapper[VersionedMessage[StaticDomainParameters]]
    with HasProtoV0[protoV0.StaticDomainParameters] {

  /** Compute the max size limit for the sum of the envelope payloads of a batch
    *
    * taking 0.9 of the max inbound size, assuming that this will be enough to accommodate any metadata
    * and the list of recipients.
    */
  def maxBatchMessageSize: NonNegativeInt =
    NonNegativeInt.tryCreate((0.9 * maxInboundMessageSize.unwrap).toInt)

  override def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[StaticDomainParameters] = VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: protoV0.StaticDomainParameters =
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
      protocolVersion = protocolVersion.fullVersion,
    )
}

object StaticDomainParameters extends HasVersionedMessageCompanion[StaticDomainParameters] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(protoV0.StaticDomainParameters)(fromProtoV0)
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

  def fromProtoV0(
      domainParametersP: protoV0.StaticDomainParameters
  ): ParsingResult[StaticDomainParameters] = {
    def requiredKeySchemes[P, A: Order](
        field: String,
        content: Seq[P],
        parse: (String, P) => ParsingResult[A],
    ): ParsingResult[NonEmpty[Set[A]]] =
      ProtoConverter.pareRequiredNonEmpty(parse(field, _), field, content).map(_.toSet)

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
  *                                   After the timeout, every stakeholder of the contract can initiate a transfer-in,
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
  */
final case class DynamicDomainParameters(
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
) extends HasVersionedWrapper[VersionedMessage[DynamicDomainParameters]]
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

  override def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[DynamicDomainParameters] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: protoV0.DynamicDomainParameters =
    protoV0.DynamicDomainParameters(
      participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
      mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
      transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
      topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
      ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
    )
}

object DynamicDomainParameters extends HasVersionedMessageCompanion[DynamicDomainParameters] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(protoV0.DynamicDomainParameters)(fromProtoV0)
  )

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

  def initialValues(topologyChangeDelay: NonNegativeFiniteDuration) = DynamicDomainParameters(
    participantResponseTimeout = defaultParticipantResponseTimeout,
    mediatorReactionTimeout = defaultMediatorReactionTimeout,
    transferExclusivityTimeout = defaultTransferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
  )

  def initialValues(clock: Clock): DynamicDomainParameters = {
    val topologyChangeDelay = clock match {
      case _: RemoteClock | _: SimClock => defaultTopologyChangeDelayNonStandardClock
      case _ => defaultTopologyChangeDelay
    }

    initialValues(topologyChangeDelay)
  }

  // if there is no topology change delay defined (or not yet propagated), we'll use this one
  val topologyChangeDelayIfAbsent: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero

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
