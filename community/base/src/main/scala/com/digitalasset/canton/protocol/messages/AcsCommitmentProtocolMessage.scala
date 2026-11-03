// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SigningKeyUsage,
  SyncCryptoApi,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{SynchronizerLimits, v32}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

final case class AcsCommitmentProtocolMessage private (
    acsCommitment: AcsCommitment,
    signature: Signature,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcsCommitmentProtocolMessage.type
    ]
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[AcsCommitmentProtocolMessage] {

  @transient override protected lazy val companionObj: AcsCommitmentProtocolMessage.type =
    AcsCommitmentProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitment.psid

  protected def toProtoV32: v32.AcsCommitmentProtocolMessage =
    v32.AcsCommitmentProtocolMessage(
      acsCommitment = acsCommitment.toByteString,
      signature = signature.toProtoV30.some,
    )

  override protected[messages] def toProtoSomeEnvelopeContentV32
      : v32.EnvelopeContent.SomeEnvelopeContent =
    v32.EnvelopeContent.SomeEnvelopeContent.AcsCommitmentProtocolMessage(toProtoV32)
}

final case class AcsCommitmentDeserializationContext(
    synchronizerLimits: SynchronizerLimits
)
object AcsCommitmentProtocolMessage
    extends VersioningCompanionContext[
      AcsCommitmentProtocolMessage,
      AcsCommitmentDeserializationContext,
    ] {

  override def name: String = "AcsCommitmentProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.AcsCommitmentProtocolMessage
    )(
      supportedProtoVersionPVV(_)(fromProtoV32),
      _.toProtoV32,
    )
  )

  def create(
      acsCommitment: AcsCommitment,
      signature: Signature,
      sizeLimit: PositiveInt,
      protocolVersion: ProtocolVersion,
  ): Either[String, AcsCommitmentProtocolMessage] = {
    val pvv = ProtocolVersionValidation.PV(protocolVersion)

    ProtoValidation
      .validateLength(
        acsCommitment.toByteString,
        "acs_commitment",
        pvv,
        ValidationParam(ProtocolVersion.newBoundsOnPV37 -> sizeLimit),
      )
      .leftMap(_.message)
      .map(_ =>
        new AcsCommitmentProtocolMessage(acsCommitment, signature)(
          AcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(protocolVersion)
        )
      )
  }

  private[messages] def fromProtoV32(
      expectedProtocolVersion: ProtocolVersionValidation,
      context: AcsCommitmentDeserializationContext,
      message: v32.AcsCommitmentProtocolMessage,
  ): ParsingResult[AcsCommitmentProtocolMessage] = {
    val v32.AcsCommitmentProtocolMessage(acsCommitmentP, signatureP) = message

    for {
      acsCommitment <- ProtoValidation.validateLengthThen(
        acsCommitmentP,
        "acs_commitment",
        expectedProtocolVersion,
        ValidationParam(
          ProtocolVersion.newBoundsOnPV37 -> context.synchronizerLimits.acsCommitmentLimits.maxCommitmentSizeBytes
        ),
      )(AcsCommitment.fromByteString(expectedProtocolVersion, _))
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        signatureP,
      )
      rpv <- AcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(ProtoVersion(32))
      // Direct instantiation bypasses `create` limit check because `acsCommitmentP` is already length-validated above
    } yield new AcsCommitmentProtocolMessage(acsCommitment, signature)(rpv)
  }

  def signAndCreate(
      cryptoApi: SynchronizerCryptoClient,
      acsCommitment: AcsCommitment,
      sizeLimit: PositiveInt,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, AcsCommitmentProtocolMessage] = {
    val hashPurpose = HashPurpose.AcsCommitment
    val serialization = acsCommitment.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    for {
      snapshot <- EitherT.liftF(cryptoApi.awaitSnapshot(acsCommitment.period.toInclusive))
      signature <- snapshot.sign(hash, SigningKeyUsage.ProtocolOnly, None).leftMap(_.toString)
      message <- EitherT.fromEither[FutureUnlessShutdown](
        create(acsCommitment, signature, sizeLimit, cryptoApi.psid.protocolVersion)
      )
    } yield message
  }

  @VisibleForTesting
  private[canton] def signImmediatelyAndCreate(
      cryptoApi: SyncCryptoApi,
      acsCommitment: AcsCommitment,
      sizeLimit: PositiveInt,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, AcsCommitmentProtocolMessage] = {
    val hashPurpose = HashPurpose.AcsCommitment
    val serialization = acsCommitment.getCryptographicEvidence
    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    for {
      signature <- cryptoApi.sign(hash, SigningKeyUsage.ProtocolOnly, None).leftMap(_.toString)
      message <- EitherT.fromEither(create(acsCommitment, signature, sizeLimit, protocolVersion))
    } yield message
  }

  def verifySignature(
      snapshot: SyncCryptoApi,
      message: AcsCommitmentProtocolMessage,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val hash = snapshot.pureCrypto.digest(
      HashPurpose.AcsCommitment,
      message.acsCommitment.getCryptographicEvidence,
    )
    for {
      sender <- EitherT.fromEither[FutureUnlessShutdown](
        UniqueIdentifier
          .fromProtoPrimitive(message.acsCommitment.sender, "sender")
          .bimap(_.toString, ParticipantId.apply)
      )
      _ <- snapshot
        .verifySignature(hash, sender, message.signature, SigningKeyUsage.ProtocolOnly)
        .leftMap(_.toString)
    } yield ()
  }

  implicit val acsCommitmentProtocolMessageMessageCast
      : ProtocolMessageContentCast[AcsCommitmentProtocolMessage] =
    ProtocolMessageContentCast.create[AcsCommitmentProtocolMessage](name) {
      case m: AcsCommitmentProtocolMessage => Some(m)
      case _ => None
    }

}
