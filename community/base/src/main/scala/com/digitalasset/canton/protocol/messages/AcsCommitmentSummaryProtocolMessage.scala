// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.either.*
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
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
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

import scala.concurrent.ExecutionContext

final case class AcsCommitmentSummaryProtocolMessage private (
    acsCommitmentSummary: AcsCommitmentSummary,
    signature: Signature,
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[AcsCommitmentSummaryProtocolMessage] {

  @transient override protected lazy val companionObj: AcsCommitmentSummaryProtocolMessage.type =
    AcsCommitmentSummaryProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitmentSummary.psid

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    AcsCommitmentSummaryProtocolMessage.type
  ] = AcsCommitmentSummaryProtocolMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  override protected[messages] def toProtoSomeEnvelopeContentV32
      : v32.EnvelopeContent.SomeEnvelopeContent =
    v32.EnvelopeContent.SomeEnvelopeContent.AcsCommitmentSummaryProtocolMessage(toProtoV32)

  private def toProtoV32: v32.AcsCommitmentSummaryProtocolMessage = {
    val summaryBytes = acsCommitmentSummary.toByteString
    v32.AcsCommitmentSummaryProtocolMessage(
      acsCommitmentSummary = summaryBytes,
      signature = signature.toProtoV30.some,
    )
  }
}

object AcsCommitmentSummaryProtocolMessage
    extends VersioningCompanionContext[
      AcsCommitmentSummaryProtocolMessage,
      AcsCommitmentDeserializationContext,
    ] {

  override def name: String = "AcsCommitmentSummaryProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.AcsCommitmentSummaryProtocolMessage
    )(
      supportedProtoVersionPVV(_)(fromProtoV32),
      _.toProtoV32,
    )
  )

  def create(
      acsCommitmentSummary: AcsCommitmentSummary,
      signature: Signature,
      sizeLimit: PositiveInt,
  ): Either[String, AcsCommitmentSummaryProtocolMessage] = {
    val pvv = ProtocolVersionValidation.PV(acsCommitmentSummary.psid.protocolVersion)

    ProtoValidation
      .validateLength(
        acsCommitmentSummary.toByteString,
        "acs_commitment_summary",
        pvv,
        ValidationParam(ProtocolVersion.nextValidationBoundPV -> sizeLimit),
      )
      .leftMap(_.message)
      .map(_ => new AcsCommitmentSummaryProtocolMessage(acsCommitmentSummary, signature))
  }

  private[messages] def fromProtoV32(
      pvv: ProtocolVersionValidation,
      context: AcsCommitmentDeserializationContext,
      message: v32.AcsCommitmentSummaryProtocolMessage,
  ): ParsingResult[AcsCommitmentSummaryProtocolMessage] = {
    val v32.AcsCommitmentSummaryProtocolMessage(acsCommitmentSummaryP, signatureP) = message

    for {
      acsCommitmentSummary <- ProtoValidation.validateLengthThen(
        acsCommitmentSummaryP,
        "acs_commitment_summary",
        pvv,
        ValidationParam(
          ProtocolVersion.nextValidationBoundPV -> context.synchronizerLimits.acsCommitmentLimits.maxCommitmentSummarySizeBytes
        ),
      )(AcsCommitmentSummary.fromByteString(pvv, context, _))

      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        signatureP,
      )
    } yield new AcsCommitmentSummaryProtocolMessage(acsCommitmentSummary, signature)
  }

  // TODO(#34736) Re-use more code for signing and verifying instead of copy-pasting
  def signAndCreate(
      cryptoApi: SynchronizerCryptoClient,
      acsCommitmentSummary: AcsCommitmentSummary,
      sizeLimit: PositiveInt,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, AcsCommitmentSummaryProtocolMessage] = {
    val hashPurpose = HashPurpose.AcsCommitmentSummary
    val serialization = acsCommitmentSummary.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)

    for {
      snapshot <- EitherT.liftF(cryptoApi.awaitSnapshot(acsCommitmentSummary.commitmentTick))
      signature <- snapshot.sign(hash, SigningKeyUsage.ProtocolOnly, None).leftMap(_.toString)
      protocolMessage <- EitherT.fromEither(
        create(acsCommitmentSummary, signature, sizeLimit)
      )
    } yield protocolMessage
  }

  def verifySignature(
      snapshot: SyncCryptoApi,
      message: AcsCommitmentSummaryProtocolMessage,
      signer: Member,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val hash = snapshot.pureCrypto.digest(
      HashPurpose.AcsCommitmentSummary,
      message.acsCommitmentSummary.getCryptographicEvidence,
    )

    snapshot
      .verifySignature(hash, signer, message.signature, SigningKeyUsage.ProtocolOnly)
      .leftMap(_.toString)
  }

  implicit val acsCommitmentSummaryProtocolMessageMessageCast
      : ProtocolMessageContentCast[AcsCommitmentSummaryProtocolMessage] =
    ProtocolMessageContentCast.create[AcsCommitmentSummaryProtocolMessage](name) {
      case m: AcsCommitmentSummaryProtocolMessage => Some(m)
      case _ => None
    }
}
