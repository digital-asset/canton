// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.option.*
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SigningKeyUsage,
  SyncCryptoApi,
  SyncCryptoError,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v30, v31, v32}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanionContext,
}

import scala.concurrent.ExecutionContext

final case class AcsCommitmentProtocolMessage(
    acsCommitment: AcsCommitment,
    signature: Signature,
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[AcsCommitmentProtocolMessage] {

  @transient override protected lazy val companionObj: AcsCommitmentProtocolMessage.type =
    AcsCommitmentProtocolMessage

  override def psid: PhysicalSynchronizerId = acsCommitment.psid

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    AcsCommitmentProtocolMessage.type
  ] = AcsCommitmentProtocolMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  protected def toProtoV32: v32.AcsCommitmentProtocolMessage =
    v32.AcsCommitmentProtocolMessage(
      acsCommitment = acsCommitment.toByteString,
      signature = signature.toProtoV30.some,
    )

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} cannot be serialized to envelope content v30"
    )

  override protected[messages] def toProtoSomeEnvelopeContentV31
      : v31.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} cannot be serialized to envelope content v31"
    )

  override protected[messages] def toProtoSomeEnvelopeContentV32
      : v32.EnvelopeContent.SomeEnvelopeContent =
    v32.EnvelopeContent.SomeEnvelopeContent.AcsCommitmentProtocolMessage(toProtoV32)
}

object AcsCommitmentProtocolMessage
    extends VersioningCompanionContext[
      AcsCommitmentProtocolMessage,
      ProtocolVersionValidation,
    ] {

  override def name: String = "AcsCommitmentProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.AcsCommitmentProtocolMessage
    )(
      supportedProtoVersion(_)(fromProtoV32),
      _.toProtoV32,
    ),
  )

  private[messages] def fromProtoV32(
      expectedProtocolVersion: ProtocolVersionValidation,
      message: v32.AcsCommitmentProtocolMessage,
  ): ParsingResult[AcsCommitmentProtocolMessage] = {
    val v32.AcsCommitmentProtocolMessage(acsCommitmentP, signaturesP) = message
    for {
      acsCommitment <- AcsCommitment.fromByteString(expectedProtocolVersion, acsCommitmentP)
      signatures <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        signaturesP,
      )
    } yield AcsCommitmentProtocolMessage(acsCommitment, signatures)
  }

  def signAndCreate(
      cryptoApi: SynchronizerCryptoClient,
      acsCommitment: AcsCommitment,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, AcsCommitmentProtocolMessage] = {
    val hashPurpose = HashPurpose.AcsCommitment
    val serialization = acsCommitment.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    for {
      snapshot <- EitherT.liftF(cryptoApi.awaitSnapshot(acsCommitment.period.toInclusive))
      signature <- snapshot.sign(hash, SigningKeyUsage.ProtocolOnly, None)
    } yield AcsCommitmentProtocolMessage(acsCommitment, signature)
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
