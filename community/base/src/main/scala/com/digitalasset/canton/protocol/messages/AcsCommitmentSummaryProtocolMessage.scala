// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
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
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanionContext,
}

import scala.concurrent.ExecutionContext

final case class AcsCommitmentSummaryProtocolMessage(
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
    v32.EnvelopeContent.SomeEnvelopeContent.AcsCommitmentSummaryProtocolMessage(toProtoV32)

  private def toProtoV32: v32.AcsCommitmentSummaryProtocolMessage =
    v32.AcsCommitmentSummaryProtocolMessage(
      acsCommitmentSummary = acsCommitmentSummary.toByteString,
      signature = signature.toProtoV30.some,
    )
}

object AcsCommitmentSummaryProtocolMessage
    extends VersioningCompanionContext[AcsCommitmentSummaryProtocolMessage, ProtocolVersion] {

  override def name: String = "AcsCommitmentSummaryProtocolMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.AcsCommitmentSummaryProtocolMessage
    )(
      supportedProtoVersion(_)(fromProtoV32),
      _.toProtoV32,
    ),
  )

  private[messages] def fromProtoV32(
      expectedProtocolVersion: ProtocolVersion,
      message: v32.AcsCommitmentSummaryProtocolMessage,
  ): ParsingResult[AcsCommitmentSummaryProtocolMessage] = {
    val v32.AcsCommitmentSummaryProtocolMessage(acsCommitmentSummaryP, signaturesP) = message
    for {
      acsCommitmentSummary <- AcsCommitmentSummary.fromByteString(
        expectedProtocolVersion,
        acsCommitmentSummaryP,
      )
      signatures <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signatures",
        signaturesP,
      )
    } yield AcsCommitmentSummaryProtocolMessage(acsCommitmentSummary, signatures)
  }

  // TODO(#34070) Re-use more code for signing and verifying instead of copy-pasting
  def signAndCreate(
      cryptoApi: SynchronizerCryptoClient,
      acsCommitmentSummary: AcsCommitmentSummary,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, AcsCommitmentSummaryProtocolMessage] = {
    val hashPurpose = HashPurpose.AcsCommitmentSummary
    val serialization = acsCommitmentSummary.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)

    for {
      snapshot <- EitherT.liftF(cryptoApi.awaitSnapshot(acsCommitmentSummary.commitmentTick))
      signature <- snapshot.sign(hash, SigningKeyUsage.ProtocolOnly, None)
    } yield AcsCommitmentSummaryProtocolMessage(acsCommitmentSummary, signature)
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
