// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.{
  AuthenticatedMessageType,
  hashForMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

trait CryptoProvider[E <: Env[E]] {

  def psid: PhysicalSynchronizerId

  def signHash(
      hash: Hash,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]]

  def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, SignedMessage[MessageT]]]

  def verifySignature(
      hash: Hash,
      member: BftNodeId,
      signature: Signature,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]]

  def verifySignedMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      signedMessage: SignedMessage[MessageT],
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]] =
    verifySignature(
      hashForMessage(signedMessage.message, signedMessage.from, authenticatedMessageType, psid),
      signedMessage.from,
      signedMessage.signature,
      operationId = s"verify-signature-$authenticatedMessageType",
    )
}

object CryptoProvider {

  sealed trait AuthenticatedMessageType extends Product with PrettyPrintingFromCompanion {
    override final def prettyCompanion: PrettyPrintingCompanion[AuthenticatedMessageType] =
      AuthenticatedMessageType
  }
  object AuthenticatedMessageType extends PrettyPrintingCompanion[AuthenticatedMessageType] {

    override protected val pretty: Pretty[AuthenticatedMessageType] =
      prettyOfObject[AuthenticatedMessageType]

    case object BftOrderingPbftBlock extends AuthenticatedMessageType
    case object BftAvailabilityAck extends AuthenticatedMessageType
    case object BftSignedAvailabilityMessage extends AuthenticatedMessageType
    case object BftSignedConsensusMessage extends AuthenticatedMessageType
    case object BftSignedStateTransferMessage extends AuthenticatedMessageType
    case object BftSignedRetransmissionMessage extends AuthenticatedMessageType
  }

  private val hashPurposeForMessageMapping
      : Map[AuthenticatedMessageType, (HashPurpose, HashPurpose)] =
    Map(
      AuthenticatedMessageType.BftOrderingPbftBlock ->
        (HashPurpose.BftOrderingPbftBlock, HashPurpose.BftOrderingPbftBlockV2),
      AuthenticatedMessageType.BftAvailabilityAck ->
        (HashPurpose.BftAvailabilityAck, HashPurpose.BftSignedAvailabilityAckV2),
      AuthenticatedMessageType.BftSignedAvailabilityMessage ->
        (HashPurpose.BftSignedAvailabilityMessage, HashPurpose.BftSignedAvailabilityMessageV2),
      AuthenticatedMessageType.BftSignedConsensusMessage ->
        (HashPurpose.BftSignedConsensusMessage, HashPurpose.BftSignedConsensusMessageV2),
      AuthenticatedMessageType.BftSignedStateTransferMessage ->
        (HashPurpose.BftSignedStateTransferMessage, HashPurpose.BftSignedStateTransferMessageV2),
      AuthenticatedMessageType.BftSignedRetransmissionMessage ->
        (HashPurpose.BftSignedRetransmissionMessage, HashPurpose.BftSignedRetransmissionMessageV2),
    )

  val BftOrderingSigningKeyUsage: NonEmpty[Set[SigningKeyUsage]] =
    SigningKeyUsage.ProtocolOnly

  def hashForMessage[MessageT <: ProtocolVersionedMemoizedEvidence](
      messageT: MessageT,
      from: BftNodeId,
      authenticatedMessageType: AuthenticatedMessageType,
      psid: PhysicalSynchronizerId,
  ): Hash = {
    val bindPsid = psid.protocolVersion > ProtocolVersion.v37
    val builder = Hash.build(toHashPurposeForMessage(authenticatedMessageType, bindPsid), Sha256)
    if (bindPsid) builder.addString(psid.toProtoPrimitive)
    builder
      .addString(from)
      .addByteString(messageT.getCryptographicEvidence)
      .finish()
  }

  def timeCrypto[T](
      metrics: BftOrderingMetrics,
      call: => FutureUnlessShutdown[T],
      operationId: String,
  )(implicit
      metricsContext: MetricsContext
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(
      metrics.performance.orderingStageLatency
        .timeFuture(call.unwrap)(
          metricsContext.withExtraLabels(
            metrics.performance.orderingStageLatency.labels.stage.Key -> operationId
          )
        )
    )

  private def toHashPurposeForMessage(
      authenticatedMessageType: AuthenticatedMessageType,
      bindPsid: Boolean,
  ): HashPurpose = {
    val (v1, v2) = hashPurposeForMessageMapping(authenticatedMessageType)
    if (bindPsid) v2 else v1
  }
}

final case class DelegationCryptoProvider[E <: Env[E]](
    signer: CryptoProvider[E],
    verifier: CryptoProvider[E],
) extends CryptoProvider[E] {
  override def psid: PhysicalSynchronizerId = verifier.psid
  override def signHash(hash: Hash, operationId: String)(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]] =
    signer.signHash(hash, operationId)

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    signer.signMessage(message, authenticatedMessageType)

  override def verifySignature(
      hash: Hash,
      member: BftNodeId,
      signature: Signature,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]] =
    verifier.verifySignature(hash, member, signature, operationId)
}
