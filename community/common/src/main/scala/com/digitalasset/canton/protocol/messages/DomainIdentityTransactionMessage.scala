// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.topology.{DomainId, _}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{HasProtoV0, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

case class DomainTopologyTransactionMessage private (
    domainTopologyManagerSignature: Signature,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    override val domainId: DomainId,
) extends ProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtoV0[v0.DomainTopologyTransactionMessage]
    with NoCopy {
  def hashToSign(hashOps: HashOps): Hash =
    DomainTopologyTransactionMessage.hash(transactions, domainId, hashOps)

  override def toProtoV0: v0.DomainTopologyTransactionMessage = {
    v0.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV0),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
    )
  }

  override def toProtoEnvelopeContentV0(version: ProtocolVersion): v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)
    )

  override def toProtoEnvelopeContentV1(version: ProtocolVersion): v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)
    )
}

object DomainTopologyTransactionMessage {

  private[this] def apply(
      domainTopologyManagerSignature: Signature,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
  ): DomainTopologyTransactionMessage =
    throw new UnsupportedOperationException("Use the public create/tryCreate methods instead")

  implicit val domainIdentityTransactionMessageCast
      : ProtocolMessageContentCast[DomainTopologyTransactionMessage] = {
    case ditm: DomainTopologyTransactionMessage => Some(ditm)
    case _ => None
  }

  private def hash(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
      hashOps: HashOps,
  ): Hash = {
    val builder = hashOps
      .build(HashPurpose.DomainTopologyTransactionMessageSignature)
      .add(domainId.toProtoPrimitive)
    transactions.foreach(elem => builder.add(elem.getCryptographicEvidence))
    builder.finish()
  }

  def create(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      syncCrypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, DomainTopologyTransactionMessage] = {
    val hashToSign = hash(transactions, domainId, syncCrypto.crypto.pureCrypto)
    syncCrypto
      .sign(hashToSign)
      .map(signature => new DomainTopologyTransactionMessage(signature, transactions, domainId))
  }

  def tryCreate(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      crypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[DomainTopologyTransactionMessage] =
    create(transactions, crypto, domainId).fold(
      err =>
        throw new IllegalStateException(
          s"Failed to create domain topology transaction message: $err"
        ),
      identity,
    )

  def fromProtoV0(
      message: v0.DomainTopologyTransactionMessage
  ): ParsingResult[DomainTopologyTransactionMessage] = {
    import cats.syntax.traverse._

    def decodeTransactions(
        payload: List[ByteString]
    ): ParsingResult[List[SignedTopologyTransaction[TopologyChangeOp]]] =
      payload.traverse(SignedTopologyTransaction.fromByteString)

    for {
      succeededContent <- decodeTransactions(message.transactions.toList)
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV0,
        "signature",
        message.signature,
      )
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
    } yield new DomainTopologyTransactionMessage(signature, succeededContent, DomainId(domainUid))
  }
}
