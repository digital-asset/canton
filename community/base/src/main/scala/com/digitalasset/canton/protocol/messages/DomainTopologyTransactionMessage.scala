// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.protocol.messages.AcceptedTopologyTransactionsX.AcceptedRequest
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v2.EnvelopeContent
import com.digitalasset.canton.protocol.{v0, v1, v2}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final case class DomainTopologyTransactionMessage private (
    domainTopologyManagerSignature: Signature,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      DomainTopologyTransactionMessage.type
    ]
) extends UnsignedProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with UnsignedProtocolMessageV2 {
  def hashToSign(hashOps: HashOps): Hash =
    DomainTopologyTransactionMessage.hash(transactions, domainId, hashOps)

  def toProtoV0: v0.DomainTopologyTransactionMessage = {
    v0.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV0),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
    )
  }

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)
    )

  override def toProtoSomeEnvelopeContentV2: v2.EnvelopeContent.SomeEnvelopeContent =
    v2.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)

  @transient override protected lazy val companionObj: DomainTopologyTransactionMessage.type =
    DomainTopologyTransactionMessage
}

object DomainTopologyTransactionMessage
    extends HasProtocolVersionedCompanion[DomainTopologyTransactionMessage] {

  implicit val domainIdentityTransactionMessageCast
      : ProtocolMessageContentCast[DomainTopologyTransactionMessage] =
    ProtocolMessageContentCast.create[DomainTopologyTransactionMessage](
      "DomainTopologyTransactionMessage"
    ) {
      case ditm: DomainTopologyTransactionMessage => Some(ditm)
      case _ => None
    }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.DomainTopologyTransactionMessage
    )(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

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
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, DomainTopologyTransactionMessage] = {
    val hashToSign = hash(transactions, domainId, syncCrypto.crypto.pureCrypto)
    syncCrypto
      .sign(hashToSign)
      .map(signature =>
        DomainTopologyTransactionMessage(
          signature,
          transactions,
          domainId,
        )(protocolVersionRepresentativeFor(protocolVersion))
      )
  }

  def tryCreate(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      crypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[DomainTopologyTransactionMessage] =
    create(transactions, crypto, domainId, protocolVersion).fold(
      err =>
        throw new IllegalStateException(
          s"Failed to create domain topology transaction message: $err"
        ),
      identity,
    )

  def fromProtoV0(
      message: v0.DomainTopologyTransactionMessage
  ): ParsingResult[DomainTopologyTransactionMessage] = {

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
    } yield DomainTopologyTransactionMessage(
      signature,
      succeededContent,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
  }

  override protected def name: String = "DomainTopologyTransactionMessage"
}

final case class AcceptedTopologyTransactionsX private (
    override val domainId: DomainId,
    accepted: Seq[AcceptedRequest],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcceptedTopologyTransactionsX.type
    ]
) extends UnsignedProtocolMessage
    // TODO(#11255) make me a SignedProtocolMessageContent
    with UnsignedProtocolMessageV2 {

  @transient override protected lazy val companionObj: AcceptedTopologyTransactionsX.type =
    AcceptedTopologyTransactionsX

  override protected[messages] def toProtoSomeEnvelopeContentV2
      : EnvelopeContent.SomeEnvelopeContent =
    v2.EnvelopeContent.SomeEnvelopeContent.AcceptedTopologyTransactions(toProtoV2)

  def toProtoV2: v2.AcceptedTopologyTransactionsX = v2.AcceptedTopologyTransactionsX(
    domainId.toProtoPrimitive,
    accepted = accepted.map(_.toProtoV2),
  )

}

object AcceptedTopologyTransactionsX
    extends HasProtocolVersionedCompanion[
      AcceptedTopologyTransactionsX
    ] {

  def create(
      domainId: DomainId,
      accepted: Seq[AcceptedRequest],
      protocolVersion: ProtocolVersion,
  ): AcceptedTopologyTransactionsX =
    AcceptedTopologyTransactionsX(domainId = domainId, accepted = accepted)(
      supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)
    )

  override protected def name: String = "AcceptedTopologyTransactionsX"

  implicit val acceptedTopologyTransactionXMessageCast
      : ProtocolMessageContentCast[AcceptedTopologyTransactionsX] =
    ProtocolMessageContentCast.create[AcceptedTopologyTransactionsX](
      name
    ) {
      case att: AcceptedTopologyTransactionsX => Some(att)
      case _ => None
    }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.minimum),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v2.AcceptedTopologyTransactionsX
    )(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  private[messages] def fromProtoV2(
      message: v2.AcceptedTopologyTransactionsX
  ): ParsingResult[AcceptedTopologyTransactionsX] = {
    val v2.AcceptedTopologyTransactionsX(domain, accepted) = message
    for {
      domainId <- DomainId.fromProtoPrimitive(domain, "domain")
      accepted <- accepted.traverse(acceptedRequestFromProtoV2)
    } yield AcceptedTopologyTransactionsX(domainId, accepted.toList)(
      protocolVersionRepresentativeFor(ProtoVersion(2))
    )
  }

  private def acceptedRequestFromProtoV2(
      message: v2.AcceptedTopologyTransactionsX.AcceptedRequest
  ): ParsingResult[AcceptedRequest] = {
    val v2.AcceptedTopologyTransactionsX.AcceptedRequest(requestId, transactions) = message
    for {
      requestId <- String255.fromProtoPrimitive(requestId, "request_id")
      transactions <- transactions.traverse(SignedTopologyTransactionX.fromProtoV2)
    } yield AcceptedRequest(requestId, transactions.toList)
  }

  final case class AcceptedRequest(
      requestId: TopologyRequestId,
      transactions: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  ) {
    def toProtoV2: v2.AcceptedTopologyTransactionsX.AcceptedRequest =
      v2.AcceptedTopologyTransactionsX.AcceptedRequest(
        requestId = requestId.toProtoPrimitive,
        transactions = transactions.map(_.toProtoV2),
      )
  }
}
