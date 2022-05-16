// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.{ProtocolVersionedMemoizedEvidence, ProtoConverter}
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtoV0,
  HasProtocolVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.logging.pretty.PrettyInstances._
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId

/** A signed topology transaction
  *
  * Every topology transaction needs to be authorized by an appropriate key. This object represents such
  * an authorization, where there is a signature of a given key of the given topology transaction.
  *
  * Whether the key is eligible to authorize the topology transaction depends on the topology state
  */
case class SignedTopologyTransaction[+Op <: TopologyChangeOp](
    transaction: TopologyTransaction[Op],
    key: SigningPublicKey,
    signature: Signature,
)(
    val representativeProtocolVersion: ProtocolVersion,
    val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[VersionedMessage[SignedTopologyTransaction[TopologyChangeOp]]]
    with HasProtoV0[v0.SignedTopologyTransaction]
    with ProtocolVersionedMemoizedEvidence
    with Product
    with Serializable
    with PrettyPrinting {

  override protected def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected def toProtoVersioned
      : VersionedMessage[SignedTopologyTransaction[TopologyChangeOp]] =
    SignedTopologyTransaction.toProtoVersioned(this)

  override protected def toProtoV0: v0.SignedTopologyTransaction =
    v0.SignedTopologyTransaction(
      transaction = transaction.getCryptographicEvidence,
      key = Some(key.toProtoV0),
      signature = Some(signature.toProtoV0),
    )

  def verifySignature(pureApi: CryptoPureApi): Either[SignatureCheckError, Unit] = {
    val hash = transaction.hashToSign(pureApi)
    pureApi.verifySignature(hash, key, signature)
  }

  override def pretty: Pretty[SignedTopologyTransaction.this.type] =
    prettyOfClass(unnamedParam(_.transaction), param("key", _.key))

  def uniquePath: UniquePath = transaction.element.uniquePath

  def operation: Op = transaction.op

  def restrictedToDomain: Option[DomainId] = transaction.element.mapping.restrictedToDomain
}

object SignedTopologyTransaction
    extends HasMemoizedProtocolVersionedWrapperCompanion[SignedTopologyTransaction[
      TopologyChangeOp
    ]] {
  override val name: String = "SignedTopologyTransaction"

  val supportedProtoVersions = SupportedProtoVersions(
    0 -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.SignedTopologyTransaction)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  import com.digitalasset.canton.resource.DbStorage.Implicits._

  /** Sign the given topology transaction. */
  def create[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: SigningPublicKey,
      hashOps: HashOps,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
  )(implicit ec: ExecutionContext): EitherT[Future, SigningError, SignedTopologyTransaction[Op]] =
    for {
      signature <- crypto.sign(transaction.hashToSign(hashOps), signingKey.id)
    } yield SignedTopologyTransaction(transaction, signingKey, signature)(protocolVersion, None)

  private def fromProtoV0(transactionP: v0.SignedTopologyTransaction)(
      bytes: ByteString
  ): ParsingResult[SignedTopologyTransaction[TopologyChangeOp]] =
    for {
      transaction <- TopologyTransaction.fromByteString(transactionP.transaction)
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV0,
        "key",
        transactionP.key,
      )
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV0,
        "signature",
        transactionP.signature,
      )
      protocolVersion = supportedProtoVersions.protocolVersionRepresentativeFor(0)
    } yield SignedTopologyTransaction(transaction, publicKey, signature)(
      protocolVersion,
      Some(bytes),
    )

  /** returns true if two transactions are equivalent */
  def equivalent(
      first: SignedTopologyTransaction[TopologyChangeOp],
      second: SignedTopologyTransaction[TopologyChangeOp],
  ): Boolean = (first, second) match {
    case (
          SignedTopologyTransaction(firstTx, firstKey, _),
          SignedTopologyTransaction(secondTx, secondKey, _),
        ) =>
      TopologyTransaction.equivalent(firstTx, secondTx) && firstKey == secondKey
  }

  def createGetResultDomainTopologyTransaction
      : GetResult[SignedTopologyTransaction[TopologyChangeOp]] =
    GetResult { r =>
      fromByteString(r.<<)
        .valueOr(err =>
          throw new DbSerializationException(s"Failed to deserialize TopologyTransaction: $err")
        )
    }

  implicit def setParameterTopologyTransaction(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[SignedTopologyTransaction[TopologyChangeOp]] = {
    (d: SignedTopologyTransaction[TopologyChangeOp], pp: PositionedParameters) =>
      pp >> d.toByteArray
  }
}
