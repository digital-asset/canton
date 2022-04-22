// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.bifunctor._
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DeserializationError,
  HasCryptographicEvidence,
  ProtoConverter,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.version.HasProtoV0
import com.digitalasset.canton.{LedgerTransactionId, ProtoDeserializationError}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

/** The root hash of a Merkle tree used as an identifier for requests.
  *
  * Extends [[com.digitalasset.canton.serialization.HasCryptographicEvidence]] so that [[RootHash]]'s serialization
  * can be used to compute the hash of an inner Merkle node from its children using [[RootHash.getCryptographicEvidence]].
  * Serialization to Protobuf fields can be done with [[RootHash.toProtoPrimitive]]
  */
case class RootHash(private val hash: Hash) extends PrettyPrinting with HasCryptographicEvidence {
  def unwrap: Hash = hash

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence

  def toProtoPrimitive: ByteString = getCryptographicEvidence

  override def pretty: Pretty[RootHash] = prettyOfParam(_.unwrap)
}

object RootHash {
  def fromByteString(bytes: ByteString): Either[DeserializationError, RootHash] =
    Hash.fromByteString(bytes).map(RootHash(_))

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[RootHash] =
    Hash.fromProtoPrimitive(bytes).map(RootHash(_))

  def fromProtoPrimitiveOption(
      bytes: ByteString
  ): ParsingResult[Option[RootHash]] =
    Hash.fromProtoPrimitiveOption(bytes).map(_.map(RootHash(_)))
}

/** A hash-based transaction id. */
case class TransactionId(private val hash: Hash) extends HasCryptographicEvidence {
  def unwrap: Hash = hash

  def toRootHash: RootHash = RootHash(hash)

  def toProtoPrimitive: ByteString = getCryptographicEvidence

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence

  def asLedgerTransactionId: Either[String, LedgerTransactionId] =
    LedgerTransactionId.fromString(hash.toHexString)

  def tryAsLedgerTransactionId: LedgerTransactionId =
    LedgerTransactionId.assertFromString(hash.toHexString)
}

object TransactionId {

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[TransactionId] =
    Hash
      .fromByteString(bytes)
      .bimap(ProtoDeserializationError.CryptoDeserializationError, TransactionId(_))

  def fromRootHash(rootHash: RootHash): TransactionId = TransactionId(rootHash.unwrap)

  /** Ordering for [[TransactionId]]s based on the serialized hash */
  implicit val orderTransactionId: Order[TransactionId] =
    Order.by[TransactionId, ByteString](_.hash.getCryptographicEvidence)(
      ByteStringUtil.orderByteString
    )

  implicit val orderingTransactionId: Ordering[TransactionId] = orderTransactionId.toOrdering

  implicit val prettyTransactionId: Pretty[TransactionId] = {
    import Pretty._
    prettyOfParam(_.unwrap)
  }

  implicit val setParameterTransactionId: SetParameter[TransactionId] = (v, pp) => pp.>>(v.hash)

  implicit val getResultTransactionId: GetResult[TransactionId] = GetResult { r =>
    TransactionId(r.<<)
  }

  implicit val setParameterOptionTransactionId: SetParameter[Option[TransactionId]] = (v, pp) =>
    pp.>>(v.map(_.hash))

  implicit val getResultOptionTransactionId: GetResult[Option[TransactionId]] = GetResult { r =>
    (r.<<[Option[Hash]]).map(TransactionId(_))
  }
}

/** A hash-based transaction view id
  *
  * Views from different requests may have the same view hash.
  */
case class ViewHash(private val hash: Hash) extends PrettyPrinting {
  def unwrap: Hash = hash

  def toProtoPrimitive: ByteString = hash.getCryptographicEvidence

  def toRootHash: RootHash = RootHash(hash)

  override def pretty: Pretty[ViewHash] = prettyOfClass(unnamedParam(_.hash))
}

object ViewHash {

  def fromProtoPrimitive(hash: ByteString): ParsingResult[ViewHash] =
    Hash.fromProtoPrimitive(hash).map(ViewHash(_))

  def fromProtoPrimitiveOption(
      hash: ByteString
  ): ParsingResult[Option[ViewHash]] =
    Hash.fromProtoPrimitiveOption(hash).map(_.map(ViewHash(_)))

  def fromRootHash(hash: RootHash): ViewHash = ViewHash(hash.unwrap)

  /** Ordering for [[ViewHash]] based on the serialized hash */
  implicit val orderViewHash: Order[ViewHash] =
    Order.by[ViewHash, ByteString](_.hash.getCryptographicEvidence)(ByteStringUtil.orderByteString)
}

/** A confirmation request is identified by the sequencer timestamp. */
case class RequestId(private val ts: CantonTimestamp) extends PrettyPrinting {
  def unwrap: CantonTimestamp = ts

  override def pretty: Pretty[RequestId] = prettyOfClass(unnamedParam(_.ts))
}

object RequestId {
  implicit val requestIdOrdering = Ordering.by[RequestId, CantonTimestamp](_.unwrap)
  implicit val requestIdOrder = Order.fromOrdering[RequestId]
}

/** A transfer is identified by the origin domain and the sequencer timestamp on the transfer-out request. */
case class TransferId(originDomain: DomainId, requestTimestamp: CantonTimestamp)
    extends PrettyPrinting
    with HasProtoV0[v0.TransferId] {
  override def toProtoV0: v0.TransferId =
    v0.TransferId(
      originDomain = originDomain.toProtoPrimitive,
      timestamp = Some(requestTimestamp.toProtoPrimitive),
    )

  override def pretty: Pretty[TransferId] = prettyOfClass(
    param("ts", _.requestTimestamp),
    param("origin", _.originDomain),
  )

}

object TransferId {
  def fromProtoV0(transferIdP: v0.TransferId): ParsingResult[TransferId] =
    transferIdP match {
      case v0.TransferId(originDomainP, requestTimestampP) =>
        for {
          originDomain <- DomainId.fromProtoPrimitive(originDomainP, "TransferId.originDomain")
          requestTimestamp <- ProtoConverter
            .required("TransferId.timestamp", requestTimestampP)
            .flatMap(CantonTimestamp.fromProtoPrimitive)
        } yield TransferId(originDomain, requestTimestamp)
    }
}
