// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.PrettyInstances.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, checked}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.SetParameter

import scala.reflect.ClassTag

/** Replace or Remove */
sealed trait TopologyChangeOp extends Product with Serializable with PrettyPrinting {
  def toProto: v30.Enums.TopologyChangeOp

  final def select[TargetOp <: TopologyChangeOp](implicit
      O: ClassTag[TargetOp]
  ): Option[TargetOp] = O.unapply(this)

  override protected def pretty: Pretty[TopologyChangeOp.this.type] = adHocPrettyInstance
}

object TopologyChangeOp {

  /** Adds or replaces an existing mapping with the same unique key. */
  final case object Replace extends TopologyChangeOp {
    override def toProto: v30.Enums.TopologyChangeOp =
      v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE
  }

  /** Removes an existing mappings with the same unique key. */
  final case object Remove extends TopologyChangeOp {
    override def toProto: v30.Enums.TopologyChangeOp =
      v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_REMOVE
  }

  type Remove = Remove.type
  type Replace = Replace.type

  def unapply(
      tx: TopologyTransaction[TopologyChangeOp, TopologyMapping]
  ): Option[TopologyChangeOp] = Some(tx.operation)
  def unapply(
      tx: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]
  ): Option[TopologyChangeOp] = Some(tx.operation)

  def fromProtoV30(
      protoOp: v30.Enums.TopologyChangeOp
  ): ParsingResult[Option[TopologyChangeOp]] =
    protoOp match {
      case v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED => Right(None)
      case v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_REMOVE => Right(Some(Remove))
      case v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE => Right(Some(Replace))
      case v30.Enums.TopologyChangeOp.Unrecognized(x) => Left(UnrecognizedEnum(protoOp.name, x))
    }

  implicit val setParameterTopologyChangeOp: SetParameter[TopologyChangeOp] = (v, pp) =>
    pp.setInt(v.toProto.value)

}

trait TopologyTransactionLike[+Op <: TopologyChangeOp, +M <: TopologyMapping] {
  def operation: Op
  def serial: PositiveInt
  def mapping: M
  def hash: TxHash
}

trait DelegatedTopologyTransactionLike[+Op <: TopologyChangeOp, +M <: TopologyMapping]
    extends TopologyTransactionLike[Op, M] {
  protected def transactionLikeDelegate: TopologyTransactionLike[Op, M]
  override final def operation: Op = transactionLikeDelegate.operation
  override final def serial: PositiveInt = transactionLikeDelegate.serial
  override final def mapping: M = transactionLikeDelegate.mapping
  override final def hash: TxHash = transactionLikeDelegate.hash

}

/** Change to the distributed synchronizer topology
  *
  * A topology transaction is a state change to the synchronizer topology. There are different types
  * of topology states (so called mappings, because they map some id to some value).
  *
  * Each mapping has some variables and some combination of these variables makes a "unique key".
  * Subsequent changes to that key need to have an incremental serial number.
  *
  * Topology changes always affect certain identities. Therefore, these topology transactions need
  * to be authorized through signatures.
  *
  * An authorized transaction is called a [[SignedTopologyTransaction]]
  *
  * Invariant:
  *   - Instances of [[TopologyTransaction]] are guaranteed to be serializable (i.e. their
  *     serialization does not fail)
  *
  * In order to ensure that instances are serializable, the `create` constructor tries to serialize
  * the new instance and returns it only if serialization is successful. For this, the class needs
  * to inherit from `HasProtocolVersionedWrapperE` and thereby have serialization methods that can
  * possibly fail.
  */
final case class TopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping] private (
    operation: Op,
    serial: PositiveInt,
    mapping: M,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TopologyTransaction.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends TopologyTransactionLike[Op, M]
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrinting
    with HasProtocolVersionedWrapperE[TopologyTransaction[TopologyChangeOp, TopologyMapping]] {

  @VisibleForTesting
  def reverse: TopologyTransaction[TopologyChangeOp, M] = {
    val next = (operation: TopologyChangeOp) match {
      case TopologyChangeOp.Replace => TopologyChangeOp.Remove
      case TopologyChangeOp.Remove => TopologyChangeOp.Replace
    }
    // Reversing does not change serializability
    checked(
      TopologyTransaction
        .create(
          next,
          serial = serial.increment,
          mapping = mapping,
          representativeProtocolVersion,
        )
        .valueOr(err =>
          throw new IllegalStateException(s"Failed to reverse topology transaction: $err")
        )
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMapping: ClassTag]
      : Option[TopologyTransaction[Op, TargetMapping]] =
    mapping
      .select[TargetMapping]
      .map(_ => this.asInstanceOf[TopologyTransaction[Op, TargetMapping]])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOp: ClassTag]: Option[TopologyTransaction[TargetOp, M]] =
    operation.select[TargetOp].map(_ => this.asInstanceOf[TopologyTransaction[TargetOp, M]])

  def select[TargetOp <: TopologyChangeOp: ClassTag, TargetMapping <: TopologyMapping: ClassTag]
      : Option[TopologyTransaction[TargetOp, TargetMapping]] =
    selectOp[TargetOp].flatMap(_.selectMapping[TargetMapping])

  /** returns hash of the given transaction */
  lazy val hash: TxHash =
    TxHash(
      Hash.digest(
        HashPurpose.TopologyTransactionSignature,
        this.getCryptographicEvidence,
        HashAlgorithm.Sha256,
      )
    )

  @VisibleForTesting // Annotated because the visibility is lifted to be used in tests
  override def toByteStringUnmemoized: ByteString = checked(
    super[HasProtocolVersionedWrapperE].toByteString
      .valueOr(_ =>
        throw new IllegalStateException("Invariant violation: this class should be serializable")
      )
  )

  /** Same as `toByteString`, but does not require the caller to handle an error, given the class
    * invariant that it is serializable.
    */
  // TODO(i33934): use memoization for `toByteString` (see `fromProtoV30` below)
  def toByteStringChecked: ByteString = toByteStringUnmemoized

  def toProtoV30: Either[String, v30.TopologyTransaction] =
    mapping.toProtoV30.map(serializedMapping =>
      v30.TopologyTransaction(
        operation = operation.toProto,
        serial = serial.value,
        mapping = Some(serializedMapping),
      )
    )

  def toProtoV31: Either[String, v31.TopologyTransaction] =
    mapping.toProtoV31.map(serializedMapping =>
      v31.TopologyTransaction(
        operation = operation.toProto,
        serial = serial.value,
        mapping = Some(serializedMapping),
      )
    )

  /** Indicates how to pretty print this instance. See `PrettyPrintingTest` for examples on how to
    * implement this method.
    */
  override protected def pretty: Pretty[TopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.mapping),
      param("serial", _.serial),
      param("operation", _.operation),
      param("hash", _.hash.hash),
    )

  @transient override protected lazy val companionObj: TopologyTransaction.type =
    TopologyTransaction
}

object TopologyTransaction
    extends VersioningCompanionMemoizationE[
      TopologyTransaction[TopologyChangeOp, TopologyMapping]
    ] {

  final case class TxHash(hash: Hash) extends AnyVal

  override val name: String = "TopologyTransaction"

  type GenericTopologyTransaction = TopologyTransaction[TopologyChangeOp, TopologyMapping]
  type PositiveTopologyTransaction = TopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]

  val versioningTable: VersioningTable =
    VersioningTable(
      ProtoVersion(30) -> VersionedProtoCodec.applyE(ProtocolVersion.v34)(v30.TopologyTransaction)(
        supportedProtoVersionMemoized(_)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def create[Op <: TopologyChangeOp, M <: TopologyMapping](
      op: Op,
      serial: PositiveInt,
      mapping: M,
      protocolVersion: ProtocolVersion,
  ): Either[String, TopologyTransaction[Op, M]] = {
    val rpv = protocolVersionRepresentativeFor(protocolVersion)
    create(op, serial, mapping, rpv)
  }

  def tryCreate[Op <: TopologyChangeOp, M <: TopologyMapping](
      op: Op,
      serial: PositiveInt,
      mapping: M,
      protocolVersion: ProtocolVersion,
  ): TopologyTransaction[Op, M] =
    create(op, serial, mapping, protocolVersion).valueOr(err =>
      throw new IllegalStateException(s"Failed to create topology transaction: $err")
    )

  private def create[Op <: TopologyChangeOp, M <: TopologyMapping](
      op: Op,
      serial: PositiveInt,
      mapping: M,
      rpv: RepresentativeProtocolVersion[TopologyTransaction.type],
  ): Either[String, TopologyTransaction[Op, M]] = {
    val transaction = TopologyTransaction[Op, M](op, serial, mapping)(rpv, None)

    // Ensure the transaction is serializable
    // Technically, at this point we could pass the serialized bytes for the `deserializedFrom`, but this
    // seems to break `TopologyTransactionProcessorTest`, see `TestingIdentityFactory#mkTrans`.
    // TODO(i33934): fix this
    transaction.toByteString.map(_ => TopologyTransaction[Op, M](op, serial, mapping)(rpv, None))
  }

  private def fromProtoV30(transactionP: v30.TopologyTransaction)(
      bytes: ByteString
  ): ParsingResult[TopologyTransaction[TopologyChangeOp, TopologyMapping]] = {
    val v30.TopologyTransaction(opP, serialP, mappingP) = transactionP
    for {
      mapping <- ProtoConverter.parseRequired(TopologyMapping.fromProtoV30, "mapping", mappingP)
      serial <- ProtoConverter.parsePositiveInt("serial", serialP)
      op <- ProtoConverter.parseEnum(TopologyChangeOp.fromProtoV30, "operation", opP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      tx = TopologyTransaction(op, serial, mapping)(
        rpv,
        Some(bytes),
      )
      // Ensure the transaction is serializable
      // TODO(i33934): use memoization for `toByteString`; this will implicitly guarantee that a deserialized transaction is serializable
      _ <- tx.toByteString.leftMap(err =>
        ProtoDeserializationError.OtherError(s"Transaction is not serializable: $err")
      )
    } yield tx
  }

  // TODO(i32231): Note: can be made private when we add v31 support to the versioningTable
  protected def fromProtoV31(transactionP: v31.TopologyTransaction)(
      bytes: ByteString
  ): ParsingResult[TopologyTransaction[TopologyChangeOp, TopologyMapping]] = {
    val v31.TopologyTransaction(opP, serialP, mappingP) = transactionP
    for {
      mapping <- ProtoConverter.parseRequired(TopologyMapping.fromProtoV31, "mapping", mappingP)
      serial <- ProtoConverter.parsePositiveInt("serial", serialP)
      op <- ProtoConverter.parseEnum(TopologyChangeOp.fromProtoV30, "operation", opP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
    } yield TopologyTransaction(op, serial, mapping)(
      rpv,
      Some(bytes),
    )
  }
}
