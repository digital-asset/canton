// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import com.daml.lf.transaction.TransactionCoder
import com.daml.lf.transaction.TransactionOuterClass
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  MemoizedEvidenceWithFailure,
  ProtoConverter,
  SerializationCheckFailed,
}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

/** Represents a serializable contract instance and memoizes the serialization.
  *
  * @param contractInstance The contract instance whose serialization is to be memoized.
  * @param deserializedFrom If set, the given [[ByteString]] will be deemed to be the valid serialization for
  *                         the given contract instance. If [[None]],
  *                         the serialization is produced by [[TransactionCoder.encodeContractInstance]].
  */
case class SerializableRawContractInstance private (contractInstance: LfContractInst)(
    override val deserializedFrom: Option[ByteString]
) extends MemoizedEvidenceWithFailure[ValueCoder.EncodeError] {

  /** @throws com.digitalasset.canton.serialization.SerializationCheckFailed If the serialization of the contract instance failed
    */
  @throws[SerializationCheckFailed[ValueCoder.EncodeError]]
  protected[this] override def toByteStringChecked: Either[ValueCoder.EncodeError, ByteString] =
    TransactionCoder
      .encodeContractInstance(ValueCoder.CidEncoder, contractInstance)
      .map(_.toByteString)

  lazy val contractHash: LfHash =
    LfHash.assertHashContractInstance(
      contractInstance.unversioned.template,
      contractInstance.unversioned.arg,
    )
}

object SerializableRawContractInstance {
  implicit def contractGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[SerializableRawContractInstance] = GetResult { r =>
    SerializableRawContractInstance
      .fromByteString(ByteString.copyFrom(r.<<[Array[Byte]]))
      .getOrElse(throw new DbDeserializationException("Invalid contract instance"))
  }

  implicit def contractSetParameter(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[SerializableRawContractInstance] = (c, pp) =>
    pp >> c.getCryptographicEvidence.toByteArray

  // Make the apply method inaccessible such that creation must happen through factory methods
  private[this] def apply(
      contractInstance: LfContractInst
  )(deserializedFrom: Option[ByteString]): SerializableRawContractInstance =
    throw new UnsupportedOperationException(
      "Use a factory method for creating SerializableRawContractInstance"
    )

  def create(
      contractInstance: LfContractInst
  ): Either[ValueCoder.EncodeError, SerializableRawContractInstance] =
    try {
      Right(new SerializableRawContractInstance(contractInstance)(None))
    } catch {
      case SerializationCheckFailed(err: ValueCoder.EncodeError) => Left(err)
    }

  /** Build a [[SerializableRawContractInstance]] from lf-protobuf and ContractId encoded ContractInst
    * @param bytes byte string representing contract instance
    * @return  contract id
    */
  def fromByteString(
      bytes: ByteString
  ): ParsingResult[SerializableRawContractInstance] =
    for {
      contractInstanceP <- ProtoConverter.protoParser(
        TransactionOuterClass.ContractInstance.parseFrom
      )(bytes)
      contractInstance <- TransactionCoder
        .decodeVersionedContractInstance(ValueCoder.CidDecoder, contractInstanceP)
        .leftMap(error => ValueConversionError("", error.toString))
    } yield createWithSerialization(contractInstance)(bytes)

  @VisibleForTesting
  def createWithSerialization(contractInst: LfContractInst)(
      deserializedFrom: ByteString
  ): SerializableRawContractInstance =
    new SerializableRawContractInstance(contractInst)(Some(deserializedFrom))
}
