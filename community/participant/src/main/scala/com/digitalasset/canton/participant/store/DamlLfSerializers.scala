// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.lf.CantonOnly
import com.daml.lf.transaction.{Node, TransactionCoder, TransactionOuterClass, Versioned}
import com.daml.lf.value.Value.ContractInstanceWithAgreement
import com.daml.lf.value.ValueCoder
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.digitalasset.canton.protocol
import com.digitalasset.canton.protocol.{
  AgreementText,
  LfContractInst,
  LfNodeCreate,
  LfNodeId,
  LfVersionedTransaction,
}
import com.google.protobuf.ByteString

/** Serialization and deserialization utilities for transactions and contracts.
  * Only intended for use within database storage.
  * Should not be used for hashing as no attempt is made to keep the serialization deterministic.
  * Errors are returned as an Either but it is expected callers will eventually throw a [[com.digitalasset.canton.store.db.DbSerializationException]] or [[com.digitalasset.canton.store.db.DbDeserializationException]].
  * Currently throws [[com.google.protobuf.InvalidProtocolBufferException]] if the `parseFrom` operations fail to read the provided bytes.
  */
private[store] object DamlLfSerializers {

  def serializeTransaction(
      versionedTransaction: LfVersionedTransaction
  ): Either[EncodeError, ByteString] =
    TransactionCoder
      .encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, versionedTransaction)
      .map(_.toByteString)

  def deserializeTransaction(bytes: ByteString): Either[DecodeError, LfVersionedTransaction] =
    TransactionCoder
      .decodeTransaction(
        TransactionCoder.NidDecoder,
        ValueCoder.CidDecoder,
        TransactionOuterClass.Transaction.parseFrom(bytes),
      )

  def serializeContract(
      contract: LfContractInst,
      agreementText: AgreementText,
  ): Either[EncodeError, ByteString] =
    TransactionCoder
      .encodeContractInstance(
        ValueCoder.CidEncoder,
        contract.map(ContractInstanceWithAgreement(_, agreementText.v)),
      )
      .map(_.toByteString)

  def deserializeContract(
      bytes: ByteString
  ): Either[DecodeError, Versioned[ContractInstanceWithAgreement]] =
    TransactionCoder
      .decodeVersionedContractInstance(
        ValueCoder.CidDecoder,
        TransactionOuterClass.ContractInstance.parseFrom(bytes),
      )

  def deserializeCreateNode(
      proto: TransactionOuterClass.Node
  ): Either[DecodeError, protocol.LfNodeCreate] = {
    for {
      version <- TransactionCoder.decodeVersion(proto.getVersion)
      nodeWithId <- CantonOnly.decodeVersionedNode(
        TransactionCoder.NidDecoder,
        ValueCoder.CidDecoder,
        version,
        proto,
      )
      createNode <- nodeWithId._2 match {
        case create: Node.Create => Right(create)
        case node => Left(DecodeError(s"Failed deserialize Create Node $node"))
      }
    } yield createNode
  }

  def serializeCreateNode(
      create: LfNodeCreate
  ): Either[EncodeError, ByteString] =
    CantonOnly
      .encodeNode(
        TransactionCoder.NidEncoder,
        ValueCoder.CidEncoder,
        create.version,
        LfNodeId(0),
        create,
      )
      .map(_.toByteString)

}
