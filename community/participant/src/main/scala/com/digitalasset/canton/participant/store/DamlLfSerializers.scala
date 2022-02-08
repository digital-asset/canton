// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.protocol.{LfContractInst, LfVersionedTransaction}
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

  def serializeContract(contract: LfContractInst): Either[EncodeError, ByteString] =
    TransactionCoder
      .encodeContractInstance(ValueCoder.CidEncoder, contract)
      .map(_.toByteString)

  def deserializeContract(bytes: ByteString): Either[DecodeError, LfContractInst] =
    TransactionCoder
      .decodeVersionedContractInstance(
        ValueCoder.CidDecoder,
        TransactionOuterClass.ContractInstance.parseFrom(bytes),
      )
}
