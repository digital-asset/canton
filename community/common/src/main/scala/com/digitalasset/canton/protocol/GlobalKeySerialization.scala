// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import com.daml.lf.CantonOnly
import com.daml.lf.value.ValueCoder.{CidEncoder => LfDummyCidEncoder}
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.LfTransactionUtil

object GlobalKeySerialization {

  def toProto(
      globalKey: LfGlobalKey,
      transactionVersion: LfTransactionVersion,
  ): Either[String, v0.GlobalKey] = {
    // TODO(oliver, #2720): Track contract keys by their hash, i.e. store and send/serialize them by the hash
    val serializedTemplateId = ValueCoder.encodeIdentifier(globalKey.templateId).toByteString
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay" to use an LfContractId encoder
      // which we will no longer use anyway, once we represent contract keys by hashes instead of key values.
      // Until then serialize/deserialize the value versioned to make the byte string self-describing as not all
      // the callers have an lf node with an implied transaction version.
      serializedKey <- ValueCoder
        .encodeVersionedValue(LfDummyCidEncoder, transactionVersion, globalKey.key)
        .map(_.toByteString)
        .leftMap(_.errorMessage)
    } yield v0.GlobalKey(serializedTemplateId, serializedKey)
  }

  def assertToProto(key: LfGlobalKey): v0.GlobalKey =
    toProto(
      key,
      CantonOnly.DummyTransactionVersion,
    ) // TODO(oliver, #2720): Only production usage that relies on DummyTransactionVersion
      .fold(
        err => throw new IllegalArgumentException(s"Can't encode contract key: $err"),
        identity,
      )

  def fromProtoV0(protoKey: v0.GlobalKey): ParsingResult[LfGlobalKey] =
    for {
      pTemplateId <- ProtoConverter.protoParser(ValueOuterClass.Identifier.parseFrom)(
        protoKey.templateId
      )
      templateId <- ValueCoder
        .decodeIdentifier(pTemplateId)
        .leftMap(err =>
          ProtoDeserializationError
            .ValueDeserializationError("GlobalKey.templateId", err.errorMessage)
        )
      deserializedProtoKey <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(
        protoKey.key
      )
      unsafeKey <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, deserializedProtoKey)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
        )
      key <- LfTransactionUtil
        .checkNoContractIdInKey(unsafeKey)
        .leftMap(cid =>
          ProtoDeserializationError
            .ValueDeserializationError("GlobalKey.key", s"Key contains contract Id $cid")
        )
    } yield LfGlobalKey(templateId, key) // TODO(oliver, #2720): read hash instead of key value

}
