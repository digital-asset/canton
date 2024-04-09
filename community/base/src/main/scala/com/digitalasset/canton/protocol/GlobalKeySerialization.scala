// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfVersioned, ProtoDeserializationError}

object GlobalKeySerialization {

  def toProto(globalKey: LfVersioned[LfGlobalKey]): Either[String, v30.GlobalKey] = {
    val templateIdP = ValueCoder.encodeIdentifier(globalKey.unversioned.templateId)
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      keyP <- ValueCoder
        .encodeVersionedValue(globalKey.map(_.key))
        .leftMap(_.errorMessage)
    } yield v30.GlobalKey(templateId = templateIdP.toByteString, key = keyP.toByteString)
  }

  def assertToProto(key: LfVersioned[LfGlobalKey]): v30.GlobalKey =
    toProto(key)
      .fold(
        err => throw new IllegalArgumentException(s"Can't encode contract key: $err"),
        identity,
      )

  def fromProtoV30(globalKeyP: v30.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] = {
    val v30.GlobalKey(templateIdBytes, keyBytes) = globalKeyP
    for {
      templateIdP <- ProtoConverter.protoParser(ValueOuterClass.Identifier.parseFrom)(
        templateIdBytes
      )
      templateId <- ValueCoder
        .decodeIdentifier(templateIdP)
        .leftMap(err =>
          ProtoDeserializationError
            .ValueDeserializationError("GlobalKey.templateId", err.errorMessage)
        )

      keyP <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(
        keyBytes
      )
      versionedKey <- ValueCoder
        .decodeVersionedValue(keyP)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
        )

      globalKey <-
        LfGlobalKey
          .build(templateId, versionedKey.unversioned)
          .leftMap(err =>
            ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
          )

    } yield LfVersioned(versionedKey.version, globalKey)
  }

}
