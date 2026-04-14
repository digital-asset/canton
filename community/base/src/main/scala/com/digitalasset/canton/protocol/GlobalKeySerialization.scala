// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfVersioned, ProtoDeserializationError}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}

object GlobalKeySerialization {

  def toProtoV30(globalKey: LfVersioned[LfGlobalKey]): Either[String, v30.GlobalKey] = {
    val templateIdP = ValueCoder.encodeIdentifier(globalKey.unversioned.templateId)
    for {
      // Contract keys are not allowed to hold contract ids; therefore it is "okay"
      // to use a dummy LfContractId encoder.
      keyP <- ValueCoder
        .encodeVersionedValue(globalKey.map(_.key))
        .leftMap(_.errorMessage)
    } yield v30.GlobalKey(
      templateId = templateIdP.toByteString,
      key = keyP.toByteString,
      globalKey.unversioned.packageName,
    )
  }

  def assertToProtoV30(key: LfVersioned[LfGlobalKey]): v30.GlobalKey =
    toProtoV30(key)
      .valueOr(err => throw new IllegalArgumentException(s"Can't encode contract key: $err"))

  def fromProtoV30(globalKeyP: v30.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] = {
    val v30.GlobalKey(templateIdBytes, keyBytes, packageNameP) = globalKeyP
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

      packageName <- Ref.PackageName
        .fromString(packageNameP)
        .leftMap(err => ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err))

      globalKey <- crypto.Hash
        .hashContractKey(templateId, packageName, versionedKey.unversioned)
        .flatMap(keyHash =>
          LfGlobalKey.build(templateId, packageName, versionedKey.unversioned, keyHash)
        )
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
        )

    } yield LfVersioned(versionedKey.version, globalKey)
  }

  def toProtoV31(globalKey: LfVersioned[LfGlobalKey]): Either[String, v31.GlobalKey] = {
    val templateIdP = ValueCoder.encodeIdentifier(globalKey.unversioned.templateId)
    for {
      keyP <- ValueCoder
        .encodeVersionedValue(globalKey.map(_.key))
        .leftMap(_.errorMessage)
    } yield v31.GlobalKey(
      templateId = templateIdP.toByteString,
      key = keyP.toByteString,
      globalKey.unversioned.packageName,
      hash = globalKey.unversioned.hash.bytes.toByteString,
    )
  }

  def assertToProtoV31(key: LfVersioned[LfGlobalKey]): v31.GlobalKey =
    toProtoV31(key)
      .valueOr(err => throw new IllegalArgumentException(s"Can't encode contract key: $err"))

  def fromProtoV31(globalKeyP: v31.GlobalKey): ParsingResult[LfVersioned[LfGlobalKey]] = {
    val v31.GlobalKey(templateIdBytes, keyBytes, packageNameP, hashBytes) = globalKeyP
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
      hash <- com.digitalasset.daml.lf.crypto.Hash
        .fromBytes(Bytes.fromByteString(hashBytes))
        .left
        .map(ProtoDeserializationError.OtherError(_))
      keyP <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(keyBytes)
      versionedKey <- ValueCoder
        .decodeVersionedValue(keyP)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err.toString)
        )

      packageName <- Ref.PackageName
        .fromString(packageNameP)
        .leftMap(err => ProtoDeserializationError.ValueDeserializationError("GlobalKey.proto", err))

      globalKey <- LfGlobalKey
        .build(templateId, packageName, versionedKey.unversioned, hash)
        .leftMap(err =>
          ProtoDeserializationError.ValueDeserializationError("GlobalKey.key", err.toString)
        )

    } yield LfVersioned(versionedKey.version, globalKey)
  }
}
