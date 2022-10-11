// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  *
  * This wrapper is to be used if a single instance needs to be serialized to different proto versions.
  *
  * The underlying ProtoClass is [[com.digitalasset.canton.version.UntypedVersionedMessage]]
  * but we often specify the typed alias [[com.digitalasset.canton.version.VersionedMessage]]
  * instead.
  */
trait HasVersionedWrapper[+ProtoClass <: scalapb.GeneratedMessage]
    extends HasVersionedToByteString {

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    */
  protected def toProtoVersioned(version: ProtocolVersion): ProtoClass

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  override def toByteString(version: ProtocolVersion): ByteString = toProtoVersioned(
    version
  ).toByteString

  /** Yields a byte array representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteArray(version: ProtocolVersion): Array[Byte] = toByteString(version).toByteArray

  /** Writes the byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance to a file. */
  def writeToFile(outputFile: String, version: ProtocolVersion = ProtocolVersion.latest): Unit = {
    val bytes = toByteString(version)
    BinaryFileUtil.writeByteStringToFile(outputFile, bytes)
  }
}

/** Traits for the companion objects of classes that implement [[HasVersionedWrapper]].
  * Provide default methods.
  */
trait HasVersionedMessageCompanion[
    ValueClass <: HasVersionedWrapper[VersionedMessage[ValueClass]]
] {

  /** The name of the class as used for pretty-printing and error reporting */
  protected def name: String

  /** Proto versions that are supported by `fromProtoVersioned` and `fromByteString`
    * See the helpers `supportedProtoVersion` and `supportedProtoVersionMemoized`
    * below to define a `Parser`.
    */
  protected def supportedProtoVersions: Map[Int, Parser]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[ValueClass]
  ): ByteString => ParsingResult[ValueClass] =
    ProtoConverter.protoParser(p.parseFrom)(_).flatMap(fromProto)

  type Parser = ByteString => ParsingResult[ValueClass]

  def fromProtoVersioned(
      proto: VersionedMessage[ValueClass]
  ): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      data =>
        supportedProtoVersions
          .get(proto.version)
          .map(_(data))
          .getOrElse(ProtoDeserializationError.VersionError(name, proto.version).asLeft[ValueClass])
    }

  def fromByteString(bytes: ByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def tryFromByteString(bytes: ByteString): ValueClass =
    fromByteString(bytes).valueOr(err =>
      throw new IllegalArgumentException(s"Deserializing $name bytestring failed: $err")
    )

  def fromByteArray(bytes: Array[Byte]): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParserArray(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def readFromFile(
      inputFile: String
  ): Either[String, ValueClass] = {
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromByteString(bs).leftMap(_.toString)
    } yield value
  }

  def tryReadFromFile(inputFile: String): ValueClass = readFromFile(inputFile).valueOr(err =>
    throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
  )

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[ValueClass] = GetResult { r =>
    fromByteArray(r.<<[Array[Byte]]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[ValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }

  def getVersionedSetParameter(protocolVersion: ProtocolVersion)(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[ValueClass] = { (value, pp) =>
    pp >> value.toByteArray(protocolVersion)
  }

  def getVersionedSetParameterO(protocolVersion: ProtocolVersion)(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[ValueClass]] =
    (valueO, pp) => pp >> valueO.map(_.toByteArray(protocolVersion))
}

/** Traits for the companion objects of classes that implement [[HasVersionedWrapper]].
  * They provide default methods.
  * Unlike [[HasVersionedMessageCompanion]] these traits allow to pass additional
  * context to the conversion methods (see, e.g., [[com.digitalasset.canton.data.TransferInViewTree.fromProtoVersioned]]
  * which takes a `HashOps` parameter).
  */
trait HasVersionedMessageWithContextCompanion[ValueClass, Ctx] {

  /** The name of the class as used for pretty-printing and error reporting */
  protected def name: String

  protected def supportedProtoVersions: Map[Int, Parser]

  type Parser = (Ctx, ByteString) => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Ctx, Proto) => ParsingResult[ValueClass]
  ): (Ctx, ByteString) => ParsingResult[ValueClass] =
    (ctx: Ctx, data: ByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _))

  def fromProtoVersioned(
      ctx: Ctx
  )(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      data =>
        supportedProtoVersions
          .get(proto.version)
          .map(_(ctx, data))
          .getOrElse(ProtoDeserializationError.VersionError(name, proto.version).asLeft[ValueClass])
    }

  def fromByteString(ctx: Ctx)(bytes: ByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(ctx)(VersionedMessage(proto))
  } yield valueClass
}
