// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.either._
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

/** Trait for classes that have a corresponding Protobuf definition but **don't** need the ability to be directly serialized
  * into a ByteString or ByteArray.
  * Commonly, this interface should be used when the corresponding Protobuf message is only used in other Protobuf
  * messages or for gRPC calls and responses, but never sent around or stored as a bytestring.
  * E.g. an `Envelope` is always embedded in a `Batch`, and therefore doesn't need a serialization method itself because
  * it will be indirectly serialized when the enclosing Batch is serialized.
  */
trait HasProtoV0[ProtoClass <: scalapb.GeneratedMessage] {

  /** Yields the proto representation of the class.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.MemoizedEvidence]]).
    */
  protected def toProtoV0: ProtoClass
}

/** Same as [[HasProtoV0]] but `toProtoV0` takes a version argument.This trait generally only be used in rare cases
  * when a Protobuf message contains a nested `Versioned...` wrapper - see e.g. Batch and Envelope
  */
trait HasProtoV0WithVersion[ProtoClass <: scalapb.GeneratedMessage] {

  /** Yields the proto representation of the class.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.MemoizedEvidence]]).
    */
  protected def toProtoV0(version: ProtocolVersion): ProtoClass
}

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  */
trait HasVersionedWrapper[ProtoClass <: scalapb.GeneratedMessage] extends HasVersionedToByteString {

  /** Yields the proto representation of the class inside a `Versioned...` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.MemoizedEvidence]]).
    */
  protected def toProtoVersioned(version: ProtocolVersion): ProtoClass

  /** Yields a byte string representation of the corresponding `Versioned...` wrapper of this instance.
    */
  override def toByteString(version: ProtocolVersion): ByteString = toProtoVersioned(
    version
  ).toByteString

  /** Yields a byte array representation of the corresponding `Versioned...` wrapper of this instance.
    */
  def toByteArray(version: ProtocolVersion): Array[Byte] = toByteString(version).toByteArray

  /** Writes the byte string representation of the corresponding `Versioned...` wrapper of this instance to a file. */
  def writeToFile(outputFile: String, version: ProtocolVersion = ProtocolVersion.latest): Unit = {
    val bytes = toByteString(version)
    BinaryFileUtil.writeByteStringToFile(outputFile, bytes)
  }
}

/** Trait for the companion objects of classes that implement [[HasVersionedWrapper]].
  * Provides default methods
  */
trait HasVersionedWrapperCompanion[
    ProtoClass <: scalapb.GeneratedMessage,
    ValueClass <: HasVersionedWrapper[ProtoClass],
] {

  /** The companion object of the generated ProtoBuf class */
  protected def ProtoClassCompanion: scalapb.GeneratedMessageCompanion[ProtoClass]

  /** The name of the class as used for pretty-printing */
  protected def name: String

  protected def fromProtoVersioned(proto: ProtoClass): ParsingResult[ValueClass]

  def fromByteString(bytes: ByteString): ParsingResult[ValueClass] =
    ProtoConverter.protoParser(ProtoClassCompanion.parseFrom)(bytes).flatMap(fromProtoVersioned)

  def tryFromByteString(bytes: ByteString): ValueClass =
    fromByteString(bytes).valueOr(err =>
      throw new IllegalArgumentException(s"Deserializing $name bytestring failed: $err")
    )

  def fromByteArray(bytes: Array[Byte]): ParsingResult[ValueClass] =
    ProtoConverter
      .protoParserArray(ProtoClassCompanion.parseFrom)(bytes)
      .flatMap(fromProtoVersioned)

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

  implicit def hasVersionedWrapperSetParameter(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[ValueClass] = { (value, pp) =>
    pp >> value.toByteArray(ProtocolVersion.v2_0_0_Todo_i8793)
  }

  implicit def hasVersionedWrapperSetParameterO(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[ValueClass]] =
    (valueO, pp) => pp >> valueO.map(_.toByteArray(ProtocolVersion.v2_0_0_Todo_i8793))
}
