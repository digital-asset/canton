// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either._
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{ProtoDeserializationError, checked}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.immutable

trait HasRepresentativeProtocolVersion {
  def representativeProtocolVersion: RepresentativeProtocolVersion[_]
}

/** See method `representativeProtocolVersion` below for more context */
sealed abstract case class RepresentativeProtocolVersion[+ValueClass](
    private val v: ProtocolVersion
) {

  /** When using this method, keep in mind that for a given companion object `C` that implements
    * `HasProtocolVersionedWrapperCompanion` and for a protocol version `pv`, then
    * `C.protocolVersionRepresentativeFor(pv).representative` is different than `pv`.
    * In particular, do not use a representative for a given class to construct a representative
    * for another class.
    */
  def representative: ProtocolVersion = v
}

final case class ProtobufVersion(v: Int) extends AnyVal

object ProtobufVersion {
  implicit val protobufVersionOrdering: Ordering[ProtobufVersion] =
    Ordering.by[ProtobufVersion, Int](_.v)
}

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  *
  * This version of the wrapper is to be used when some attributes of the class
  * depend on the protocol version (e.g., the signature).
  * The protocol version is then bundled with the instance and does not need to
  * be passed to the toProtoVersioned, toByteString and getCryptographicEvidence
  * methods.
  *
  * The underlying ProtoClass is [[com.digitalasset.canton.version.UntypedVersionedMessage]]
  * but we often specify the typed alias [[com.digitalasset.canton.version.VersionedMessage]]
  * instead.
  */
trait HasProtocolVersionedWrapper[ValueClass] extends HasRepresentativeProtocolVersion {
  self: ValueClass =>

  protected def companionObj: HasProtocolVersionedWrapperCompanion[ValueClass]

  def isEquivalentTo(protocolVersion: ProtocolVersion): Boolean =
    companionObj.protocolVersionRepresentativeFor(protocolVersion) == representativeProtocolVersion

  /** We have a correspondence {protobuf version} <-> {[protocol version]}: each proto version
    * correspond to a list of consecutive protocol versions. The representative is one instance
    * of this list, usually the smallest value. In other words, the protobuf versions induce an
    * equivalence relation on the list of protocol version, thus use of `representative`.
    *
    * The method `protocolVersionRepresentativeFor` below
    * allows to query the representative for an equivalence class.
    */
  def representativeProtocolVersion: RepresentativeProtocolVersion[ValueClass]

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    */
  def toProtoVersioned: VersionedMessage[ValueClass] =
    companionObj.supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion)
            if representativeProtocolVersion.representative >= supportedVersion.fromInclusive.representative =>
          VersionedMessage(supportedVersion.serializer(self), protoVersion.v)
      }
      .getOrElse {
        VersionedMessage(
          companionObj.supportedProtoVersions.higherConverter.serializer(self),
          companionObj.supportedProtoVersions.higherProtoVersion.v,
        )
      }

  /** Yields the Protobuf version that this class will be serialized to
    */
  def protobufVersion: ProtobufVersion =
    companionObj.protobufVersionFor(representativeProtocolVersion)

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteString: ByteString = toProtoVersioned.toByteString

  /** Yields a byte array representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteArray: Array[Byte] = toByteString.toByteArray

  def writeToFile(outputFile: String): Unit =
    BinaryFileUtil.writeByteStringToFile(outputFile, toByteString)
}

/** This trait has the logic to store proto (de)serializers and retrieve them by protocol version.
  * @tparam ValueClass
  */
trait HasSupportedProtoVersions[ValueClass] {

  /** The name of the class as used for pretty-printing and error reporting */
  protected def name: String

  // Deserializer: (Proto => ValueClass)
  type Deserializer
  // Serializer: (ValueClass => Proto)
  type Serializer = ValueClass => ByteString

  def protocolVersionRepresentativeFor(
      protocolVersion: ProtocolVersion
  ): RepresentativeProtocolVersion[ValueClass] =
    supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)

  def protocolVersionRepresentativeFor(
      protoVersion: ProtobufVersion
  ): RepresentativeProtocolVersion[ValueClass] =
    supportedProtoVersions.protocolVersionRepresentativeFor(protoVersion)

  /** Return the Protobuf version corresponding to the representative protocol version
    */
  def protobufVersionFor(
      protocolVersion: RepresentativeProtocolVersion[ValueClass]
  ): ProtobufVersion =
    supportedProtoVersions.protobufVersionFor(protocolVersion)

  /** Supported protobuf version
    * @param fromInclusive The protocol version when this protobuf version was introduced
    * @param deserializer Deserialization method
    * @param serializer Serialization method
    */
  case class VersionedProtoConverter(
      fromInclusive: RepresentativeProtocolVersion[ValueClass],
      deserializer: Deserializer,
      serializer: Serializer,
  )

  object VersionedProtoConverter {
    def apply(
        fromInclusive: ProtocolVersion,
        deserializer: Deserializer,
        serializer: Serializer,
    ): VersionedProtoConverter = VersionedProtoConverter(
      new RepresentativeProtocolVersion[ValueClass](fromInclusive) {},
      deserializer,
      serializer,
    )
  }

  sealed abstract case class SupportedProtoVersions(
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[ProtobufVersion, VersionedProtoConverter]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def protobufVersionsCount: Int = converters.size

    def converterFor(protocolVersion: ProtocolVersion): VersionedProtoConverter =
      converters
        .collectFirst {
          case (_, converter) if protocolVersion >= converter.fromInclusive.representative =>
            converter
        }
        .getOrElse(higherConverter)

    def deserializerFor(protoVersion: ProtobufVersion): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)

    def deserializerFor(protocolVersion: ProtocolVersion): Deserializer = converterFor(
      protocolVersion
    ).deserializer

    def serializerFor(protocolVersion: RepresentativeProtocolVersion[ValueClass]): Serializer =
      converterFor(protocolVersion.representative).serializer

    def protobufVersionFor(
        protocolVersion: RepresentativeProtocolVersion[ValueClass]
    ): ProtobufVersion = converters
      .collectFirst {
        case (protobufVersion, converter)
            if protocolVersion.representative >= converter.fromInclusive.representative =>
          protobufVersion
      }
      .getOrElse(higherProtoVersion)

    def protocolVersionRepresentativeFor(
        protoVersion: ProtobufVersion
    ): RepresentativeProtocolVersion[ValueClass] =
      converters
        .get(protoVersion)
        .map(_.fromInclusive)
        .getOrElse(higherConverter.fromInclusive)

    def protocolVersionRepresentativeFor(
        protocolVersion: ProtocolVersion
    ): RepresentativeProtocolVersion[ValueClass] = converterFor(protocolVersion).fromInclusive
  }

  object SupportedProtoVersions {
    def apply(
        head: (ProtobufVersion, VersionedProtoConverter),
        tail: (ProtobufVersion, VersionedProtoConverter)*
    ): SupportedProtoVersions = SupportedProtoVersions(
      NonEmpty.mk(Seq, head, tail: _*)
    )

    def apply(
        converters: NonEmpty[Seq[(ProtobufVersion, VersionedProtoConverter)]]
    ): SupportedProtoVersions = {

      val sortedConverters = checked(
        NonEmptyUtil.fromUnsafe(
          immutable.SortedMap.from(converters)(implicitly[Ordering[ProtobufVersion]].reverse)
        )
      )
      val (_, lowestProtocolVersion) = sortedConverters.last1

      require(
        lowestProtocolVersion.fromInclusive.representative == ProtocolVersion.minimum_protocol_version,
        s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum_protocol_version}, found $lowestProtocolVersion",
      )

      new SupportedProtoVersions(sortedConverters) {}
    }
  }

  /** Proto versions that are supported by `fromProtoVersioned` and `fromByteString`
    * See the helper `supportedProtoVersion` below to define a `Parser`.
    */
  def supportedProtoVersions: SupportedProtoVersions
}

sealed trait HasProtocolVersionedWrapperCompanion[
    ValueClass
] extends HasSupportedProtoVersions[ValueClass] {

  /** The name of the class as used for pretty-printing and error reporting */
  protected def name: String

  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  // Deserializer: (Proto => ValueClass)
  type Deserializer
}

trait HasMemoizedProtocolVersionedWrapperCompanion[ValueClass <: HasRepresentativeProtocolVersion]
    extends HasProtocolVersionedWrapperCompanion[ValueClass] {
  // Deserializer: (Proto => ValueClass)
  type Deserializer = (OriginalByteString, DataByteString) => ParsingResult[ValueClass]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => (OriginalByteString => ParsingResult[ValueClass])
  ): Deserializer =
    (original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(_)(original))

  def fromByteString(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- supportedProtoVersions
      .deserializerFor(ProtobufVersion(proto.version))(bytes, data)
  } yield valueClass
}

trait HasMemoizedProtocolVersionedWithContextCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    Context,
] extends HasProtocolVersionedWrapperCompanion[ValueClass] {
  type Deserializer = (Context, OriginalByteString, DataByteString) => ParsingResult[ValueClass]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => (OriginalByteString => ParsingResult[ValueClass])
  ): Deserializer =
    (ctx: Context, original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _)(original))

  def fromByteString(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- supportedProtoVersions
      .deserializerFor(ProtobufVersion(proto.version))(context, bytes, data)
  } yield valueClass
}

trait HasProtocolVersionedCompanion[
    ValueClass <: HasRepresentativeProtocolVersion
] extends HasProtocolVersionedWrapperCompanion[ValueClass] {
  type Deserializer = DataByteString => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[ValueClass]
  ): Deserializer =
    (data: DataByteString) => ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto)

  def fromByteArray(bytes: Array[Byte]): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParserArray(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def fromProtoVersioned(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      supportedProtoVersions.deserializerFor(ProtobufVersion(proto.version))
    }

  def fromByteString(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
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
}

trait HasProtocolVersionedWithContextCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    Context,
] extends HasProtocolVersionedWrapperCompanion[ValueClass] {
  type Deserializer = (Context, DataByteString) => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => ParsingResult[ValueClass]
  ): Deserializer =
    (ctx: Context, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _))

  def fromProtoVersioned(
      context: Context
  )(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      supportedProtoVersions.deserializerFor(ProtobufVersion(proto.version))(context, _)
    }

  def fromByteString(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(context)(VersionedMessage(proto))
  } yield valueClass

}

trait HasProtocolVersionedSerializerCompanion[ValueClass <: HasRepresentativeProtocolVersion]
    extends HasProtocolVersionedWrapperCompanion[ValueClass] {
  type Deserializer = Unit
}

trait ProtocolVersionedCompanionDbHelpers[ValueClass <: HasProtocolVersionedWrapper[ValueClass]] {
  def getVersionedSetParameter(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[ValueClass] = { (value, pp) =>
    pp >> value.toByteArray
  }
}
