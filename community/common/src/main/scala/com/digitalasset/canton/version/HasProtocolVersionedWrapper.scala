// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{ProtoDeserializationError, checked}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable

trait HasRepresentativeProtocolVersion {
  def representativeProtocolVersion: RepresentativeProtocolVersion[_]
}

/** See method `representativeProtocolVersion` below for more context */
sealed abstract case class RepresentativeProtocolVersion[+ValueClass](
    private val v: ProtocolVersion
) extends PrettyPrinting {

  /** When using this method, keep in mind that for a given companion object `C` that implements
    * `HasProtocolVersionedWrapperCompanion` and for a protocol version `pv`, then
    * `C.protocolVersionRepresentativeFor(pv).representative` is different than `pv`.
    * In particular, do not use a representative for a given class to construct a representative
    * for another class.
    */
  def representative: ProtocolVersion = v

  override def pretty: Pretty[RepresentativeProtocolVersion[_]] = prettyOfParam(_.v)
}

object RepresentativeProtocolVersion {

  implicit val setParameterRepresentativeProtocolVersion
      : SetParameter[RepresentativeProtocolVersion[_]] =
    (rpv: RepresentativeProtocolVersion[_], pp: PositionedParameters) => pp >> rpv.v

}

final case class ProtoVersion(v: Int) extends AnyVal

object ProtoVersion {
  implicit val protoVersionOrdering: Ordering[ProtoVersion] =
    Ordering.by[ProtoVersion, Int](_.v)
}

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  *
  * This wrapper is to be used when every instance can be tied to a single protocol version.
  * Consequently, some attributes of the class may depend on the protocol version (e.g., the signature).
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

  /** We have a correspondence {Proto version} <-> {[protocol version]}: each proto version
    * correspond to a list of consecutive protocol versions. The representative is one instance
    * of this list, usually the smallest value. In other words, the Proto versions induce an
    * equivalence relation on the list of protocol version, thus use of `representative`.
    *
    * The method `protocolVersionRepresentativeFor` below
    * allows to query the representative for an equivalence class.
    */
  def representativeProtocolVersion: RepresentativeProtocolVersion[ValueClass]

  private def serializeToHighestVersion: VersionedMessage[ValueClass] = {
    VersionedMessage(
      companionObj.supportedProtoVersions.higherConverter.serializer(self),
      companionObj.supportedProtoVersions.higherProtoVersion.v,
    )
  }

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    *
    * Be aware that if calling on a class that defines a LegacyProtoConverter, this method will still
    * return a VersionedMessage. If the current protocol version maps to the
    * legacy converter, deserialization will then fail (as it will try to deserialize to the raw protobuf instead of the
    * VersionedMessage wrapper this was serialized to.
    * Prefer using toByteString which handles this use case correctly.
    */
  def toProtoVersioned: VersionedMessage[ValueClass] =
    companionObj.supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion)
            if representativeProtocolVersion.representative >= supportedVersion.fromInclusive.representative =>
          VersionedMessage(supportedVersion.serializer(self), protoVersion.v)
      }
      .getOrElse(serializeToHighestVersion)

  /** Yields the Proto version that this class will be serialized to
    */
  def protoVersion: ProtoVersion =
    companionObj.protoVersionFor(representativeProtocolVersion)

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteString: ByteString = companionObj.supportedProtoVersions.converters
    .collectFirst {
      case (protoVersion, supportedVersion)
          if representativeProtocolVersion.representative >= supportedVersion.fromInclusive.representative =>
        supportedVersion match {
          case versioned if versioned.isVersioned =>
            VersionedMessage(supportedVersion.serializer(self), protoVersion.v).toByteString
          case legacy =>
            legacy.serializer(self)
        }
    }
    .getOrElse(serializeToHighestVersion.toByteString)

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
      protoVersion: ProtoVersion
  ): RepresentativeProtocolVersion[ValueClass] =
    supportedProtoVersions.protocolVersionRepresentativeFor(protoVersion)

  /** Return the Proto version corresponding to the representative protocol version
    */
  def protoVersionFor(
      protocolVersion: RepresentativeProtocolVersion[ValueClass]
  ): ProtoVersion = supportedProtoVersions.protoVersionFor(protocolVersion)

  /** Return the Proto version corresponding to the protocol version
    */
  def protoVersionFor(protocolVersion: ProtocolVersion): ProtoVersion =
    supportedProtoVersions.protoVersionFor(protocolVersionRepresentativeFor(protocolVersion))

  /** Base class for (de)serializating from/to protobuf of ValueClass from a specific PV
    */
  sealed trait ProtoCodec {
    def fromInclusive: RepresentativeProtocolVersion[ValueClass]
    def deserializer: Deserializer
    def serializer: Serializer
    // Can't always rely on the subtype to differentiate between instances of ProtoCodec, because the type is erased
    // at compile time when it is a dependent type of ValueClass (e.g in HasProtocolVersionedWrapper).
    // Instead use this method to differentiate between versioned and un-versioned serialization
    def isVersioned: Boolean
  }

  /** Supported Proto version
    * @param fromInclusive The protocol version when this Proto version was introduced
    * @param deserializer Deserialization method
    * @param serializer Serialization method
    */
  case class VersionedProtoConverter(
      fromInclusive: RepresentativeProtocolVersion[ValueClass],
      deserializer: Deserializer,
      serializer: Serializer,
  ) extends ProtoCodec {
    override val isVersioned = true
  }

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

  /** Used to (de)serialize classes which for legacy reasons where not wrapped in VersionedMessage
    * Chances are this is NOT the class you want to use, use VersionedProtoConverter instead when adding serialization
    * to a new class
    */
  case class LegacyProtoConverter(
      fromInclusive: RepresentativeProtocolVersion[ValueClass],
      deserializer: Deserializer,
      serializer: Serializer,
  ) extends ProtoCodec {
    override val isVersioned = false
  }

  object LegacyProtoConverter {
    def apply(
        fromInclusive: ProtocolVersion,
        deserializer: Deserializer,
        serializer: Serializer,
    ): LegacyProtoConverter = LegacyProtoConverter(
      new RepresentativeProtocolVersion[ValueClass](fromInclusive) {},
      deserializer,
      serializer,
    )
  }

  case class SupportedProtoVersions private (
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[ProtoVersion, ProtoCodec]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def protoVersionsCount: Int = converters.size

    def converterFor(protocolVersion: ProtocolVersion): ProtoCodec =
      converters
        .collectFirst {
          case (_, converter) if protocolVersion >= converter.fromInclusive.representative =>
            converter
        }
        .getOrElse(higherConverter)

    def deserializerFor(protoVersion: ProtoVersion): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)

    def protoVersionFor(
        protocolVersion: RepresentativeProtocolVersion[ValueClass]
    ): ProtoVersion = converters
      .collectFirst {
        case (protoVersion, converter)
            if protocolVersion.representative >= converter.fromInclusive.representative =>
          protoVersion
      }
      .getOrElse(higherProtoVersion)

    def protocolVersionRepresentativeFor(
        protoVersion: ProtoVersion
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
        head: (ProtoVersion, ProtoCodec),
        tail: (ProtoVersion, ProtoCodec)*
    ): SupportedProtoVersions = SupportedProtoVersions.fromNonEmpty(
      NonEmpty.mk(Seq, head, tail: _*)
    )

    def fromNonEmpty(
        converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec)]]
    ): SupportedProtoVersions = {

      val sortedConverters = checked(
        NonEmptyUtil.fromUnsafe(
          immutable.SortedMap.from(converters)(implicitly[Ordering[ProtoVersion]].reverse)
        )
      )
      val (_, lowestProtocolVersion) = sortedConverters.last1

      require(
        lowestProtocolVersion.fromInclusive.representative == ProtocolVersion.minimum,
        s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum}, found $lowestProtocolVersion",
      )

      SupportedProtoVersions(sortedConverters)
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

  protected def deserializeForVersion(
      rpv: RepresentativeProtocolVersion[ValueClass],
      deserializeLegacyProto: Deserializer => ParsingResult[ValueClass],
      deserializeVersionedProto: => ParsingResult[ValueClass],
  ): ParsingResult[ValueClass] = {
    val converter =
      supportedProtoVersions.converterFor(rpv.representative)

    converter match {
      case LegacyProtoConverter(_, deserializer, _) => deserializeLegacyProto(deserializer)
      case _: VersionedProtoConverter => deserializeVersionedProto
    }
  }
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

  def fromByteArray(bytes: Array[Byte]): ParsingResult[ValueClass] = fromByteString(
    ByteString.copyFrom(bytes)
  )

  def fromByteString(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- supportedProtoVersions
      .deserializerFor(ProtoVersion(proto.version))(bytes, data)
  } yield valueClass

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the version to use for the deserialization.
    * @param protoVersion Proto version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protoVersion: ProtoVersion
  )(bytes: OriginalByteString): ParsingResult[ValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protoVersion),
      _(bytes, bytes),
      fromByteString(bytes),
    )
  }
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
      .deserializerFor(ProtoVersion(proto.version))(context, bytes, data)
  } yield valueClass

  def fromByteArray(context: Context)(bytes: Array[Byte]): ParsingResult[ValueClass] =
    fromByteString(context)(ByteString.copyFrom(bytes))
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
      supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))
    }

  def fromByteString(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the version to use for the deserialization.
    * @param protocolVersion protocol version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protocolVersion: ProtocolVersion
  )(bytes: OriginalByteString): ParsingResult[ValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protocolVersion),
      _(bytes),
      fromByteString(bytes),
    )
  }

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
      getResultByteArray: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[ValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
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
      supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))(context, _)
    }

  def fromByteString(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(context)(VersionedMessage(proto))
  } yield valueClass

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the Proto version to use for the deserialization.
    * @param protoVersion Proto version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protoVersion: ProtoVersion
  )(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protoVersion),
      _(context, bytes),
      fromByteString(context)(bytes),
    )
  }

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the protocol version to use for the deserialization.
    * @param protocolVersion protocol version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protocolVersion: ProtocolVersion
  )(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protocolVersion),
      _(context, bytes),
      fromByteString(context)(bytes),
    )
  }
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

  def getVersionedSetParameterO(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[ValueClass]] = (valueO, pp) => pp >> valueO.map(_.toByteArray)
}
