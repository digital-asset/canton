// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.{ProtoDeserializationError, checked}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString

import scala.collection.immutable

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
trait HasProtocolVersionedWrapper[+ProtoClass <: scalapb.GeneratedMessage] {

  /** We have a correspondence {protobuf version} <-> {[protocol version]}: each proto version
    * correspond to a list of consecutive protocol versions. The representative is one instance
    * of this list, usually the smallest value. In other words, the protobuf versions induce an
    * equivalence relation on the list of protocol version, thus use of `representative`.
    *
    * The method `protocolVersionRepresentativeFor` below
    * allows to query the representative for an equivalence class.
    */
  def representativeProtocolVersion: ProtocolVersion

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.MemoizedEvidence]]).
    */
  protected def toProtoVersioned: ProtoClass

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  protected def toByteString: ByteString = toProtoVersioned.toByteString

  /** Yields a byte array representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  protected def toByteArray: Array[Byte] = toByteString.toByteArray

  def getCryptographicEvidence: ByteString
}

trait GenericHasMemoizedProtocolVersionedWrapperCompanion[ValueClass <: HasProtocolVersionedWrapper[
  VersionedMessage[ValueClass]
]] {

  /** The name of the class as used for pretty-printing and error reporting */
  protected def name: String

  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  // Deserializer: (Proto => ValueClass)
  type Deserializer
  // Serializer: (ValueClass => Proto)
  type Serializer = ValueClass => ByteString

  def protocolVersionRepresentativeFor(protocolVersion: ProtocolVersion): ProtocolVersion =
    supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)

  def protocolVersionRepresentativeFor(protoVersion: Int): ProtocolVersion =
    supportedProtoVersions.protocolVersionRepresentativeFor(protoVersion)

  /** Supported protobuf version
    * @param fromInclusive The protocol version when this protobuf version was introduced
    * @param deserializer Deserialization method
    * @param serializer Serialization method
    */
  case class VersionedProtoConverter(
      fromInclusive: ProtocolVersion,
      deserializer: Deserializer,
      serializer: Serializer,
  )

  sealed abstract case class SupportedProtoVersions(
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[Int, VersionedProtoConverter]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def deserializerFor(protoVersion: Int): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)

    def protocolVersionRepresentativeFor(protoVersion: Int): ProtocolVersion =
      converters
        .get(protoVersion)
        .map(_.fromInclusive)
        .getOrElse(higherConverter.fromInclusive)

    def protocolVersionRepresentativeFor(protocolVersion: ProtocolVersion): ProtocolVersion =
      converters
        .collectFirst {
          case (_, supportedVersion) if protocolVersion >= supportedVersion.fromInclusive =>
            supportedVersion.fromInclusive
        }
        .getOrElse(higherConverter.fromInclusive)
  }

  object SupportedProtoVersions {
    def apply(
        head: (Int, VersionedProtoConverter),
        tail: (Int, VersionedProtoConverter)*
    ): SupportedProtoVersions = SupportedProtoVersions(
      NonEmpty.mk(Seq, head, tail: _*)
    )

    def apply(converters: NonEmpty[Seq[(Int, VersionedProtoConverter)]]): SupportedProtoVersions = {
      val sortedConverters = checked(
        NonEmptyUtil.fromUnsafe(
          immutable.SortedMap.from(converters)(implicitly[Ordering[Int]].reverse)
        )
      )
      val (_, lowestProtocolVersion) = sortedConverters.last1

      require(
        lowestProtocolVersion.fromInclusive == ProtocolVersion.minimum_protocol_version,
        s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum_protocol_version}, found $lowestProtocolVersion",
      )

      new SupportedProtoVersions(sortedConverters) {}
    }
  }

  /** Proto versions that are supported by `fromProtoVersioned` and `fromByteString`
    * See the helper `supportedProtoVersion` below to define a `Parser`.
    */
  def supportedProtoVersions: SupportedProtoVersions

  def toProtoVersioned(v: ValueClass): VersionedMessage[ValueClass] =
    supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion)
            if v.representativeProtocolVersion >= supportedVersion.fromInclusive =>
          VersionedMessage(supportedVersion.serializer(v), protoVersion)
      }
      .getOrElse {
        VersionedMessage(
          supportedProtoVersions.higherConverter.serializer(v),
          supportedProtoVersions.higherProtoVersion,
        )
      }
}

trait HasMemoizedProtocolVersionedWrapperCompanion[ValueClass <: HasProtocolVersionedWrapper[
  VersionedMessage[ValueClass]
]] extends GenericHasMemoizedProtocolVersionedWrapperCompanion[ValueClass] {
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
    valueClass <- supportedProtoVersions.deserializerFor(proto.version)(bytes, data)
  } yield valueClass
}

trait HasMemoizedProtocolVersionedWithContextCompanion[ValueClass <: HasProtocolVersionedWrapper[
  VersionedMessage[ValueClass]
], Context]
    extends GenericHasMemoizedProtocolVersionedWrapperCompanion[ValueClass] {
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
    valueClass <- supportedProtoVersions.deserializerFor(proto.version)(context, bytes, data)
  } yield valueClass
}
