// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion.ProtocolVersionWithStatus
import com.digitalasset.canton.{DiscardOps, ProtoDeserializationError, checked}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable
import scala.math.Ordered.orderingToOrdered

trait HasRepresentativeProtocolVersion {
  def representativeProtocolVersion: RepresentativeProtocolVersion[_]
}

/** See [[com.digitalasset.canton.version.HasProtocolVersionedWrapper.representativeProtocolVersion]] for more context */
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

  // As `ValueClass` is a phantom type on `RepresentativeProtocolVersion`,
  // we can have a single Ordering object for all of them here.
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def orderRepresentativeProtocolVersion[ValueClass]
      : Ordering[RepresentativeProtocolVersion[ValueClass]] =
    orderingRepresentativeProtocolVersionInternal
      .asInstanceOf[Ordering[RepresentativeProtocolVersion[ValueClass]]]

  private[this] val orderingRepresentativeProtocolVersionInternal
      : Ordering[RepresentativeProtocolVersion[Any]] =
    Ordering.by(_.representative)

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

  protected def companionObj: HasProtocolVersionedWrapperCompanion[ValueClass, _]

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
            if representativeProtocolVersion >= supportedVersion.fromInclusive =>
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
          if representativeProtocolVersion >= supportedVersion.fromInclusive =>
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

  /** Casts this instance's representative protocol version to one for the target type.
    * This only succeeds if the versioning schemes are the same.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def castRepresentativeProtocolVersion[V, T <: HasSupportedProtoVersions[V]](
      target: T
  ): Either[String, RepresentativeProtocolVersion[V]] = {
    val sourceTable = companionObj.supportedProtoVersions.table
    val targetTable = target.supportedProtoVersions.table

    Either.cond(
      sourceTable == targetTable,
      representativeProtocolVersion.asInstanceOf[RepresentativeProtocolVersion[V]],
      "Source and target versioning schemes should be the same",
    )
  }
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
  protected[this] case class VersionedProtoConverter(
      fromInclusive: RepresentativeProtocolVersion[ValueClass],
      deserializer: Deserializer,
      serializer: Serializer,
  ) extends ProtoCodec
      with PrettyPrinting {
    override val isVersioned = true

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        unnamedParam(_ => HasSupportedProtoVersions.this.getClass.getSimpleName.unquoted),
        param("fromInclusive", _.fromInclusive),
      )
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

    // TODO(#12020) This should be become apply
    def mk[ProtoClass <: scalapb.GeneratedMessage, Status <: ProtocolVersion.Status](
        fromInclusive: ProtocolVersion.ProtocolVersionWithStatus[Status]
    )(
        protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & Status
    )(
        parser: scalapb.GeneratedMessageCompanion[ProtoClass] => Deserializer,
        serializer: Serializer,
    ): VersionedProtoConverter =
      VersionedProtoConverter(fromInclusive, parser(protoCompanion), serializer)
  }

  /** Used to (de)serialize classes which for legacy reasons where not wrapped in VersionedMessage
    * Chances are this is NOT the class you want to use, use VersionedProtoConverter instead when adding serialization
    * to a new class
    */
  protected[this] case class LegacyProtoConverter(
      fromInclusive: RepresentativeProtocolVersion[ValueClass],
      deserializer: Deserializer,
      serializer: Serializer,
  ) extends ProtoCodec
      with PrettyPrinting {
    override val isVersioned = false

    override def pretty: Pretty[this.type] = prettyOfClass(
      unnamedParam(_ => HasSupportedProtoVersions.this.getClass.getSimpleName.unquoted),
      param("fromInclusive", _.fromInclusive),
    )
  }

  object LegacyProtoConverter {
    def apply(
        fromInclusive: ProtocolVersionWithStatus[ProtocolVersion.Stable],
        deserializer: Deserializer,
        serializer: Serializer,
    ): LegacyProtoConverter = LegacyProtoConverter(
      new RepresentativeProtocolVersion[ValueClass](fromInclusive) {},
      deserializer,
      serializer,
    )

    // TODO(#12020) This should be become apply
    def mk[ProtoClass <: scalapb.GeneratedMessage](
        fromInclusive: ProtocolVersion.ProtocolVersionWithStatus[ProtocolVersion.Stable]
    )(
        // A legacy converter should not be used for an unstable Protobuf message.
        protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & ProtocolVersion.Stable
    )(
        parser: scalapb.GeneratedMessageCompanion[ProtoClass] => Deserializer,
        serializer: Serializer,
    ): LegacyProtoConverter =
      LegacyProtoConverter(fromInclusive, parser(protoCompanion), serializer)

  }

  protected def deserializationErrorK(error: ProtoDeserializationError): Deserializer

  protected[this] case class UnsupportedProtoCodec(
      fromInclusive: RepresentativeProtocolVersion[ValueClass]
  ) extends ProtoCodec
      with PrettyPrinting {
    override val isVersioned = false

    private def valueClassName: String = HasSupportedProtoVersions.this.getClass.getSimpleName

    def deserializationError: ProtoDeserializationError = ProtoDeserializationError.OtherError(
      s"Cannot deserialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
    )
    override def deserializer: Deserializer = deserializationErrorK(deserializationError)
    override def serializer: Serializer = throw new UnsupportedOperationException(
      s"Cannot serialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
    )
    override def pretty: Pretty[this.type] = prettyOfClass(
      unnamedParam(_.valueClassName.unquoted),
      param("fromInclusive", _.fromInclusive),
    )
  }

  object UnsupportedProtoCodec {
    def apply(fromInclusive: ProtocolVersion): UnsupportedProtoCodec =
      new UnsupportedProtoCodec(new RepresentativeProtocolVersion[ValueClass](fromInclusive) {})
  }

  case class SupportedProtoVersions private (
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[ProtoVersion, ProtoCodec]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def converterFor(protocolVersion: ProtocolVersion): ProtoCodec = {
      converters
        .collectFirst {
          case (_, converter) if protocolVersion >= converter.fromInclusive.representative =>
            converter
        }
        .getOrElse(higherConverter)
    }

    def deserializerFor(protoVersion: ProtoVersion): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)

    def protoVersionFor(
        protocolVersion: RepresentativeProtocolVersion[ValueClass]
    ): ProtoVersion = converters
      .collectFirst {
        case (protoVersion, converter) if protocolVersion >= converter.fromInclusive =>
          protoVersion
      }
      .getOrElse(higherProtoVersion)

    def protocolVersionRepresentativeFor(
        protoVersion: ProtoVersion
    ): RepresentativeProtocolVersion[ValueClass] =
      table.getOrElse(protoVersion, higherConverter.fromInclusive)

    def protocolVersionRepresentativeFor(
        protocolVersion: ProtocolVersion
    ): RepresentativeProtocolVersion[ValueClass] = converterFor(protocolVersion).fromInclusive

    lazy val table: Map[ProtoVersion, RepresentativeProtocolVersion[ValueClass]] =
      converters.forgetNE.fmap(_.fromInclusive)
  }

  object SupportedProtoVersions {
    def apply(
        head: (ProtoVersion, ProtoCodec),
        tail: (ProtoVersion, ProtoCodec)*
    ): SupportedProtoVersions = SupportedProtoVersions.fromNonEmpty(
      NonEmpty.mk(Seq, head, tail: _*)
    )

    /*
     Throws an error if a protocol version is used twice.
     This indicates an error in the converters list since one protocol version
     cannot correspond to two proto versions.
     */
    private def ensureNoDuplicates(converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec)]]): Unit =
      NonEmpty
        .from {
          converters.forgetNE
            .groupMap { case (_, codec) => codec.fromInclusive.representative } {
              case (protoVersion, _) => protoVersion
            }
            .filter { case (_, protoVersions) => protoVersions.lengthCompare(1) > 0 }
            .toList
        }
        .foreach { duplicates =>
          throw new IllegalArgumentException(
            s"Some protocol versions appear several times in `$name`: $duplicates "
          )
        }
        .discard

    private def fromNonEmpty(
        converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec)]]
    ): SupportedProtoVersions = {
      ensureNoDuplicates(converters)

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

trait HasProtocolVersionedWrapperCompanion[
    ValueClass,
    DeserializedValueClass,
] extends HasSupportedProtoVersions[ValueClass] {

  /** The name of the class as used for pretty-printing and error reporting */
  protected def name: String

  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  protected def deserializeForVersion(
      rpv: RepresentativeProtocolVersion[ValueClass],
      deserializeLegacyProto: Deserializer => ParsingResult[DeserializedValueClass],
      deserializeVersionedProto: => ParsingResult[DeserializedValueClass],
  ): ParsingResult[DeserializedValueClass] = {
    val converter =
      supportedProtoVersions.converterFor(rpv.representative)

    converter match {
      case LegacyProtoConverter(_, deserializer, _) => deserializeLegacyProto(deserializer)
      case _: VersionedProtoConverter => deserializeVersionedProto
      case unsupported: UnsupportedProtoCodec =>
        Left(unsupported.deserializationError)
    }
  }
}

/** Trait for companion objects of serializable classes with memoization.
  * Use this class if deserialization produces a different type than where serialization starts.
  * For example, if a container can serialize its elements, but the container's deserializer
  * does not deserialize the elements and instead leaves them as Bytestring.
  *
  * Use [[HasMemoizedProtocolVersionedWrapperCompanion]] if the type distinction between serialization and deseserialization is not needed.
  */
trait HasMemoizedProtocolVersionedWrapperCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, DeserializedValueClass] {
  // Deserializer: (Proto => DeserializedValueClass)
  override type Deserializer =
    (OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    (original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(_)(original))

  def fromByteArray(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] = fromByteString(
    ByteString.copyFrom(bytes)
  )

  def fromByteString(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = for {
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
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protoVersion),
      _(bytes, bytes),
      fromByteString(bytes),
    )
  }

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): (OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass] =
    (_, _) => Left(error)
}

/** Trait for companion objects of serializable classes with memoization and a (de)serialization context.
  * Use this class if deserialization produces a different type than where serialization starts.
  * For example, if a container can serialize its elements, but the container's deserializer
  * does not deserialize the elements and instead leaves them as Bytestring.
  *
  * Use [[HasMemoizedProtocolVersionedWithContextCompanion]] if the type distinction between serialization and deseserialization is not needed.
  */
trait HasMemoizedProtocolVersionedWithContextCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
    Context,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, DeserializedValueClass] {
  override type Deserializer =
    (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    (ctx: Context, original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _)(original))

  def fromByteString(
      context: Context
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- supportedProtoVersions
      .deserializerFor(ProtoVersion(proto.version))(context, bytes, data)
  } yield valueClass

  def fromByteArray(context: Context)(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] =
    fromByteString(context)(ByteString.copyFrom(bytes))

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass] =
    (_, _, _) => Left(error)
}

/** Trait for companion objects of serializable classes without memoization.
  * Use this class if deserialization produces a different type than where serialization starts.
  * For example, if a container can serialize its elements, but the container's deserializer
  * does not deserialize the elements and instead leaves them as Bytestring.
  *
  * Use [[HasProtocolVersionedCompanion]] if the type distinction between serialization and deseserialization is not needed.
  */
trait HasProtocolVersionedCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, DeserializedValueClass] {
  override type Deserializer = DataByteString => ParsingResult[DeserializedValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[DeserializedValueClass]
  ): Deserializer =
    (data: DataByteString) => ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto)

  def fromByteArray(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParserArray(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def fromProtoVersioned(
      proto: VersionedMessage[DeserializedValueClass]
  ): ParsingResult[DeserializedValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))
    }

  def fromByteString(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = for {
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
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protocolVersion),
      _(bytes),
      fromByteString(bytes),
    )
  }

  def readFromFile(
      inputFile: String
  ): Either[String, DeserializedValueClass] = {
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromByteString(bs).leftMap(_.toString)
    } yield value
  }

  def tryReadFromFile(inputFile: String): DeserializedValueClass =
    readFromFile(inputFile).valueOr(err =>
      throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
    )

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[DeserializedValueClass] = GetResult { r =>
    fromByteArray(r.<<[Array[Byte]]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArray: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[DeserializedValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): DataByteString => ParsingResult[DeserializedValueClass] = _ => Left(error)
}

trait HasProtocolVersionedWithContextCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    Context,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, ValueClass] {
  override type Deserializer = (Context, DataByteString) => ParsingResult[ValueClass]

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

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): (Context, DataByteString) => ParsingResult[ValueClass] = (_, _) => Left(error)
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
