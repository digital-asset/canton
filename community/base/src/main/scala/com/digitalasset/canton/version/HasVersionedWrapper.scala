// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import cats.{Id, Monad}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{ProtoDeserializationError, checked}
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import slick.jdbc.{GetResult, SetParameter}

import java.io.{InputStream, OutputStream}
import scala.collection.immutable
import scala.util.Try
import scala.util.control.NonFatal

/** Trait for classes that can be serialized by using ProtoBuf. See "CONTRIBUTING.md" for our
  * guidelines on serialization.
  *
  * This wrapper is to be used if a single instance needs to be serialized to different proto
  * versions.
  *
  * The underlying ProtoClass is [[com.digitalasset.canton.version.v1.UntypedVersionedMessage]] but
  * we often specify the typed alias [[com.digitalasset.canton.version.VersionedMessage]] instead.
  *
  * @tparam F
  *   Typically Id (for classes whose serialization always succeeds) or Either[String, ?] if
  *   serialization can fail (e.g., because the instance cannot be serialized to the specified
  *   protocol version).
  */
// In the versioning framework, such calls are legitimate
@SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
trait HasVersionedWrapperF[F[_], ValueClass] extends HasVersionedToByteStringF[F] {
  self: ValueClass =>

  implicit def monadF: Monad[F]

  protected def companionObj: HasVersionedMessageCompanionCommonF[F, ValueClass]

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto
    * serializations. Keep it protected, if there are good reasons for it (e.g.
    * [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    */
  def toProtoVersioned(version: ProtocolVersion): F[VersionedMessage[ValueClass]] =
    companionObj.supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion) if version >= supportedVersion.fromInclusive =>
          toProtoVersioned(supportedVersion.serializer, protoVersion)
      }
      .getOrElse(serializeToHighestVersion)

  private def toProtoVersioned(
      serializer: ValueClass => F[scalapb.GeneratedMessage],
      protoVersion: ProtoVersion,
  ): F[VersionedMessage[ValueClass]] =
    monadF.map[scalapb.GeneratedMessage, VersionedMessage[ValueClass]](
      serializer(self)
    )(proto => VersionedMessage(proto.toByteString, protoVersion.v))

  private def serializeToHighestVersion: F[VersionedMessage[ValueClass]] =
    toProtoVersioned(
      companionObj.supportedProtoVersions.higherConverter.serializer,
      companionObj.supportedProtoVersions.higherProtoVersion,
    )

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of
    * this instance.
    */
  override def toByteString(version: ProtocolVersion): F[ByteString] = monadF.map(
    toProtoVersioned(version)
  )(_.toByteString)

  /** Yields a byte array representation of the corresponding `UntypedVersionedMessage` wrapper of
    * this instance.
    */
  def toByteArray(version: ProtocolVersion): F[Array[Byte]] =
    monadF.map(toByteString(version))(_.toByteArray)

  /** Serializes this instance to a message together with a delimiter (the message length) to the
    * given output stream.
    *
    * This method works in conjunction with parseDelimitedFromTrusted which deserializes the message
    * again. It is useful for serializing multiple messages to a single output stream through
    * multiple invocations.
    *
    * @param output
    *   the sink to which this message is serialized to
    * @return
    *   an Either where left represents an error message, and right represents a successful message
    *   serialization
    */
  def writeDelimitedTo(pv: ProtocolVersion, output: OutputStream): Either[String, Unit]

  /** Writes the byte string representation of the corresponding `UntypedVersionedMessage` wrapper
    * of this instance to a file.
    */
  def writeToFile(
      outputFile: String,
      version: ProtocolVersion,
  ): F[Unit] =
    monadF.map(toByteString(version))(BinaryFileUtil.writeByteStringToFile(outputFile, _))
}

trait HasVersionedWrapper[ValueClass] extends HasVersionedWrapperF[Id, ValueClass] {
  self: ValueClass =>

  override implicit val monadF: Monad[Id] = cats.catsInstancesForId

  override def writeDelimitedTo(pv: ProtocolVersion, output: OutputStream): Either[String, Unit] = {
    val message = toProtoVersioned(pv)

    Try(message.writeDelimitedTo(output)).toEither.leftMap(e =>
      s"Cannot serialize ${companionObj.name} into the given output stream due to: ${e.getMessage}"
    )
  }
}

trait HasVersionedWrapperE[ValueClass] extends HasVersionedWrapperF[Either[String, *], ValueClass] {
  self: ValueClass =>

  override implicit val monadF: Monad[Either[String, *]] =
    cats.instances.either.catsStdInstancesForEither

  override def writeDelimitedTo(pv: ProtocolVersion, output: OutputStream): Either[String, Unit] = {

    val message = toProtoVersioned(pv)

    Try(message.map(_.writeDelimitedTo(output))).toEither
      .leftMap(_.getMessage)
      .flatten
      .leftMap(e =>
        s"Cannot serialize ${companionObj.name} into the given output stream due to: $e"
      )
  }
}

// Implements shared behavior of [[HasVersionedMessageCompanion]] and [[HasVersionedMessageWithContextCompanion]]
trait HasVersionedMessageCompanionCommonF[F[_], ValueClass] {

  /** The name of the class as used for pretty-printing and error reporting */
  def name: String

  type Deserializer

  /** Proto versions that are supported by `fromProtoVersioned`, `fromByteString`,
    * `toProtoVersioned` and `toByteString`. See the helpers `supportedProtoVersion` and
    * `supportedProtoVersionMemoized` below to define a `ProtoCodec`.
    */
  def supportedProtoVersions: SupportedProtoVersions

  case class ProtoCodec(
      fromInclusive: ProtocolVersion,
      deserializer: Deserializer,
      serializer: ValueClass => F[scalapb.GeneratedMessage],
  )

  case class SupportedProtoVersions private (
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[ProtoVersion, ProtoCodec]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def converterFor(protocolVersion: ProtocolVersion): ProtoCodec =
      converters
        .collectFirst {
          case (_, converter) if protocolVersion >= converter.fromInclusive =>
            converter
        }
        .getOrElse(higherConverter)

    def deserializerFor(protoVersion: ProtoVersion): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)
  }

  object SupportedProtoVersions {
    def apply(
        head: (ProtoVersion, ProtoCodec),
        tail: (ProtoVersion, ProtoCodec)*
    ): SupportedProtoVersions = SupportedProtoVersions.fromNonEmpty(
      NonEmpty.mk(Seq, head, tail*)
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
            .groupMap { case (_, codec) => codec.fromInclusive } { case (protoVersion, _) =>
              protoVersion
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
        lowestProtocolVersion.fromInclusive == ProtocolVersion.minimum,
        s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum}, found ${lowestProtocolVersion.fromInclusive}",
      )

      SupportedProtoVersions(sortedConverters)
    }
  }
}

/** Traits for the companion objects of classes that implement [[HasVersionedWrapper]]. Provide
  * default methods.
  */
trait HasVersionedMessageCompanionF[F[_], ValueClass]
    extends HasVersionedMessageCompanionCommonF[F, ValueClass] {
  type Deserializer = ByteString => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[ValueClass]
  ): ByteString => ParsingResult[ValueClass] =
    ProtoConverter.protoParser(p.parseFrom)(_).flatMap(fromProto)

  def fromProtoVersioned(
      proto: VersionedMessage[ValueClass]
  ): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      data => supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))(data)
    }

  /** The embedded version is not validated */
  def fromTrustedByteString(bytes: ByteString): ParsingResult[ValueClass] =
    for { // no input validation for proto version
      proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromProtoVersioned(VersionedMessage(proto))
    } yield valueClass

  def fromTrustedByteArray(bytes: Array[Byte]): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParserArray(v1.UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def readFromTrustedFile(
      inputFile: String
  ): Either[String, ValueClass] =
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromTrustedByteString(bs).leftMap(_.toString)
    } yield value

  def tryReadFromTrustedFile(inputFile: String): ValueClass =
    readFromTrustedFile(inputFile).valueOr(err =>
      throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
    )

  /** Deserializes a message using a delimiter (the message length) from the given input stream.
    *
    * '''Unsafe!''' No deserialization validation is performed.
    *
    * Do NOT use this method unless you can justify that the given bytes originate from a trusted
    * source.
    *
    * This method works in conjunction with
    * [[com.digitalasset.canton.version.HasVersionedWrapper.writeDelimitedTo]] which should have
    * been used to serialize the message. It is useful for deserializing multiple messages from a
    * single input stream through repeated invocations.
    *
    * @param input
    *   the source from which a message is deserialized
    * @return
    *   an Option that is None when there are no messages left anymore, otherwise it wraps an Either
    *   where left represents a deserialization error (exception) and right represents the
    *   successfully deserialized message
    */
  def parseDelimitedFromTrusted(
      input: InputStream
  ): Option[ParsingResult[ValueClass]] = {
    def fromTrustedProtoVersioned(
        proto: VersionedMessage[ValueClass]
    ): ParsingResult[ValueClass] =
      proto.wrapper.data
        .toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
        .flatMap(supportedProtoVersions.deserializerFor(ProtoVersion(proto.version)))

    try {
      v1.UntypedVersionedMessage
        .parseDelimitedFrom(input)
        .map(VersionedMessage[ValueClass])
        .map(fromTrustedProtoVersioned)
    } catch {
      case protoBuffException: InvalidProtocolBufferException =>
        Some(Left(ProtoDeserializationError.BufferException(protoBuffException)))
      case NonFatal(e) =>
        Some(Left(ProtoDeserializationError.OtherError(e.getMessage)))
    }
  }

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[ValueClass] = GetResult { r =>
    fromTrustedByteArray(r.<<[Array[Byte]]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[ValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromTrustedByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }
}

trait HasVersionedMessageCompanionDbHelpers[
    ValueClass <: HasVersionedWrapper[ValueClass]
] {
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

/** Traits for the companion objects of classes that implement [[HasVersionedWrapper]]. They provide
  * default methods. Unlike [[HasVersionedMessageCompanion]] these traits allow to pass additional
  * context to the conversion methods.
  */
trait HasVersionedMessageWithContextCompanionF[F[_], ValueClass, Ctx]
    extends HasVersionedMessageCompanionCommonF[F, ValueClass] {
  type Deserializer = (Ctx, ByteString) => ParsingResult[ValueClass]

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
      data => supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))(ctx, data)
    }

  /** The embedded version is not validated */
  def fromTrustedByteString(ctx: Ctx)(bytes: ByteString): ParsingResult[ValueClass] =
    for { // no input validation for proto version
      proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromProtoVersioned(ctx)(VersionedMessage(proto))
    } yield valueClass
}
