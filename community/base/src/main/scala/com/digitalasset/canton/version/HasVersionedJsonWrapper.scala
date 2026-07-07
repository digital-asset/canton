// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import cats.{Functor, Id}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import org.json4s.jackson.JsonMethods.*
import org.json4s.{JValue, StringInput}
import scalapb.GeneratedMessageCompanion
import scalapb.json4s.JsonFormat
import slick.jdbc.{GetResult, SetParameter}

/** Trait for classes that can be serialized to JSON via scalapb's ProtoBuf to JSON serialization
  * mechanism. See "contributing/how-to-choose-BaseVersioningCompanion.md" for our guidelines on
  * serialization.
  *
  * This wrapper is to be used if a single instance needs to be serialized to different proto
  * versions.
  *
  * The underlying class is [[com.digitalasset.canton.version.UntypedVersionedJsonMessage]] but we
  * often specify the typed alias [[com.digitalasset.canton.version.VersionedJsonMessage]] instead.
  *
  * @tparam F
  *   Typically Id (for classes whose serialization always succeeds) or Either[String, ?] if
  *   serialization can fail (e.g., because the instance cannot be serialized to the specified
  *   protocol version).
  */
trait HasVersionedJsonWrapperF[F[_], ValueClass] {
  self: ValueClass =>

  protected def functorF: Functor[F]

  protected def companionObj: HasVersionedMessageCompanionCommonF[F, ValueClass]

  /** Yields the proto representation of the class inside an
    * [[com.digitalasset.canton.version.UntypedVersionedJsonMessage]] wrapper.
    */
  def toProtoVersioned(version: ProtocolVersion): F[VersionedJsonMessage[ValueClass]] =
    companionObj.supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion) if version >= supportedVersion.fromInclusive =>
          toProtoVersioned(supportedVersion.serializer, protoVersion)
      }
      .getOrElse(serializeToHighestVersion)

  private def toProtoVersioned(
      serializer: ValueClass => F[scalapb.GeneratedMessage],
      protoVersion: ProtoVersion,
  ): F[VersionedJsonMessage[ValueClass]] =
    functorF.map[scalapb.GeneratedMessage, VersionedJsonMessage[ValueClass]](
      serializer(self)
    )(proto => VersionedJsonMessage(JsonFormat.toJson(proto), protoVersion.v))

  private def serializeToHighestVersion: F[VersionedJsonMessage[ValueClass]] =
    toProtoVersioned(
      companionObj.supportedProtoVersions.higherConverter.serializer,
      companionObj.supportedProtoVersions.higherProtoVersion,
    )

  /** Yields a string representation of the corresponding
    * [[com.digitalasset.canton.version.v1.UntypedVersionedMessage]] wrapper of this instance.
    */
  def toJsonString(version: ProtocolVersion): F[String] = functorF.map(
    toProtoVersioned(version)
  )(versioned => compact(versioned.toJson))
}

trait HasVersionedJsonWrapper[ValueClass] extends HasVersionedJsonWrapperF[Id, ValueClass] {
  self: ValueClass =>

  override protected def functorF: Functor[Id] = Functor[Id]
}

/** Traits for the companion objects of classes that implement
  * [[com.digitalasset.canton.version.HasVersionedJsonWrapper]]. Provide default methods.
  */
trait HasVersionedJsonMessageCompanion[ValueClass]
    extends HasVersionedMessageCompanionCommonF[Id, ValueClass] {
  type Deserializer = JValue => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage: GeneratedMessageCompanion](
      fromProto: Proto => ParsingResult[ValueClass]
  ): JValue => ParsingResult[ValueClass] = { jvalue =>
    fromProto(JsonFormat.fromJson[Proto](jvalue))
  }

  override protected def unsupportedProtoCodecDeserializer(
      protocolVersion: ProtocolVersion
  ): JValue => ParsingResult[ValueClass] = _ =>
    Left(unsupportedDeserializationError(protocolVersion))

  def fromProtoVersioned(
      versionedJson: VersionedJsonMessage[ValueClass]
  ): ParsingResult[ValueClass] =
    supportedProtoVersions.deserializerFor(ProtoVersion(versionedJson.version))(versionedJson.data)

  /** The embedded version is not validated */
  def fromTrustedJsonString(jsonString: String): ParsingResult[ValueClass] = {
    val jvalue = parse(StringInput(jsonString), useBigDecimalForDouble = true)

    for { // no input validation for proto version
      proto <- UntypedVersionedJsonMessage
        .fromJson(jvalue)
        .leftMap(ProtoDeserializationError.OtherError(_))
      valueClass <- fromProtoVersioned(VersionedJsonMessage(proto))
    } yield valueClass
  }

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[String]
  ): GetResult[ValueClass] = GetResult { r =>
    fromTrustedJsonString(r.<<[String]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArrayO: GetResult[Option[String]]
  ): GetResult[Option[ValueClass]] = GetResult { r =>
    r.<<[Option[String]]
      .map(
        fromTrustedJsonString(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }
}

/** The JSON equivalent to
  * [[com.digitalasset.canton.version.HasVersionedMessageCompanionDbHelpers]].
  */
trait HasVersionedJsonMessageCompanionDbHelpers[
    ValueClass <: HasVersionedJsonWrapper[ValueClass]
] {
  def getVersionedSetParameter(protocolVersion: ProtocolVersion): SetParameter[ValueClass] = {
    (value, pp) =>
      pp >> value.toJsonString(protocolVersion)
  }

  def getVersionedSetParameterO(
      protocolVersion: ProtocolVersion
  ): SetParameter[Option[ValueClass]] =
    (valueO, pp) => pp >> valueO.map(_.toJsonString(protocolVersion))
}
