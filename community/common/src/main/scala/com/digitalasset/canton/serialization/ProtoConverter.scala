// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import java.time.{DateTimeException, Duration, Instant}
import java.util.UUID
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances._
import com.digitalasset.canton.ProtoDeserializationError.{
  BufferException,
  FieldNotSet,
  StringConversionError,
  TimestampConversionError,
}
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.google.protobuf.timestamp.Timestamp
import com.google.protobuf.{ByteString, CodedInputStream, InvalidProtocolBufferException}

/** Can convert messages to and from proto objects
  * @tparam A type of the message to be serialized
  * @tparam Proto type of the proto message
  * @tparam Err type of deserialization errors
  */
trait ProtoConverter[A, Proto, Err] {

  /** Convert an instance to a protobuf structure
    * @param value to be serialized
    * @return serialized proto
    */
  def toProtoPrimitive(value: A): Proto

  /** Convert proto value to its native type
    * @param value to be deserialized
    * @return deserialized value
    */
  def fromProtoPrimitive(value: Proto): Either[Err, A]
}

object ProtoConverter {
  type ParsingResult[+T] = Either[ProtoDeserializationError, T]

  /** Helper to convert protobuf exceptions into ProtoDeserializationErrors
    *
    * i.e. usage: ProtoConverter.protoParser(v0.MessageContent.parseFrom)
    */
  def protoParser[A](parseFrom: CodedInputStream => A): ByteString => Either[BufferException, A] =
    bytes =>
      Either
        .catchOnly[InvalidProtocolBufferException](parseFrom(bytes.newCodedInput))
        .leftMap(BufferException)

  def protoParserArray[A](parseFrom: Array[Byte] => A): Array[Byte] => Either[BufferException, A] =
    bytes =>
      Either.catchOnly[InvalidProtocolBufferException](parseFrom(bytes)).leftMap(BufferException)

  /** Helper for extracting an optional field where the value is required
    * @param field the field name
    * @param optValue the optional value
    * @return a [[scala.Right$]] of the value if set or
    *         a [[scala.Left$]] of [[com.digitalasset.canton.ProtoDeserializationError.FieldNotSet]] error
    */
  def required[B](field: String, optValue: Option[B]): Either[FieldNotSet, B] =
    optValue.toRight(FieldNotSet(field))

  def parseRequired[A, P](
      fromProto: P => ParsingResult[A],
      field: String,
      optValue: Option[P],
  ): ParsingResult[A] =
    required(field, optValue).flatMap(fromProto)

  def parse[A, P](
      parseFrom: CodedInputStream => P,
      fromProto: P => ParsingResult[A],
      value: ByteString,
  ): ParsingResult[A] =
    protoParser(parseFrom)(value).flatMap(fromProto)

  def pareRequiredNonEmpty[A, P](
      fromProto: P => ParsingResult[A],
      field: String,
      content: Seq[P],
  ): ParsingResult[NonEmpty[Seq[A]]] =
    for {
      contentNE <- NonEmpty
        .from(content)
        .toRight(ProtoDeserializationError.OtherError(s"Sequence $field not set or empty"))
      parsed <- contentNE.toNEF.traverse(fromProto)
    } yield parsed

  def parseLfPartyId(party: String): Either[StringConversionError, LfPartyId] =
    LfPartyId.fromString(party).leftMap(StringConversionError)

  object InstantConverter extends ProtoConverter[Instant, Timestamp, ProtoDeserializationError] {
    override def toProtoPrimitive(value: Instant): Timestamp =
      Timestamp(value.getEpochSecond, value.getNano)

    override def fromProtoPrimitive(proto: Timestamp): ParsingResult[Instant] =
      try {
        Right(Instant.ofEpochSecond(proto.seconds, proto.nanos.toLong))
      } catch {
        case _: DateTimeException =>
          Left(TimestampConversionError("timestamp exceeds min or max of Instant"))
        case _: ArithmeticException => Left(TimestampConversionError("numeric overflow"))
      }

  }

  object DurationConverter
      extends ProtoConverter[
        java.time.Duration,
        com.google.protobuf.duration.Duration,
        ProtoDeserializationError,
      ] {
    override def toProtoPrimitive(duration: Duration): com.google.protobuf.duration.Duration =
      com.google.protobuf.duration.Duration(duration.getSeconds, duration.getNano)
    override def fromProtoPrimitive(
        duration: com.google.protobuf.duration.Duration
    ): ParsingResult[java.time.Duration] =
      Right(java.time.Duration.ofSeconds(duration.seconds, duration.nanos.toLong))
  }

  object UuidConverter extends ProtoConverter[UUID, String, StringConversionError] {
    override def toProtoPrimitive(uuid: UUID): String = uuid.toString

    override def fromProtoPrimitive(uuidP: String): Either[StringConversionError, UUID] =
      Either
        .catchOnly[IllegalArgumentException](UUID.fromString(uuidP))
        .leftMap(err => StringConversionError(err.getMessage))
  }
}
