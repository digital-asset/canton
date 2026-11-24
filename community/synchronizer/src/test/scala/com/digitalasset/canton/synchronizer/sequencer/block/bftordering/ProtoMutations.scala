// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering

import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.google.protobuf.ByteString
import scalapb.descriptors.{
  Descriptor,
  FieldDescriptor,
  PByteString,
  PEmpty,
  PInt,
  PLong,
  PMessage,
  PRepeated,
  PString,
  PValue,
  ScalaType,
}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

/** Generates hostile variants of well-formed protobuf messages, to check that parsers of untrusted
  * input reject them with an error instead of throwing.
  */
object ProtoMutations {

  type Companion = GeneratedMessageCompanion[? <: GeneratedMessage]

  private val longBoundaries: LazyList[Long] = LazyList(Long.MinValue, Long.MaxValue, -1L)
  private val intBoundaries: LazyList[Int] = LazyList(Int.MinValue, Int.MaxValue, -1)
  private val stringBoundaries: LazyList[String] = LazyList("", "\u0000 not an identifier ￿")
  private val bytesBoundaries: LazyList[ByteString] =
    LazyList(ByteString.EMPTY, ByteString.copyFrom(Array[Byte](0x7f)))

  private val signedMessagePayloadField: String =
    v30.SignedMessage.scalaDescriptor.fullName + ".message"

  /** The payload types of the `v30.SignedMessage`s met while walking a message: `outermost` for
    * those not nested in another signed message, `nested` for all the others.
    */
  final case class SignedPayloads(outermost: Companion, nested: Companion) {
    def inner: SignedPayloads = SignedPayloads(nested, nested)
  }

  /** All the messages that differ from `message` in exactly one field, at any depth, for every
    * field and every boundary value of the field's type:
    *
    *   - integers are set to their minimum, maximum and -1 (this covers
    *     `google.protobuf.Timestamp`, which is a pair of integers);
    *   - strings and bytes are emptied and replaced by garbage;
    *   - present sub-messages are removed, absent ones are added, empty and with each of their
    *     integers set to a boundary value;
    *   - repeated fields are emptied.
    *
    * The payload of a `v30.SignedMessage` is opaque bytes on the wire, so it is decoded as given by
    * `signedPayloads`, mutated in the same way and encoded again. Booleans, floating point numbers
    * and enums are left alone.
    */
  def singleFieldMutants[A <: GeneratedMessage](message: A, signedPayloads: SignedPayloads)(implicit
      companion: GeneratedMessageCompanion[A]
  ): LazyList[A] =
    messageMutants(message.toPMessage, signedPayloads).map(companion.messageReads.read)

  private def messageMutants(
      message: PMessage,
      signedPayloads: SignedPayloads,
  ): LazyList[PMessage] =
    message.value.to(LazyList).flatMap { case (field, value) =>
      fieldMutants(field, value, signedPayloads).map(mutant =>
        PMessage(message.value.updated(field, mutant))
      )
    }

  private def fieldMutants(
      field: FieldDescriptor,
      value: PValue,
      signedPayloads: SignedPayloads,
  ): LazyList[PValue] =
    value match {
      case PRepeated(elements) =>
        LazyList(PRepeated(Vector.empty)).filter(_ => elements.nonEmpty) #:::
          elements.zipWithIndex.to(LazyList).flatMap { case (element, index) =>
            presentValueMutants(field, element, signedPayloads).map(mutant =>
              PRepeated(elements.updated(index, mutant))
            )
          }
      case PEmpty =>
        field.scalaType match {
          case ScalaType.Message(descriptor) => addedMessages(descriptor)
          case _ => LazyList.empty
        }
      case _: PMessage => PEmpty #:: presentValueMutants(field, value, signedPayloads)
      case _ => presentValueMutants(field, value, signedPayloads)
    }

  private def presentValueMutants(
      field: FieldDescriptor,
      value: PValue,
      signedPayloads: SignedPayloads,
  ): LazyList[PValue] =
    value match {
      case PLong(current) => longBoundaries.filter(_ != current).map(PLong(_))
      case PInt(current) => intBoundaries.filter(_ != current).map(PInt(_))
      case PString(current) => stringBoundaries.filter(_ != current).map(PString(_))
      case PByteString(current) =>
        (bytesBoundaries.filter(_ != current) #:::
          LazyList(current)
            .filter(_ => field.fullName == signedMessagePayloadField)
            .flatMap(signedPayloadMutants(_, signedPayloads.outermost, signedPayloads.inner)))
          .map(PByteString(_))
      case message: PMessage => messageMutants(message, signedPayloads)
      case _ => LazyList.empty
    }

  private def addedMessages(descriptor: Descriptor): LazyList[PMessage] =
    PMessage(Map.empty) #::
      descriptor.fields.to(LazyList).filterNot(_.isRepeated).flatMap { field =>
        val boundaries: LazyList[PValue] = field.scalaType match {
          case ScalaType.Long => longBoundaries.map(PLong(_))
          case ScalaType.Int => intBoundaries.map(PInt(_))
          case _ => LazyList.empty
        }
        boundaries.map(boundary => PMessage(Map(field -> boundary)))
      }

  private def signedPayloadMutants[P <: GeneratedMessage](
      versionedBytes: ByteString,
      payload: GeneratedMessageCompanion[P],
      signedPayloads: SignedPayloads,
  ): LazyList[ByteString] = {
    val versioned = UntypedVersionedMessage.parseFrom(versionedBytes.toByteArray)
    (versioned.withVersion(-1) #::
      versioned.wrapper.data.toList.to(LazyList).flatMap { data =>
        messageMutants(payload.parseFrom(data.toByteArray).toPMessage, signedPayloads)
          .map(mutant => versioned.withData(payload.messageReads.read(mutant).toByteString))
      }).map(_.toByteString)
  }
}
