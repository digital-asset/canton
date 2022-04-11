// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protobuf

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.BufferException
import com.digitalasset.canton.serialization.ProtoConverter
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class ProtobufParsingAttackTest extends AnyWordSpec with BaseTest {

  "adding a field to a message definition" should {
    "ignore wrong field types" in {

      val attackAddField = AttackAddField("foo", 123).toByteString

      // Base parser can parse this
      ProtoConverter.protoParser(Base.parseFrom)(attackAddField) shouldBe
        Right(Base(Base.Sum.One("foo")))

      // Parser for the message after the field has been added will ignore the additional field
      ProtoConverter.protoParser(AddField.parseFrom)(attackAddField) shouldBe
        Right(AddField(AddField.Sum.One("foo"), None))
    }

    "explode when the field deserialization fails" in {
      val attackAddField =
        AttackAddFieldSameType("foo", ByteString.copyFromUtf8("BYTESTRING")).toByteString

      // Base parser can parse this
      ProtoConverter.protoParser(Base.parseFrom)(attackAddField) shouldBe
        Right(Base(Base.Sum.One("foo")))

      // Parser for the message after the field has been added will explode
      ProtoConverter
        .protoParser(AddField.parseFrom)(attackAddField)
        .left
        .value shouldBe a[BufferException]
    }
  }

  "adding an alternative to a one-of" should {
    "produce different parsing results" in {

      val dummyMessage = DummyMessage("dummy")
      val attackAddVariant = AttackAddVariant("bar", dummyMessage.toByteString).toByteString

      ProtoConverter.protoParser(Base.parseFrom)(attackAddVariant) shouldBe
        Right(Base(Base.Sum.One("bar")))

      ProtoConverter.protoParser(AddVariant.parseFrom)(attackAddVariant) shouldBe
        Right(AddVariant(AddVariant.Sum.Two(dummyMessage)))
    }

    "explode when given bad alternatives" in {
      val attackAddVariant =
        AttackAddVariant("bar", ByteString.copyFromUtf8("BYTESTRING")).toByteString

      ProtoConverter.protoParser(Base.parseFrom)(attackAddVariant) shouldBe
        Right(Base(Base.Sum.One("bar")))

      ProtoConverter
        .protoParser(AddVariant.parseFrom)(attackAddVariant)
        .left
        .value shouldBe a[BufferException]
    }
  }
}
