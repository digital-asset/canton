// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.daml.ledger.api.v2.value.Value.Sum
import com.digitalasset.transcode.schema.{DynamicValue, FieldName, Identifier}
import com.digitalasset.transcode.utils.codecspec.CodecCommonSpec
import com.digitalasset.transcode.{Codec, MissingFieldsException}
import zio.*
import zio.test.*
import zio.test.Assertion.*

object ProtobufCodecWithoutAllowMissingFieldsSpec extends CodecCommonSpec[Value]:

  val schemaProcessor = ProtobufCodecConfigured(allowMissingFields = false)

  val codec = schemaProcessor.constructor(
    Identifier.fromString("#aa:aa:aa"),
    Seq(),
    schemaProcessor.record(
      Seq(
        FieldName("required_field") -> schemaProcessor.int64,
        FieldName("required_party_field") -> schemaProcessor.party,
        FieldName("optional_field") -> schemaProcessor.optional(schemaProcessor.text),
      )
    ),
  )

  override def spec =
    suite("ProtobufCodecConfigured(allowMissingFields = false)")(
      autoTests,
      test("required record field not present") {
        val truncatedRecord = Value(
          Sum.Record(
            Record(
              fields = Seq(
                RecordField(
                  label = "required_field",
                  value = Some(Value(Sum.Int64(7))),
                )
              )
            )
          )
        )
        assertZIO(
          ZIO
            .attempt(
              decode(
                truncatedRecord,
                codec,
              )
            )
            .exit
        )(
          fails(
            isSubtype[MissingFieldsException](hasMessage(containsString("required_party_field")))
          )
        )
      },
      test("multiple required record fields not present") {
        val truncatedRecord = Value(
          Sum.Record(
            Record(
              fields = Seq.empty
            )
          )
        )
        assertZIO(
          ZIO
            .attempt(
              decode(
                truncatedRecord,
                codec,
              )
            )
            .exit
        )(
          fails(
            isSubtype[MissingFieldsException](
              hasField(
                "missingFields",
                _.missingFields,
                equalTo(Set("required_field", "required_party_field")),
              )
            )
          )
        )
      },
      test("extra field in record is ignored") {
        val recordWithExtraField = Value(
          Sum.Record(
            Record(
              fields = Seq(
                RecordField(
                  label = "required_field",
                  value = Some(Value(Sum.Int64(9))),
                ),
                RecordField(
                  label = "required_party_field",
                  value = Some(Value(Sum.Party("some_party"))),
                ),
                RecordField(
                  label = "optional_field",
                  value = Some(Value(Sum.Optional(Optional(Some(Value(Sum.Text("hello"))))))),
                ),
                RecordField(
                  label = "unexpected_extra_field",
                  value = Some(Value(Sum.Text("should be ignored"))),
                ),
              )
            )
          )
        )
        assertZIO(
          ZIO
            .attempt(
              decode(
                recordWithExtraField,
                codec,
              )
            )
            .exit
        )(
          succeeds(equalTo(Array[Any](9, "some_party", Some("hello"))))
        )
      },
    )

  def decode[T](
      value: Value,
      codec0: T,
  )(using conv: Conversion[T, Codec[Value]]): DynamicValue =
    val codec = conv(codec0)
    val decodedValue = codec.toDynamicValue(value)
    decodedValue
