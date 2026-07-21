// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import com.digitalasset.canton.ProtoDeserializationError.DurationConversionError
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProtoConverterTest extends AnyWordSpec with EitherValues with Matchers {
  "ProtoConverter" should {
    "not overflow when deserializing max proto duration" in {
      val maxProtoDuration = com.google.protobuf.duration.Duration(Long.MaxValue, 1_000_000_000)
      ProtoConverter.DurationConverter
        .fromProtoPrimitive(maxProtoDuration)
        .left
        .value shouldBe a[DurationConversionError]
    }
  }
}
