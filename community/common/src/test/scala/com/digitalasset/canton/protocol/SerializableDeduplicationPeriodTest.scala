// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.google.protobuf.duration.Duration as ProtoDuration
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SerializableDeduplicationPeriodTest extends AnyWordSpec with Matchers {
  "SerializableDeduplicationPeriod.fromProtoV30" should {
    "return a parsing error for negative durations" in {
      val proto = v30.DeduplicationPeriod(
        v30.DeduplicationPeriod.Period.Duration(ProtoDuration(seconds = -1L))
      )

      SerializableDeduplicationPeriod.fromProtoV30(proto) should matchPattern {
        case Left(ValueConversionError("deduplication_period", _)) =>
      }
    }

    "return a parsing error for durations with finer-than-microsecond precision" in {
      val proto = v30.DeduplicationPeriod(
        v30.DeduplicationPeriod.Period.Duration(ProtoDuration(nanos = 3002001))
      )

      SerializableDeduplicationPeriod.fromProtoV30(proto) should matchPattern {
        case Left(ValueConversionError("deduplication_period", _)) =>
      }
    }
  }
}
