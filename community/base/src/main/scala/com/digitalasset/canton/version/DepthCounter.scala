// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class DepthCounter(depth: Int, limit: Int) {
  def inc: ParsingResult[DepthCounter] = {
    val newDepth = depth + 1
    Either.cond(
      newDepth <= limit,
      DepthCounter(newDepth, limit),
      ProtoDeserializationError.NestingTooDeep(limit),
    )
  }
}
object DepthCounter {
  val Default = DepthCounter(0, Int.MaxValue)
  def withLimit(limit: Int): DepthCounter = DepthCounter(0, limit)
}
