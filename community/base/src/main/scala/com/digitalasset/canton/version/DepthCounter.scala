// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

trait DepthCounter {
  def inc: ParsingResult[DepthCounter]
}

object DepthCounter {
  val NoLimit: DepthCounter = new DepthCounter {
    override def inc: ParsingResult[DepthCounter] = Right(this)
  }
  val ZeroLimit: DepthCounter = new DepthCounter {
    override def inc: ParsingResult[DepthCounter] =
      Left(ProtoDeserializationError.NestingTooDeep(0))
  }

  def withLimit(protocolVersion: ProtocolVersion, limit: Int): DepthCounter =
    if (protocolVersion >= ProtocolVersion.v36) Impl(0, limit) else NoLimit

  private final case class Impl(depth: Int, limit: Int) extends DepthCounter {
    def inc: ParsingResult[DepthCounter] = {
      val newDepth = depth + 1
      Either.cond(
        newDepth <= limit,
        Impl(newDepth, limit),
        ProtoDeserializationError.NestingTooDeep(limit),
      )
    }
  }
}
