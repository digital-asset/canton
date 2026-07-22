// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

/** A limit on the number of bytes produced by decompressing. */
final case class MaxBytesToDecompress(limit: NonNegativeInt) extends AnyVal

object MaxBytesToDecompress {

  /** Still used in a few places on protocol version 35. Must not be used from protocol version 36
    * on: take the limit from the topology snapshot's `maxRequestSize` instead.
    */
  val HardcodedDefault: MaxBytesToDecompress = MaxBytesToDecompress(
    NonNegativeInt.tryCreate(10 * 1024 * 1024)
  )

  /** Unbounded: use only on data that is already trusted — read back from our own store, or
    * accepted without validation. Never on anything received from the network.
    */
  val MaxValueUnsafe: MaxBytesToDecompress = MaxBytesToDecompress(NonNegativeInt.maxValue)
}
