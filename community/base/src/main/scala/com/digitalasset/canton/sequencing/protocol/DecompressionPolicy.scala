// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.ProtocolVersion

/** Hands out the [[DecompressionBudget]]s for one batch: call [[nextEnvelopeBudget]] once per
  * envelope.
  */
private[protocol] trait BatchDecompressionAllocator {
  def nextEnvelopeBudget(): DecompressionBudget
}

/** A policy bounding the bytes produced when decompressing a batch's envelopes.
  */
sealed trait DecompressionPolicy extends Product with Serializable {
  def limit: MaxBytesToDecompress

  /** Returns the source of budgets for one batch.
    *
    * The identity of the handed-out budgets is what encodes the policy.
    * [[DecompressionPolicy.PerEnvelope]] hands out a fresh budget on every call, so each envelope
    * is bounded on its own; [[DecompressionPolicy.Cumulative]] hands out the same instance every
    * time, so the batch's envelopes share one running total and it is their combined decompressed
    * size that is bounded.
    */
  private[protocol] def newBatchAllocator(): BatchDecompressionAllocator
}

object DecompressionPolicy {

  /** Bounds each envelope's decompression individually by [[limit]].
    */
  final case class PerEnvelope(limit: MaxBytesToDecompress) extends DecompressionPolicy {
    override private[protocol] def newBatchAllocator(): BatchDecompressionAllocator =
      new BatchDecompressionAllocator {
        override def nextEnvelopeBudget(): DecompressionBudget = DecompressionBudget(limit)
      }
  }

  /** Bounds the combined decompression of a batch's envelopes by one shared budget of [[limit]]
    * bytes.
    */
  final case class Cumulative(limit: MaxBytesToDecompress) extends DecompressionPolicy {
    override private[protocol] def newBatchAllocator(): BatchDecompressionAllocator = {
      val shared = DecompressionBudget(limit)
      new BatchDecompressionAllocator {
        override def nextEnvelopeBudget(): DecompressionBudget = shared
      }
    }
  }

  def forProtocolVersion(
      protocolVersion: ProtocolVersion,
      limit: MaxBytesToDecompress,
  ): DecompressionPolicy =
    if (protocolVersion > ProtocolVersion.v35) Cumulative(limit) else PerEnvelope(limit)

  /** Still used in a few places on protocol version 35. Must not be used from protocol version 36
    * on: derive the policy from the topology snapshot via [[forProtocolVersion]] instead.
    */
  val HardcodedDefault: DecompressionPolicy = PerEnvelope(MaxBytesToDecompress.HardcodedDefault)

  /** Unbounded: use only on data that is already trusted — read back from our own store, or
    * accepted without validation. Never on anything received from the network.
    */
  val MaxValueUnsafe: DecompressionPolicy = PerEnvelope(MaxBytesToDecompress.MaxValueUnsafe)
}
