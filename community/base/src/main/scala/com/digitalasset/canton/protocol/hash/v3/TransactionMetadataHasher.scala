// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v3

import com.digitalasset.canton.crypto.{HashPurpose, InteractiveSubmission}
import com.digitalasset.canton.protocol.hash
import com.digitalasset.canton.protocol.hash.{HashTracer, NodeHashBuilder, v2, v3}
import com.digitalasset.daml.lf.data.Time

private[hash] class TransactionMetadataHasher extends v2.TransactionMetadataHasher {

  override protected def newBuilder(hashTracer: HashTracer): hash.NodeHashBuilder =
    new v3.NodeHashBuilder(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = false,
    )
      .addPurpose()

  override protected[hash] def tryHashMetadataBuilder(
      metadata: InteractiveSubmission.TransactionMetadataForHashing,
      hashTracer: HashTracer,
  ): NodeHashBuilder =
    super
      .tryHashMetadataBuilder(metadata, hashTracer)
      .withContext("Max Record Time")(
        _.addOptional(
          metadata.maxRecordTime,
          b => (v: Time.Timestamp) => b.addLong(v.micros),
        )
      )
}
