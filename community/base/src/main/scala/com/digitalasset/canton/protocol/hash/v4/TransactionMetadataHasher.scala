// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v4

import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.protocol.hash
import com.digitalasset.canton.protocol.hash.{HashTracer, v3}

private[hash] class TransactionMetadataHasher extends v3.TransactionMetadataHasher {

  override protected def newBuilder(hashTracer: HashTracer): hash.NodeHashBuilder =
    new NodeHashBuilder(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = false,
    ).addPurpose()
}
