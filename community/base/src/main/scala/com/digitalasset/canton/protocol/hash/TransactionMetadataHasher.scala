// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.version.HashingSchemeVersion

/** Interface to hash transaction metadata
  */
abstract class TransactionMetadataHasher {

  /** Hashes Transaction Metadata */
  @throws[NodeHashingError]
  def tryHashMetadata(
      metadata: TransactionMetadataForHashing,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    tryHashMetadataBuilder(metadata, hashTracer).finish()

  /** Hashes Transaction Metadata */
  @throws[NodeHashingError]
  protected[hash] def tryHashMetadataBuilder(
      metadata: TransactionMetadataForHashing,
      hashTracer: HashTracer,
  ): NodeHashBuilder
}

object TransactionMetadataHasher {

  private[hash] def apply(
      hashingSchemeVersion: HashingSchemeVersion
  ): TransactionMetadataHasher =
    hashingSchemeVersion match {
      case HashingSchemeVersion.V2 =>
        new v2.TransactionMetadataHasher()
      case HashingSchemeVersion.V3 =>
        new v3.TransactionMetadataHasher()
    }
}
