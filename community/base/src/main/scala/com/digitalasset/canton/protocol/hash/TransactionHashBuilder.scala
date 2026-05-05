// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{HashBuilderFromMessageDigest, HashPurpose}
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.version.HashingSchemeVersion

object TransactionHashBuilder {
  def apply(
      purpose: HashPurpose,
      hashTracer: HashTracer,
      hashingSchemeVersion: HashingSchemeVersion,
  ): TransactionHashBuilder =
    new TransactionHashBuilder(purpose, hashTracer)
      .addPurpose()
      .addHashingSchemeVersion(hashingSchemeVersion)
}

/** HashBuilder with utilities specific to transactions.
  */
class TransactionHashBuilder private (
    purpose: HashPurpose,
    hashTracer: HashTracer,
) extends HashBuilderFromMessageDigest(Sha256, purpose, hashTracer) {
  private def addHashingSchemeVersion(hashingSchemeVersion: HashingSchemeVersion): this.type =
    addByte(
      hashingSchemeVersion.index.byteValue,
      byte => s"${HexString.toHexString(byte)} (Hashing Scheme Version)",
    )
}
