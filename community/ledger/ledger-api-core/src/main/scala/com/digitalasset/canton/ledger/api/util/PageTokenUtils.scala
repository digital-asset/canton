// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.google.protobuf.ByteString

object PageTokenUtils {

  /** Compute a 4-byte participant ID checksum for use in page tokens. */
  def calcParticipantChecksum(purpose: HashPurpose, participantId: String): ByteString =
    toChecksum(
      Hash
        .build(purpose, HashAlgorithm.Sha256)
        .addString(participantId)
        .finish()
    )

  def toChecksum(hash: Hash): ByteString = hash.unwrap.substring(0, 4)

}
