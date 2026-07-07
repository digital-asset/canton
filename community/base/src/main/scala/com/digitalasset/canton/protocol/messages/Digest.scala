// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.serialization.DeserializationError
import com.google.protobuf.ByteString

object Digest {
  type DigestType = ByteString
  type HashedDigestType = Hash

  def hashedDigestTypeToProto(digest: HashedDigestType): ByteString =
    digest.getCryptographicEvidence

  def hashedDigestTypeFromByteString(
      bytes: ByteString
  ): Either[DeserializationError, HashedDigestType] = Hash.fromByteString(bytes)
  def hashDigest(digest: DigestType): HashedDigestType =
    Hash.digest(HashPurpose.HashedAcsCommitment, digest, HashAlgorithm.Sha256)
}
