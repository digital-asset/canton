// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.participant.store.AcsDigestStore.{HashedDigest, RawDigest}
import com.digitalasset.canton.protocol.messages.Digest

trait TestDigestUtils {
  private val rawDigestByteSize = 2048

  def genRawDigest(fill: Byte): RawDigest =
    LtHash16.tryCreate(Array.fill[Byte](rawDigestByteSize)(fill)).getByteString()

  def genHashedDigest(rawDigest: RawDigest): HashedDigest =
    Digest.hashDigest(rawDigest).getCryptographicEvidence

  def timestamp(epochSeconds: PositiveLong): CantonTimestamp =
    CantonTimestamp.ofEpochSecond(epochSeconds.unwrap)

  def offsetTime(epochSeconds: PositiveLong): (Offset, CantonTimestamp) =
    (Offset.tryFromLong(epochSeconds.unwrap), timestamp(epochSeconds))

  def genParticipantDigest(rawDigest: RawDigest): (RawDigest, HashedDigest) =
    (rawDigest, genHashedDigest(rawDigest))
}
