// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  CacheConfig,
  CacheConfigWithMemoryBounds,
  CacheConfigWithTimeout,
  CachingConfigs,
  NonNegativeFiniteDuration,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.util.BytesUnit

final case class SequencerCachingConfigs(
    indexedStrings: CacheConfig = CachingConfigs.defaultStaticStringCache,
    topologySnapshot: CacheConfig = CachingConfigs.defaultTopologySnapshotCache,
    synchronizerClientMaxTimestamp: CacheConfig =
      CachingConfigs.defaultSynchronizerClientMaxTimestampCache,
    partyCache: CacheConfig = CachingConfigs.defaultPartyCache,
    participantCache: CacheConfig = CachingConfigs.defaultParticipantCache,
    keyCache: CacheConfig = CachingConfigs.defaultKeyCache,
    sessionEncryptionKeyCache: SessionEncryptionKeyCacheConfig = SessionEncryptionKeyCacheConfig(),
    transparencyCheckCache: CacheConfigWithTimeout = CachingConfigs.defaultTransparencyCheckCache,
    publicKeyConversionCache: CacheConfig = CachingConfigs.defaultPublicKeyConversionCache,
    packageVettingCache: CacheConfig = CachingConfigs.defaultPackageVettingCache,
    memberCache: CacheConfig = CachingConfigs.defaultMemberCache,
    kmsMetadataCache: CacheConfig = CachingConfigs.defaultKmsMetadataCache,
    sequencerPayloadCache: CacheConfigWithMemoryBounds =
      SequencerCachingConfigs.defaultSequencerPayloadCache,
    sequencerCatchupPayloadCache: CacheConfigWithMemoryBounds =
      SequencerCachingConfigs.defaultSequencerPayloadCache,
    bftOrderingBatchCache: CacheConfigWithMemoryBounds =
      SequencerCachingConfigs.defaultBftOrderingBatchCache,
) extends CachingConfigs

object SequencerCachingConfigs {
  val defaultSequencerPayloadCache: CacheConfigWithMemoryBounds =
    CacheConfigWithMemoryBounds(
      maximumMemory = PositiveNumeric.tryCreate(BytesUnit.MB(64L)),
      expireAfterAccess = NonNegativeFiniteDuration.ofMinutes(10),
    )
  val defaultBftOrderingBatchCache: CacheConfigWithMemoryBounds =
    CacheConfigWithMemoryBounds(
      maximumMemory = PositiveNumeric.tryCreate(BytesUnit.MB(64L)),
      expireAfterAccess = NonNegativeFiniteDuration.ofMinutes(10),
    )
}
