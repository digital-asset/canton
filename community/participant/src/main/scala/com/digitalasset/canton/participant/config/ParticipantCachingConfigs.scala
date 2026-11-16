// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  CacheConfig,
  CacheConfigWithSizeOnly,
  CacheConfigWithTimeout,
  CachingConfigs,
  SessionEncryptionKeyCacheConfig,
}

/** Configuration settings for participant caches.
  *
  * @param contractStore
  *   cache size configuration for the contract store
  */
final case class ParticipantCachingConfigs(
    indexedStrings: CacheConfig = CachingConfigs.defaultStaticStringCache,
    contractStore: CacheConfig = ParticipantCachingConfigs.defaultContractStoreCache,
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
    packageUpgradeCache: CacheConfigWithSizeOnly =
      PackageUpgradeCacheConfig.defaultPackageUpgradeCache,
    memberCache: CacheConfig = CachingConfigs.defaultMemberCache,
    kmsMetadataCache: CacheConfig = CachingConfigs.defaultKmsMetadataCache,
) extends CachingConfigs

object ParticipantCachingConfigs {
  val defaultContractStoreCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))

}
