// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Shared caching settings for tests that do not depend on a particular node module. */
final case class TestCachingConfigs(
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
) extends CachingConfigs
