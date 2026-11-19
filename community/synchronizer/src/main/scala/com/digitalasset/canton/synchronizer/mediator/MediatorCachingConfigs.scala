// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  CacheConfig,
  CacheConfigWithTimeout,
  CachingConfigs,
  SessionEncryptionKeyCacheConfig,
}

/** Configuration settings for mediator caches.
  *
  * @param finalizedMediatorConfirmationRequests
  *   cache size for the finalized mediator confirmation requests such the mediator does not have to
  *   perform a db round-trip if we have slow responders.
  */
final case class MediatorCachingConfigs(
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
    finalizedMediatorConfirmationRequests: CacheConfig =
      MediatorCachingConfigs.defaultFinalizedMediatorConfirmationRequestsCache,
) extends CachingConfigs

object MediatorCachingConfigs {
  val defaultFinalizedMediatorConfirmationRequestsCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
}
