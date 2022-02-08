// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.github.blemale.scaffeine.Scaffeine
import com.google.common.annotations.VisibleForTesting

/** Configurations settings for a single cache
  *
  * @param maximumSize the maximum size of the cache
  * @param expireAfterAccess how quickly after last access items should be expired from the cache
  */
final case class CacheConfig(
    maximumSize: PositiveNumeric[Long],
    expireAfterAccess: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(10),
) {

  def buildScaffeine(): Scaffeine[Any, Any] =
    Scaffeine().maximumSize(maximumSize.value).expireAfterAccess(expireAfterAccess.toScala)

}

/** Configuration settings for various internal caches
  *
  * @param indexedStrings cache size configuration for the static string index cache
  * @param contractStore cache size configuration for the contract store
  * @param topologySnapshot cache size configuration for topology snapshots
  */
final case class CachingConfigs(
    indexedStrings: CacheConfig = CachingConfigs.defaultStaticStringCache,
    contractStore: CacheConfig = CachingConfigs.defaultContractStoreCache,
    topologySnapshot: CacheConfig = CachingConfigs.defaultTopologySnapshotCache,
    partyCache: CacheConfig = CachingConfigs.defaultPartyCache,
    participantCache: CacheConfig = CachingConfigs.defaultParticipantCache,
    keyCache: CacheConfig = CachingConfigs.defaultKeyCache,
    packageVettingCache: CacheConfig = CachingConfigs.defaultPackageVettingCache,
    mySigningKeyCache: CacheConfig = CachingConfigs.defaultMySigningKeyCache,
)

object CachingConfigs {
  val defaultStaticStringCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultContractStoreCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(100000))
  val defaultTopologySnapshotCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(100))
  val defaultPartyCache: CacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultParticipantCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultKeyCache: CacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultPackageVettingCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultMySigningKeyCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(5))
  @VisibleForTesting
  val testing =
    CachingConfigs(contractStore = CacheConfig(maximumSize = PositiveNumeric.tryCreate(100)))

}
