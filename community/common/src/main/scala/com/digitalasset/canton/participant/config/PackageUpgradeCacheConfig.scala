// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.CacheConfigWithSizeOnly
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric

object PackageUpgradeCacheConfig {
  val defaultPackageUpgradeCache: CacheConfigWithSizeOnly = CacheConfigWithSizeOnly(
    maximumSize = PositiveNumeric.tryCreate(10000)
  )
}
