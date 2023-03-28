// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import scala.concurrent.duration.FiniteDuration

final case class IdentityProviderManagementConfig(
    cacheExpiryAfterWrite: FiniteDuration =
      IdentityProviderManagementConfig.DefaultCacheExpiryAfterWriteInSeconds
)
object IdentityProviderManagementConfig {
  import scala.concurrent.duration.*
  val MaxIdentityProviders: Int = 16
  val DefaultCacheExpiryAfterWriteInSeconds: FiniteDuration = 5.minutes
}
