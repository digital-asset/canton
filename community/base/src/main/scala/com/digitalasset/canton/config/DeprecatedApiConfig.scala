// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** The `canton.participants.<participant>.features.deprecated` feature flags, one flag per
  * `com.digitalasset.canton.version.ApiDeprecation`.
  *
  * @param enableDeprecatedEndpoints37
  *   The endpoints deprecated in Canton 3.7 are disabled by default since 3.8 and removed in 3.9.
  *   Set it to `true` to re-enable them until then.
  */
final case class DeprecatedApiConfig(enableDeprecatedEndpoints37: Boolean = false)
