// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** The `canton.participants.<participant>.features.deprecated` feature flags, one flag per
  * `com.digitalasset.canton.version.ApiDeprecation`.
  *
  * @param enableDeprecatedEndpoints37
  *   The endpoints deprecated in Canton 3.7 are enabled by default in 3.7, disabled by default in
  *   3.8 and removed in 3.9. Set it to `false` to disable them earlier (calls are rejected with
  *   `DEPRECATED_API_DISABLED`).
  */
final case class DeprecatedApiConfig(enableDeprecatedEndpoints37: Boolean = true)
