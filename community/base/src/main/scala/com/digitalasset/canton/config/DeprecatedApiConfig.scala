// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** The `canton.participants.<participant>.features.deprecated` feature flags that temporarily
  * re-enable deprecated APIs that are disabled by default, one flag per
  * `com.digitalasset.canton.version.ApiDeprecation`.
  *
  * @param enableDeprecatedEndpoints34
  *   Re-enables the gRPC and JSON Ledger API endpoints that were deprecated in Canton 3.4 and are
  *   disabled by default since Canton 3.6 (calls are rejected with `DEPRECATED_API_DISABLED`).
  * @param enableDeprecatedParameters34
  *   Re-enables the gRPC and JSON Ledger API request fields that were deprecated in Canton 3.4 and
  *   are disabled by default since Canton 3.6 (requests using them are rejected with
  *   `DEPRECATED_API_DISABLED`).
  * @param enableDeprecatedEndpoints35
  *   Re-enables the gRPC and JSON Ledger API endpoints and gRPC methods that were deprecated in
  *   Canton 3.5 and are disabled by default since Canton 3.6 (calls are rejected with
  *   `DEPRECATED_API_DISABLED`).
  */
// TODO(#35974) remove the 3.4 and 3.5 flags together with the deprecated APIs in 3.7
final case class DeprecatedApiConfig(
    enableDeprecatedEndpoints34: Boolean = false,
    enableDeprecatedParameters34: Boolean = false,
    enableDeprecatedEndpoints35: Boolean = false,
)
