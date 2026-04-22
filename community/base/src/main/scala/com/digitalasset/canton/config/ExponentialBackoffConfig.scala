// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config

/** Exponential backoff configuration for retries of operations
  *
  * @param initialDelay
  *   initial delay before the first retry
  * @param maxDelay
  *   max delay between retries
  * @param maxRetries
  *   max number of retries
  */
final case class ExponentialBackoffConfig(
    initialDelay: config.NonNegativeFiniteDuration,
    maxDelay: config.NonNegativeDuration,
    maxRetries: Int,
)
