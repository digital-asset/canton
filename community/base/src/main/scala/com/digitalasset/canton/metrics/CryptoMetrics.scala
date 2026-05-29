// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

/** Aggregates all cryptographic-related metrics.
  *
  * @param signingMetrics
  *   Metrics for signing operations
  * @param decryptionMetrics
  *   Metrics for decryption operations
  * @param kmsMetricsO
  *   Optional metrics for KMS-backed operations; defined only when KMS is in use
  */
class CryptoMetrics(
    val signingMetrics: SigningMetrics,
    val decryptionMetrics: DecryptionMetrics,
    val kmsMetricsO: Option[KmsMetrics],
)
