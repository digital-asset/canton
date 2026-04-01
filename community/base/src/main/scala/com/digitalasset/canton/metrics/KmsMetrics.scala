// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.*
import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory}

class KmsMetrics(
    parent: MetricName,
    labeledMetricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {

  private val prefix: MetricName = parent :+ "kms"

  val sessionSigningKeysFallback: Counter = labeledMetricsFactory.counter(
    MetricInfo(
      prefix :+ "session-signing-keys-fallback",
      summary =
        "Number of times signing had to fall back to the long-term key, triggering a KMS call.",
      description = """Session signing keys are configured to be valid for a short duration.
          |If this duration is too small or a session key is unavailable, the signing process
          |falls back to using the long-term key to ensure request validation succeeds.
          |This metric counts how many times signing required a direct KMS call with the long-term key.""".stripMargin,
      qualification = MetricQualification.Saturation,
    )
  )
}
