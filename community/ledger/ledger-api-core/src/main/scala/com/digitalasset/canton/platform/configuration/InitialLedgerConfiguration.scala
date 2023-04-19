// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.configuration

import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}

import java.time.Duration

/** Instructions on how to initialize an empty ledger, without a configuration.
  *
  * A configuration is only submitted if one is not detected on the ledger and `enabled` flag is set to `true`
  *
  * @param maxDeduplicationDuration  The maximum time window during which commands can be deduplicated.
  * @param avgTransactionLatency  The expected average latency of a transaction, i.e., the average
  *                               time from submitting the transaction to a write service and the
  *                               transaction being assigned a record time.
  * @param minSkew                The minimimum skew between ledger time and record time:
  *                               lt_TX >= rt_TX - minSkew
  * @param maxSkew                The maximum skew between ledger time and record time:
  *                               lt_TX <= rt_TX + maxSkew
  * @param delayBeforeSubmitting The delay until the participant tries to submit a configuration.
  */
final case class InitialLedgerConfiguration(
    maxDeduplicationDuration: Duration = Duration.ofMinutes(30),
    avgTransactionLatency: Duration = Duration.ofSeconds(0),
    minSkew: Duration = Duration.ofSeconds(30),
    maxSkew: Duration = Duration.ofSeconds(30),
    delayBeforeSubmitting: Duration = Duration.ofSeconds(0),
) {
  def toConfiguration = Configuration(
    generation = 1L,
    timeModel =
      LedgerTimeModel.apply(avgTransactionLatency, minSkew, maxSkew).fold(throw _, identity),
    maxDeduplicationDuration = maxDeduplicationDuration,
  )
}