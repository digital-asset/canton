// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricHandle.{Histogram, Timer}
import com.daml.metrics.MetricName
import com.digitalasset.canton.metrics.{MetricDoc, MetricHandle}

class TransactionProcessingMetrics(override val prefix: MetricName, val registry: MetricRegistry)
    extends MetricHandle.Factory {

  object protocolMessages {
    private val prefix = TransactionProcessingMetrics.this.prefix :+ "protocol-messages"

    @MetricDoc.Tag(
      summary = "Time to create a confirmation request",
      description =
        """The time that the transaction protocol processor needs to create a confirmation request.""",
    )
    val confirmationRequestCreation: Timer = timer(prefix :+ "confirmation-request-creation")

    @MetricDoc.Tag(
      summary = "Time to parse a transaction message",
      description =
        """The time that the transaction protocol processor needs to parse and decrypt an incoming confirmation request.""",
    )
    val transactionMessageReceipt: Timer = timer(prefix :+ "transaction-message-receipt")

    @MetricDoc.Tag(
      summary = "Confirmation request size",
      description = """Records the histogram of the sizes of (transaction) confirmation requests.""",
    )
    val confirmationRequestSize: Histogram = histogram(prefix :+ "confirmation-request-size")
  }

}
