// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.SubscriptionError

sealed trait TestSubscriptionError
    extends SubscriptionError
    with Product
    with Serializable
    with PrettyPrinting {
  override protected def pretty: Pretty[this.type] = prettyOfObject[this.type]
}
object TestSubscriptionError {
  case object RetryableError extends TestSubscriptionError
  case object UnretryableError extends TestSubscriptionError
  case object RetryableExn extends Exception
  case object FatalExn extends Exception
}
