// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace

final case class TimeoutException(operation: String, timeout: Duration)
    extends RuntimeException
    with NoStackTrace {
  override def getMessage: String = s"'$operation' did not complete within $timeout"
}
