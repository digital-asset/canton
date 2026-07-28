// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.util

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.store.TopologyTransactionRejection

object SerialUtils {
  implicit class EnhancedPositiveInt(val value: PositiveInt) extends AnyVal {
    def nextSerial: Either[TopologyTransactionRejection, RequireTypes.PositiveNumeric[Int]] =
      value.increment.leftMap(_ => TopologyTransactionRejection.Processor.MaxSerialReached)

    def nextSerial(implicit
        elc: ErrorLoggingContext
    ): Either[TopologyManagerError, RequireTypes.PositiveNumeric[Int]] =
      this.nextSerial.leftMap(_.toTopologyManagerError)
  }
}
