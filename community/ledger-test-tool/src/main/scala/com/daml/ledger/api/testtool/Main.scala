// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.runner.{AvailableTests, TestRunner}

object Main {
  def main(args: Array[String]): Unit = {
    val config = CliParser.parse(args).getOrElse(sys.exit(1))
    // TODO(#32282) Make it configurable: Add lfVersion in CLI config
    new TestRunner(AvailableTests.v2_2, config).runAndExit()
  }
}
