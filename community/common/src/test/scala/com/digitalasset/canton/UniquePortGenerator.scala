// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.RequireTypes.Port

import java.util.concurrent.atomic.AtomicInteger

class UniquePortGenerator(start: Int) extends {
  private val nextPort = new AtomicInteger(start)

  def next: Port = Port.tryCreate(nextPort.getAndIncrement())
}

/** Generates ports for canton tests that we guarantee won't be used in our tests.
  * We however don't check that the port hasn't been bound by other processes on the host.
  */
object UniquePortGenerator {
  val integrationTestsPortStart: Int = 15000
  val forIntegrationTests = new UniquePortGenerator(integrationTestsPortStart)
  val forDomainTests = new UniquePortGenerator(30000)
}
