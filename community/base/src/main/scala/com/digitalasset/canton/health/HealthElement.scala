// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

/** There are 2 subtypes of [[HealthElement]]s:
  * - ComponentHealth: Directly reports its own health
  * - ServiceHealth: Aggregates ComponentHealth under critical and soft dependencies
  * Services are queryable through their name in the gRPC Health Check service
  */
trait HealthElement {
  type State
  protected val listeners = new AtomicReference[List[HealthListener]](Nil)
  type HealthListener = (this.type, State, TraceContext) => Unit

  def registerOnHealthChange(listener: HealthListener): Unit = {
    listeners.getAndUpdate(listener :: _)
    listener(this, getState, TraceContext.empty)
  }

  /** Name of the health element. Used for logging.
    */
  def name: String

  /** Current state
    */
  def getState: State
}
