// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.daml.error.BaseError
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.lifecycle.{FlagCloseable, RunOnShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import java.util.concurrent.atomic.AtomicReference

/** Abstracts common behavior of a component that reports health
  * The State is left abstract. For a generic component state, use ComponentHealth
  */
trait BaseHealthComponent extends HealthElement {
  self: NamedLogging with FlagCloseable =>
  override type State <: ToComponentHealthState & PrettyPrinting

  def isFailed: Boolean = getState.toComponentHealthState.isFailed

  protected def initialHealthState: State

  protected lazy val stateRef = new AtomicReference[State](initialHealthState)

  def closingState: State

  override def registerOnHealthChange(listener: this.HealthListener): Unit = {
    // The first time a listener is registered, also register a shutdown hook to notify them on closing and clear them
    // to avoid further updates from that component
    if (listeners.get().isEmpty) {
      runOnShutdown_(new RunOnShutdown {
        override def name: String = "close-listeners"

        override def done: Boolean = listeners.get().isEmpty

        override def run(): Unit = {
          reportHealthState_(closingState)(TraceContext.empty)
          listeners.set(Nil)
        }
      })(TraceContext.empty)
    }

    super.registerOnHealthChange(listener)
  }

  def reportHealthState(
      state: State
  )(implicit tc: TraceContext): Boolean = {
    val old = stateRef.getAndSet(state)
    val changed = old != state
    if (changed) {
      logStateChange(old)
      listeners.get().foreach(_(this, state, tc))
    }
    changed
  }

  def reportHealthState_(state: State)(implicit tc: TraceContext): Unit = {
    reportHealthState(state).discard
  }

  private def logStateChange(
      oldState: State
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      show"$name is now in state $getState. Previous state was $oldState"
    )
  }

  override def getState: State = stateRef.get()

  def toComponentStatus: ComponentStatus =
    ComponentStatus(name, getState.toComponentHealthState)
}

/** Implements health component [[BaseHealthComponent]] using [[ComponentHealthState]] as a state.
  * Suitable for most components unless there is a need for a custom State type.
  */
trait HealthComponent extends BaseHealthComponent {
  self: NamedLogging & FlagCloseable =>
  override type State = ComponentHealthState
  override val closingState: ComponentHealthState = ComponentHealthState.ShutdownState

  /** Set the health state to Ok and if the previous state was unhealthy, log a message to inform about the resolution
    * of the ongoing issue.
    */
  def resolveUnhealthy(implicit traceContext: TraceContext): Boolean = {
    reportHealthState(ComponentHealthState.Ok())
  }

  def resolveUnhealthy_(implicit traceContext: TraceContext): Unit = {
    resolveUnhealthy.discard
  }

  /** Report that the component is now degraded.
    * Note that this will override the component state, even if it is currently failed!
    */
  def degradationOccurred(error: BaseError)(implicit tc: TraceContext): Unit = {
    reportHealthState_(
      ComponentHealthState.Degraded(
        UnhealthyState(None, Some(error))
      )
    )
  }

  /** Report that the component is now failed
    */
  def failureOccurred(error: BaseError)(implicit tc: TraceContext): Unit = {
    reportHealthState_(
      ComponentHealthState.Failed(
        UnhealthyState(None, Some(error))
      )
    )
  }

  /** Report that the component is now degraded.
    * Note that this will override the component state, even if it is currently failed!
    */
  def degradationOccurred(error: String)(implicit tc: TraceContext): Unit = {
    reportHealthState_(
      ComponentHealthState.degraded(error)
    )
  }

  /** Report that the component is now failed
    */
  def failureOccurred(error: String)(implicit tc: TraceContext): Unit = {
    reportHealthState_(ComponentHealthState.failed(error))
  }
}
