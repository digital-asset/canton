// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{HasRunOnClosing, OnShutdownRunner}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success, Try}

/** Represents a [[com.digitalasset.canton.health.ComponentHealthState]] and its corresponding
  * metric value that is used across various ACS commitment pipeline components.
  */
final case class AcsCommitmentHealthState(
    metricValue: Int,
    componentHealthState: ComponentHealthState,
)

object AcsCommitmentHealthState {

  /** The component has not yet been initialized. Reported as failure.
    */
  val NotInitialized: AcsCommitmentHealthState =
    AcsCommitmentHealthState(
      CommitmentMetrics.HealthValues.NotInitialized,
      ComponentHealthState.NotInitializedState,
    )

  /** The component is in the process of starting up. Reported as degradation.
    */
  val Starting: AcsCommitmentHealthState =
    AcsCommitmentHealthState(
      CommitmentMetrics.HealthValues.Starting,
      ComponentHealthState.degraded("Starting"),
    )

  /** The component has successfully started and is running. Reported healthy state.
    */
  val Started: AcsCommitmentHealthState =
    AcsCommitmentHealthState(CommitmentMetrics.HealthValues.Started, ComponentHealthState.Ok())

  /** The component is in the process of being stopped. Reported as degradation.
    */
  val Stopping: AcsCommitmentHealthState =
    AcsCommitmentHealthState(
      CommitmentMetrics.HealthValues.Stopping,
      ComponentHealthState.degraded("Stopping"),
    )

  /** The component has been stopped in an orderly manner. Reported as failure.
    */
  val Stopped: AcsCommitmentHealthState =
    AcsCommitmentHealthState(
      CommitmentMetrics.HealthValues.Stopped,
      ComponentHealthState.ShutdownState,
    )

  /** The component has stopped with a failure. Reported as failure.
    */
  def failed(t: Throwable): AcsCommitmentHealthState =
    AcsCommitmentHealthState(
      CommitmentMetrics.HealthValues.Failed,
      ComponentHealthState.failed(t.getMessage),
    )

  /** The component has stopped with a failure or in an orderly manner, according to the passed
    * value. Reported as failure.
    */
  def stoppedFromTry(t: Try[?]): AcsCommitmentHealthState =
    t match {
      case Success(_) => Stopped
      case Failure(t) => failed(t)
    }

}

/** Wrapper for reporting the health always to the health component and the provided metric.
  */
final case class AcsCommitmentComponentHealthReporter(
    name: String,
    healthMetric: Gauge[Int],
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  val healthComponent: AtomicHealthComponent = new AtomicHealthComponent {
    override def name: String = AcsCommitmentComponentHealthReporter.this.name

    override protected def initialHealthState: ComponentHealthState =
      AcsCommitmentHealthState.NotInitialized.componentHealthState

    override protected def associatedHasRunOnClosing: HasRunOnClosing =
      new OnShutdownRunner.PureOnShutdownRunner(logger)

    override protected def logger: TracedLogger = AcsCommitmentComponentHealthReporter.this.logger
  }

  def reportHealth(health: AcsCommitmentHealthState)(implicit traceContext: TraceContext): Unit = {
    healthMetric.updateValue(health.metricValue)
    healthComponent.reportHealthState(health.componentHealthState)
  }
}
