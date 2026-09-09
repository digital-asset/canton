// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.Eval
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.pretty.Pretty.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.health.v1.HealthCheckResponse.ServingStatus

trait HealthService
    extends CloseableHealthElement
    with CompositeHealthElement[String, HealthQuasiComponent] {
  override type State = ServingStatus
  def dependencies: Seq[HealthQuasiComponent]

  override protected def prettyState: Pretty[ServingStatus] = Pretty[ServingStatus]
}

/** A [[DependenciesHealthService]] aggregates [[CloseableHealthComponent]]s under critical and soft
  * dependencies. Services are queryable through their name in the gRPC Health Check service. Both
  * critical and soft dependencies are reported under their names too.
  *
  * The state of the [[DependenciesHealthService]] is
  * [[io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING]] if and only if none of the
  * critical dependencies have failed. Soft dependencies are merely reported as dependencies, but do
  * not influence the status of the [[DependenciesHealthService]] itself.
  *
  * Use `serviceCriticalDependencies` to put other health services into critical dependencies.
  * Notably, only the serving status of the service is considered, not the status of its
  * dependencies. This allows using the initial state of the dependent health service, i.e. during a
  * startup.
  */
final class DependenciesHealthService(
    override val name: String,
    override protected val logger: TracedLogger,
    override protected val timeouts: ProcessingTimeout,
    private val criticalDependencies: Seq[HealthQuasiComponent],
    private val softDependencies: Eval[Seq[HealthQuasiComponent]],
    private val serviceCriticalDependencies: Seq[HealthService],
) extends HealthService {

  alterDependencies(
    remove = Set.empty,
    add = criticalDependencies.map(dep => dep.name -> dep).toMap,
  )

  override protected def closingState: ServingStatus = ServingStatus.NOT_SERVING

  override protected def combineDependentStates: ServingStatus =
    if (
      criticalDependencies.forall(!_.isFailed) && serviceCriticalDependencies.forall(
        _.getState == ServingStatus.SERVING
      )
    ) ServingStatus.SERVING
    else ServingStatus.NOT_SERVING

  override protected def initialHealthState: ServingStatus =
    if (criticalDependencies.isEmpty) ServingStatus.SERVING else ServingStatus.NOT_SERVING

  // This updates this service on `serviceCriticalDependencies` changes
  serviceCriticalDependencies.foreach { service =>
    service
      .registerOnHealthChange(new HealthListener {
        override def name: String = s"critical-service-dependencies-for-${service.name}"
        override def poke()(implicit traceContext: TraceContext): Unit = refreshFromDependencies()
      })
      .discard
  }

  override def dependencies: Seq[HealthQuasiComponent] =
    criticalDependencies ++ softDependencies.value
}

object DependenciesHealthService {
  def apply(
      name: String,
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
      criticalDependencies: Seq[HealthQuasiComponent] = Seq.empty,
      softDependencies: Eval[Seq[HealthQuasiComponent]] = Eval.now(Seq.empty),
      serviceCriticalDependencies: Seq[HealthService] = Seq.empty,
  ): DependenciesHealthService =
    new DependenciesHealthService(
      name,
      logger,
      timeouts,
      criticalDependencies,
      softDependencies,
      serviceCriticalDependencies,
    )

  implicit val prettyServiceHealth: Pretty[DependenciesHealthService] = prettyOfClass(
    param("name", _.name.unquoted),
    param("state", _.getState),
  )
}
