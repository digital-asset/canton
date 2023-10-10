// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.pretty.Pretty.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.health.v1.HealthCheckResponse.ServingStatus

object HealthService {
  implicit val prettyServiceHealth: Pretty[HealthService] = prettyOfClass(
    param("name", _.name.unquoted),
    param("state", _.getState),
  )
}

final case class HealthService(
    name: String,
    criticalDependencies: Seq[BaseHealthComponent] = Seq.empty,
    softDependencies: Seq[BaseHealthComponent] = Seq.empty,
) extends HealthElement {
  self =>
  override type State = ServingStatus

  criticalDependencies.foreach(c =>
    c.registerOnHealthChange((_: c.type, _: c.type#State, tc: TraceContext) =>
      listeners.get().foreach(_(self, getState, tc))
    )
  )

  override def registerOnHealthChange(listener: HealthListener): Unit = {
    super.registerOnHealthChange(listener)
  }

  override def getState: ServingStatus = {
    if (criticalDependencies.forall(!_.isFailed)) ServingStatus.SERVING
    else ServingStatus.NOT_SERVING
  }

  def dependencies: Seq[BaseHealthComponent] = criticalDependencies ++ softDependencies
}
