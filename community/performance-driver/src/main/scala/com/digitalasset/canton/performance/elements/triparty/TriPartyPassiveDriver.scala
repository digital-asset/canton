// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.triparty

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.javaapi.data.Identifier
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.Connectivity
import com.digitalasset.canton.performance.PartyRole.TriPartyProvider
import com.digitalasset.canton.performance.elements.{DriverControl, ParticipantDriver}
import com.digitalasset.canton.performance.model.java.orchestration.ParticipantFlag
import com.digitalasset.canton.topology.SynchronizerId
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContextExecutor, Future}

class TriPartyPassiveDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterLf: LfPartyId,
    role: TriPartyProvider,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    baseSynchronizerId: SynchronizerId,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends ParticipantDriver(
      connectivity,
      partyLf,
      masterLf,
      role,
      registerGenerator = false,
      prefix,
      metricsFactory,
      loggerFactory,
      control,
      baseSynchronizerId,
    ) {
  override protected def subscribeToTemplates: Seq[Identifier] = Seq.empty

  override def flush(): Boolean = {
    ensureFlag(ParticipantFlag.FINISHED)
    false
  }

  override def start(): Future[Either[String, Unit]] = {
    done_.success(()) // we don't need to wait until we are done
    super.start()
  }

}
