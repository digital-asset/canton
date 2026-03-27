// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.environment.{
  CantonNodeBootstrapCommonArguments,
  NodeFactoryArguments,
}
import com.digitalasset.canton.error.FatalError
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbLockCounters, StorageFactory, StorageMultiFactory}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

trait SequencerNodeBootstrapFactory {

  def create(
      arguments: NodeFactoryArguments[
        SequencerNodeConfig,
        SequencerNodeParameters,
        SequencerMetrics,
      ]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): Either[String, SequencerNodeBootstrap]
}

object SequencerNodeBootstrapFactoryImpl extends SequencerNodeBootstrapFactory {
  override def create(
      arguments: NodeFactoryArguments[
        SequencerNodeConfig,
        SequencerNodeParameters,
        SequencerMetrics,
      ]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): Either[String, SequencerNodeBootstrap] = {

    val storageFactory = new StorageMultiFactory(
      arguments.config.storage,
      exitOnFatalFailures = arguments.parameters.exitOnFatalFailures,
      arguments.config.replication,
      onActive = logger =>
        FatalError.exitOnFatalError(
          "Sequencer storage went active from passive. This should never happen, considering this Sequencer's storage should never go passive",
          logger,
        )(TraceContext.empty),
      onPassive = logger =>
        FatalError.exitOnFatalError(
          "Sequencer storage went passive. This indicates another process is accessing this Sequencer's database simultaneously, which is not permitted.",
          logger,
        )(TraceContext.empty),
      mustStayActive = true,
      DbLockCounters.SEQUENCER_INIT,
      DbLockCounters.SEQUENCER_INIT_WORKER,
      arguments.futureSupervisor,
      arguments.loggerFactory,
      None,
    )

    toNodeCommonArguments(arguments, storageFactory)
      .map(new SequencerNodeBootstrap(_))
  }

  private def toNodeCommonArguments(
      arguments: NodeFactoryArguments[
        SequencerNodeConfig,
        SequencerNodeParameters,
        SequencerMetrics,
      ],
      storageFactory: StorageFactory,
  ): Either[String, CantonNodeBootstrapCommonArguments[
    SequencerNodeConfig,
    SequencerNodeParameters,
    SequencerMetrics,
  ]] =
    arguments
      .toCantonNodeBootstrapCommonArguments(
        storageFactory,
        Option.empty[ReplicaManager],
      )

}
