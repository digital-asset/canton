// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.scenarios

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnection,
  SequencerConnectionPoolDelays,
  SequencerConnections,
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
  SynchronizerConnectionConfig,
  TrafficControlParameters,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleMacros,
  MediatorReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.chaining.*

class SynchronizersConfiguration(val synchronizers: NonEmpty[List[SynchronizerConfiguration]]) {
  def bootstrap(): List[(SynchronizerConfiguration, SynchronizerId)] =
    synchronizers.forgetNE.flatMap { synchronizer =>
      if (synchronizer.needsBootstrap) {
        println(s"Bootstrapping distributed synchronizer ${synchronizer.synchronizerName}")
        Some(synchronizer -> synchronizer.bootstrap())
      } else {
        println(
          s"Synchronizer ${synchronizer.synchronizerName} already initialized, skipping bootstrapping"
        )
        None
      }
    }

  def connect(
      participant: ParticipantReference,
      index: Int,
      sequencerTrustThreshold: PositiveInt,
      sequencerLivenessMargin: NonNegativeInt,
  ): Unit = if (participant.synchronizers.list_registered().isEmpty) {
    synchronizers.foreach(
      _.connect(participant, index, sequencerTrustThreshold, sequencerLivenessMargin)
    )
  } else participant.synchronizers.reconnect_all()
}

class SynchronizerConfiguration(
    val synchronizerName: String,
    sequencers: Seq[SequencerReference],
    mediators: Seq[MediatorReference],
)(implicit consoleEnvironment: ConsoleEnvironment) {
  import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*

  def needsBootstrap: Boolean =
    sequencers.nonEmpty && mediators.nonEmpty && sequencers.forall(s => !s.health.initialized())

  def bootstrap(): SynchronizerId = {
    val mediatorsWithoutReplica = mediators.active.distinctBy(_.id)
    ConsoleMacros.bootstrap
      .synchronizer(
        synchronizerName = synchronizerName,
        sequencers = sequencers.active,
        mediators = mediatorsWithoutReplica,
        synchronizerOwners = sequencers.active,
        synchronizerThreshold = PositiveInt.tryCreate(sequencers.active.size),
        staticSynchronizerParameters =
          StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        mediatorThreshold = PositiveInt.tryCreate(mediatorsWithoutReplica.size),
      )
      .logical
  }

  def enableTrafficControlWithHighBaseRate(synchronizerId: SynchronizerId): Unit = {
    val trafficControlParameters = TrafficControlParameters(
      // Give a large amount (1GB) of base traffic (free traffic). This allows traffic control to be
      // exercised without requiring traffic purchases to be done throughout the test
      maxBaseTrafficAmount = NonNegativeLong.tryCreate(1e9d.toLong),
      readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
      // Free traffic replenishes fully every second
      maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
      setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
      enforceRateLimiting = true,
      baseEventCost = NonNegativeLong.zero,
    )

    // Update topology to enable traffic control on the synchronizer
    sequencers.foreach { seq =>
      seq.topology.synchronizer_parameters.propose_update(
        synchronizerId = synchronizerId,
        _.update(trafficControl = Some(trafficControlParameters)),
      )
    }
    // Wait for the change to be effective
    sequencers.foreach(_.topology.synchronisation.await_idle())
  }

  def connect(
      participant: ParticipantReference,
      index: Int,
      sequencerTrustThreshold: PositiveInt,
      sequencerLivenessMargin: NonNegativeInt,
  ): Unit =
    NonEmpty.from(sequencerGroups).foreach { sequencerGroup =>
      val sequencerConnections = if (sequencerTrustThreshold.value > 1) {
        SequencerConnections
          .many(
            sequencerGroup,
            sequencerTrustThreshold,
            sequencerLivenessMargin,
            SubmissionRequestAmplification.NoAmplification,
            SequencerConnectionPoolDelays.default,
          )
          .getOrElse(sys.error("Failed to create sequencer connection"))
      } else {
        SequencerConnections.single(sequencerGroups(index % sequencerGroups.size))
      }

      participant.synchronizers
        .connect_by_config(
          SynchronizerConnectionConfig(synchronizerName, sequencerConnections)
        )
    }

  lazy val sequencerGroups: Seq[SequencerConnection] =
    sequencers
      .groupBy(_.id)
      .map { case (key, value) =>
        value
          .foldLeft(None: Option[SequencerConnection]) {
            case (Some(acc), endpoint) =>
              println(s"adding endpoint $endpoint to $acc for $key")
              Some(acc.addEndpoints(endpoint).getOrElse(sys.error("Failed to add endpoint")))
            case (None, endpoint) => Some(endpoint)
          }
      }
      .flatMap(_.toList)
      .toList
      .tap(sequencerGroups =>
        println(s"Sequencer groups for synchronizer $synchronizerName: $sequencerGroups")
      )
}

object SynchronizersConfiguration {
  // One synchronizer with all the sequencers and mediators
  def singleSynchronizer(implicit env: ConsoleEnvironment): SynchronizersConfiguration = {
    import env.*

    val d = new SynchronizerConfiguration(
      "synchronizer",
      sequencers.all,
      mediators.all,
    )

    new SynchronizersConfiguration(NonEmpty.apply(List, d))
  }

  // Two synchronizers, each using half the sequencers and mediators
  def twoSynchronizers(implicit
      env: ConsoleEnvironment
  ): Either[String, SynchronizersConfiguration] = {
    import env.*

    for {
      _ <- Either.cond(
        sequencers.all.sizeIs >= 2,
        (),
        s"Expecting at least two sequencers but found ${sequencers.all.size}",
      )
      _ <- Either.cond(
        mediators.all.sizeIs >= 2,
        (),
        s"Expecting at least two mediators but found ${mediators.all.size}",
      )

      (sequencersSynchronizer1, sequencersSynchronizer2) = sequencers.all
        .splitAt(sequencers.all.size / 2)
      (mediatorsSynchronizer1, mediatorsSynchronizer2) = mediators.all
        .splitAt(mediators.all.size / 2)

      d1 = new SynchronizerConfiguration(
        "synchronizer1",
        sequencersSynchronizer1,
        mediatorsSynchronizer1,
      )

      d2 = new SynchronizerConfiguration(
        "synchronizer2",
        sequencersSynchronizer2,
        mediatorsSynchronizer2,
      )
    } yield new SynchronizersConfiguration(NonEmpty.mk(List, d1, d2))
  }
}
