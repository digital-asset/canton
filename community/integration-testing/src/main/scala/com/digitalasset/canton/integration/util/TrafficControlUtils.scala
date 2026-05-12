// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ClockConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{BaseInspection, InstanceReference}
import com.digitalasset.canton.environment.CantonNode
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters

object TrafficControlUtils {

  // Free confirmation responses and no base cost
  // This means essentially only topology transactions and confirmation requests cost traffic
  // which makes it easier to make assertion on traffic spent by nodes
  // For maximum predictability in tests, disable ACS commitments
  val predictableTraffic: TrafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeLong.zero,
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.zero,
    freeConfirmationResponses = true,
  )

  private def runOnEachInitializedSynchronizer[T](f: InitializedSynchronizer => T)(implicit
      env: TestConsoleEnvironment
  ): List[T] =
    env.initializedSynchronizers.values.toList.map(f)

  /** Enable traffic control on all currently initialized synchronizers. Use this when the
    * environment is started manually. * * See
    * [[com.digitalasset.canton.integration.EnvironmentDefinition.withTrafficControl]] for *
    * parameter semantics.
    */
  def applyTrafficControl(
      syncSynchronizerOwnersTime: InstanceReference & BaseInspection[? <: CantonNode] => Unit,
      trafficControlParameters: TrafficControlParameters =
        TrafficControlParameters.default.copy(maxBaseTrafficAmount = NonNegativeLong.maxValue),
      topUpAllMembers: Boolean = false,
      disableCommitments: Boolean = false,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val isSimClock = env.actualConfig.parameters.clock match {
      case ClockConfig.SimClock => true
      case _ => false
    }

    // We first do all updates async
    runOnEachInitializedSynchronizer { sync =>
      // If we're not in simclock, wait for synchronizer owners to catch up with a recent wall clock time
      if (!isSimClock) {
        sync.synchronizerOwners
          .collect { case owner: InstanceReference with BaseInspection[CantonNode] =>
            owner
          }
          .foreach(syncSynchronizerOwnersTime)
      }

      sync.synchronizerOwners.foreach {
        _.topology.synchronizer_parameters.propose_update(
          synchronizerId = sync.synchronizerId,
          original =>
            original.update(
              trafficControl = Some(trafficControlParameters),
              reconciliationInterval =
                if (disableCommitments) config.PositiveDurationSeconds.ofDays(365)
                else original.reconciliationInterval,
            ),
          // Don't synchronize here to run the updates in parallel to speed it up
          synchronize = None,
        )
      }
    }

    // And then check on all synchronizers that traffic is enabled, to speed things up
    utils.retry_until_true {
      runOnEachInitializedSynchronizer { sync =>
        sync.allSequencerOwners.forall(
          _.topology.synchronizer_parameters
            .latest(sync.physicalSynchronizerId)
            .trafficControl
            .isDefined
        )
      }.forall(_ == true)
    }

    // Top up members if requested, in the same way (first send all top up requests async,
    // then wait for all of them to be effective)
    if (topUpAllMembers) {
      runOnEachInitializedSynchronizer { sync =>
        val allPayingMembers =
          sync.allActiveMediators.map(_.member) ++ sync.allParticipants.map(_.member)

        allPayingMembers.foreach { member =>
          sync.allSequencerOwners.foreach {
            _.traffic_control.set_traffic_balance(
              member,
              PositiveInt.one,
              NonNegativeLong.maxValue,
            )
          }
        }
      }

      utils.retry_until_true {
        runOnEachInitializedSynchronizer { sync =>
          val allPayingMembers =
            sync.allActiveMediators.map(_.member) ++ sync.allParticipants.map(_.member)

          val traffic = sync.allSequencerOwners.head.traffic_control
            .traffic_state_of_members_approximate(allPayingMembers)
            .trafficStates
          traffic.sizeIs == allPayingMembers.size && traffic.values.forall(
            _.extraTrafficPurchased == NonNegativeLong.maxValue
          )
        }.forall(_ == true)
      }
    }
  }
}
