// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.{ConsoleMacros, InstanceReference, SequencerReference}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.topology.PhysicalSynchronizerId

import scala.concurrent.duration.DurationInt

trait OnboardsNewSequencerNode {

  protected val bftSequencerPlugin: Option[UseBftSequencer] = None

  protected def setUpAdditionalConnections(
      existingSequencerReference: SequencerReference,
      newSequencerReference: SequencerReference,
  ): Unit = bftSequencerPlugin.foreach { plugin =>
    plugin.p2pEndpoints.get.foreach { endpoints =>
      val existingSequencerEndpoint =
        endpoints(InstanceName.tryCreate(existingSequencerReference.name))
      // user-manual-entry-begin: BftSequencerAddPeerEndpoint
      newSequencerReference.bft.add_peer_endpoint(existingSequencerEndpoint)
    // existingSequencerReference.bft.add_peer_endpoint(newSequencerEndpoint) // Optional, one direction is enough
    // user-manual-entry-end: BftSequencerAddPeerEndpoint
    }
  }

  protected def onboardNewSequencer(
      synchronizerId: PhysicalSynchronizerId,
      newSequencer: SequencerReference,
      existingSequencer: SequencerReference,
      synchronizerOwners: Set[InstanceReference],
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Split into 2 branches for nicer documentation
    if (bftSequencerPlugin.isDefined) {
      // user-manual-entry-begin: DynamicallyOnboardBftSequencer
      bootstrap
        .onboard_new_sequencer(
          synchronizerId.logical,
          newSequencer,
          existingSequencer,
          synchronizerOwners,
          // Avoid issues if things are slow
          customCommandTimeout = Some(config.NonNegativeDuration.tryFromDuration(2.minutes)),
          isBftSequencer = true,
        )
      // user-manual-entry-end: DynamicallyOnboardBftSequencer
    } else {
      bootstrap
        .onboard_new_sequencer(
          synchronizerId.logical,
          newSequencer,
          existingSequencer,
          synchronizerOwners,
        )
    }

    setUpAdditionalConnections(existingSequencer, newSequencer)

    // user-manual-entry-begin: DynamicallyOnboardBftSequencer-wait-for-initialized
    newSequencer.health.wait_for_initialized()
    // user-manual-entry-end: DynamicallyOnboardBftSequencer-wait-for-initialized

    // TODO(#30996): implement disseminations retry and remove the following logic
    if (bftSequencerPlugin.isDefined) {
      // Don't disseminate anything until the new sequencer is part of the topology, as else
      //  disseminations could fail and be stuck.
      ConsoleMacros.utils.retry_until_true(env.commandTimeouts.bounded) {
        newSequencer.bft
          .get_ordering_topology()
          .sequencerIds
          .contains(newSequencer.id)
      }
    }
  }
}
