// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalSequencerReference,
  SequencerReference,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.{ForceFlag, SynchronizerId}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.Inspectors.forAll

trait OffboardsSequencerNode {
  import org.scalatest.LoneElement.*
  import org.scalatest.matchers.should.Matchers.*

  // TODO(#22509) Introduce a single off-boarding command
  protected def offboardSequencer(
      synchronizerId: SynchronizerId,
      sequencerToOffboard: LocalSequencerReference,
      sequencersOnSynchronizer: NonEmpty[Seq[SequencerReference]],
      synchronizerOwners: Set[InstanceReference],
      isBftOrderer: Boolean,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val synchronizerOwnersNE = NonEmpty
      .from(synchronizerOwners)
      .getOrElse(throw new IllegalArgumentException("synchronizerOwners must not be empty"))

    // fetch the latest SequencerSynchronizerState mapping
    val seqState1 = sequencersOnSynchronizer.head1.topology.sequencers
      .list(store = synchronizerId)
      .headOption
      .getOrElse(fail("No sequencer state found"))
      .item

    // user-manual-entry-begin: SequencerOffboardingRemoveFromTopology
    // propose the SequencerSynchronizerState that removes the sequencer
    synchronizerOwnersNE
      .foreach(
        _.topology.sequencers.propose(
          synchronizerId,
          threshold = seqState1.threshold,
          active = seqState1.active.filterNot(_ == sequencerToOffboard.id),
        )
      )
    // user-manual-entry-end: SequencerOffboardingRemoveFromTopology

    BaseTest.eventually() {
      sequencersOnSynchronizer.head1.topology.sequencers
        .list(store = synchronizerId)
        .loneElement
        .item
        .active
        .forgetNE should not contain sequencerToOffboard.id

      // If the synchronizer is running on BFT sequencers, wait for the ordering topology to be updated as well
      //  before stopping the decommissioned sequencer node, else a 3-strong or less BFT ordering network
      //  may become stuck due to insufficient quorum.
      if (isBftOrderer)
        forAll(sequencersOnSynchronizer.forgetNE) { s =>
          s.bft
            .get_ordering_topology()
            .sequencerIds should not contain sequencerToOffboard.id
        }
    }

    // user-manual-entry-begin: SequencerOffboardingRemoveExclusiveKeys

    // Remove the OwnerToKeyMapping of the offboarded sequencer. Once the sequencer
    // has been removed from the synchronizer, its OTK becomes dangling and must be
    // removed by the remaining synchronizer owners using decentralized authorization.
    sequencersOnSynchronizer.head1.topology.owner_to_key_mappings
      .list(store = Some(synchronizerId), filterKeyOwnerUid = sequencerToOffboard.id.filterString)
      .headOption
      .foreach { offboardedSequencerOtk =>
        synchronizerOwnersNE
          .foreach(owner =>
            owner.topology.transactions.propose(
              offboardedSequencerOtk.item,
              synchronizerId,
              change = Remove,
              forceChanges = ForceFlag.AlienMember,
              mustFullyAuthorize = false,
            )
          )
      }

    // user-manual-entry-end: SequencerOffboardingRemoveExclusiveKeys

    // Verify that the OTK has been removed from the topology store.
    BaseTest.eventually() {
      sequencersOnSynchronizer.head1.topology.owner_to_key_mappings
        .list(
          store = Some(synchronizerId),
          filterKeyOwnerUid = sequencerToOffboard.id.filterString,
        ) should be(empty)
    }

    sequencerToOffboard.stop()
  }
}
