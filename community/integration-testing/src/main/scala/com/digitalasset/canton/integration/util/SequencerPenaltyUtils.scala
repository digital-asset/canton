// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}

object SequencerPenaltyUtils {

  /** Read the penalty for a given sequencer.
    *
    * A sequencer receives a penalty when a submission or a subscription fails with an error.
    * Integration tests can observe that a given event has taken place by checking penalties.
    *
    * NOTE: Penalties decay over time, so they should be checked soon after the event.
    * @return
    *   the current penalty that `participant` gives to `sequencer` on the given synchronizer, or 0
    *   if the participant is not connected to the synchronizer or has not penalized the sequencer
    */
  def sequencerPenalty(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      sequencer: SequencerId,
  ): Double =
    participant.underlying
      .flatMap(_.sync.readyConnectedSynchronizerById(synchronizerId))
      .flatMap(_.sequencerClient.connectionPool.connectionPenalties.get(sequencer))
      .fold(0.0)(_.value)
}
