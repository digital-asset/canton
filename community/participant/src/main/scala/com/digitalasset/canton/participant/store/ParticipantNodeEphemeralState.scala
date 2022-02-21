// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}

import scala.concurrent.ExecutionContext

/** Some of the state of a participant that is not tied to a domain and is kept only in memory.
  */
class ParticipantNodeEphemeralState(
    val participantEventPublisher: ParticipantEventPublisher
)

object ParticipantNodeEphemeralState {
  def apply(
      participantId: ParticipantId,
      persistentState: ParticipantNodePersistentState,
      clock: Clock,
      maxDeduplicationDuration: NonNegativeFiniteDuration,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ParticipantNodeEphemeralState = {
    val participantEventPublisher = new ParticipantEventPublisher(
      participantId,
      persistentState.participantEventLog,
      persistentState.multiDomainEventLog,
      clock,
      maxDeduplicationDuration.unwrap,
      timeouts,
      loggerFactory,
    )
    new ParticipantNodeEphemeralState(
      participantEventPublisher
    )
  }
}
