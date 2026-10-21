// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class SequencerStoreTestInMemory extends AsyncWordSpec with BaseTest with SequencerStoreTest {

  "InMemorySequencerStore" should {
    behave like sequencerStore(() =>
      new InMemorySequencerStore(
        protocolVersion = testedProtocolVersion,
        sequencerMember = sequencerMember,
        blockSequencerMode = true,
        loggerFactory = loggerFactory,
        timeouts = timeouts,
        sequencerMetrics = sequencerMetrics(),
      )
    )
  }

  "the member cache" should {
    def mkStaleStore() = new StaleRegistrationSequencerStore(
      protocolVersion = testedProtocolVersion,
      sequencerMember = sequencerMember,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
      sequencerMetrics = sequencerMetrics(),
    )
    val alice = ParticipantId("alice")
    val ts1 = CantonTimestamp.ofEpochSecond(1)
    val ts2 = CantonTimestamp.ofEpochSecond(2)

    "not be overwritten by an onboarding registration racing with a registration" in {
      val store = mkStaleStore()
      for {
        registered <- store.registerMember(alice, ts2)
        _ <- store.registerOnboardingMember(alice, ts1)
        lookedUp <- store.lookupMember(alice)
      } yield lookedUp shouldBe Some(RegisteredMember.Active(registered.memberId, ts2))
    }

    "not be overwritten by a registration racing with disabling the member" in {
      val store = mkStaleStore()
      for {
        registered <- store.registerMember(alice, ts1)
        _ <- store.disableMember(alice)
        _ <- store.registerMember(alice, ts2)
        lookedUp <- store.lookupMember(alice)
      } yield lookedUp shouldBe Some(RegisteredMember.Disabled(registered.memberId, Some(ts1)))
    }
  }
}

/** Returns the member as onboarding even if it is already registered, and as active even if it is
  * already disabled, as happens when a registration reads the member before a concurrent
  * registration or disabling completes
  */
private class StaleRegistrationSequencerStore(
    protocolVersion: ProtocolVersion,
    sequencerMember: Member,
    loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
    sequencerMetrics: SequencerMetrics,
)(implicit executionContext: ExecutionContext)
    extends InMemorySequencerStore(
      protocolVersion,
      sequencerMember,
      blockSequencerMode = true,
      loggerFactory,
      timeouts,
      sequencerMetrics,
    ) {
  protected override def registerOnboardingMemberInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[RegisteredMember] =
    super
      .registerOnboardingMemberInternal(member, timestamp)
      .map(registeredMember => RegisteredMember.Onboarding(registeredMember.memberId))

  protected override def registerMemberInternal(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[RegisteredMember] =
    super
      .registerMemberInternal(member, timestamp)
      .map(registeredMember =>
        RegisteredMember.Active(
          registeredMember.memberId,
          registeredMember.registeredFrom.getOrElse(timestamp),
        )
      )
}
