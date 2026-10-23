// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.option.*
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities

class SequencerPruningStatusTest extends BaseTestWordSpec {
  def ts(epochSeconds: Int): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)

  "safe pruning point" should {
    "use the earliest acknowledgment timestamp if all members have recorded read clients" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(15),
        members = Set(
          SequencerMemberStatus.Active(
            member = DefaultTestIdentities.daSequencerId,
            registeredAt = ts(8),
            ts(10).some,
          ),
          SequencerMemberStatus.Active(
            member = DefaultTestIdentities.daMediator,
            registeredAt = ts(7),
            ts(12).some,
          ),
        ),
      )

      // should pick the earliest ack timestamp
      status.safePruningTimestamp shouldBe ts(10)
    }

    "use the registered at time for a member if no data has yet been read" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(15),
        members = Set(
          SequencerMemberStatus.Active(
            member = DefaultTestIdentities.daSequencerId,
            registeredAt = ts(8),
            ts(15).some,
          ),
          SequencerMemberStatus.Active(
            member = DefaultTestIdentities.daMediator,
            registeredAt = ts(7),
            None,
          ),
        ),
      )

      // should pick the mediator's registeredAt timestamp
      status.safePruningTimestamp shouldBe ts(7)
    }

    "return the lowest registered at if data has not been read" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(12),
        members = Set(
          SequencerMemberStatus.Active(
            member = DefaultTestIdentities.daSequencerId,
            registeredAt = ts(3),
            None,
          )
        ),
      )

      status.safePruningTimestamp shouldBe ts(3)
    }

    "ignore member entirely if they are onboarding or disabled" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(12),
        members = Set(
          SequencerMemberStatus.Onboarding(
            member = DefaultTestIdentities.daSequencerId
          ),
          SequencerMemberStatus.Disabled(
            member = DefaultTestIdentities.daSequencerId,
            registeredAt = ts(3).some,
            ts(4).some,
          ),
          SequencerMemberStatus.Active(
            member = DefaultTestIdentities.daMediator,
            registeredAt = ts(2),
            ts(12).some,
          ),
        ),
      )

      // as although the sequencer hasn't read up until this point, it is disabled
      status.safePruningTimestamp shouldBe ts(12)
    }
  }

  "v30 encoding" should {
    "be possible once members without a registration timestamp are left out" in {
      val active = SequencerMemberStatus.Active(
        member = DefaultTestIdentities.daMediator,
        registeredAt = ts(2),
        ts(5).some,
      )
      val disabled = SequencerMemberStatus.Disabled(
        member = DefaultTestIdentities.daSequencerId,
        registeredAt = ts(3).some,
        None,
      )
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(12),
        members = Set(
          SequencerMemberStatus.Onboarding(member = DefaultTestIdentities.participant1),
          SequencerMemberStatus.Disabled(
            member = DefaultTestIdentities.participant2,
            registeredAt = None,
            None,
          ),
          active,
          disabled,
        ),
      )

      status.toProtoV30.left.value should include("Cannot serialize")
      val statusP = status.withoutUnregisteredMembers.toProtoV30.value
      SequencerPruningStatus.fromProtoV30(statusP).value shouldBe status.copy(
        members = Set(active, disabled)
      )
    }
  }
}
