// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.{v30, v31}
import com.digitalasset.canton.synchronizer.sequencer.SequencerMemberStatus.{
  Active,
  Disabled,
  Onboarding,
}
import com.digitalasset.canton.topology.DefaultTestIdentities
import org.scalatest.wordspec.AnyWordSpec

class SequencerMemberStatusTest extends AnyWordSpec with BaseTest {
  val member = DefaultTestIdentities.participant1
  val ts1 = CantonTimestamp.Epoch
  val ts2 = CantonTimestamp.Epoch.plusSeconds(42)

  val onboarding = Onboarding(member)
  val active = Active(member, ts1, Some(ts2))
  val disabled = Disabled(member, Some(ts1), Some(ts2))
  val disabledWhileOnboarding = Disabled(member, None, None)

  "SequencerMemberStatus" should {
    "be able to parse v30 protobuf messages" in {
      def olderProto(enabled: Boolean): v30.SequencerMemberStatus =
        v30.SequencerMemberStatus(
          member = member.toProtoPrimitive,
          registeredAt = ts1.toProtoPrimitive,
          lastAcknowledged = None,
          enabled = enabled,
        )

      inside(SequencerMemberStatus.fromProtoV30(olderProto(enabled = true)).value) {
        case Active(m, registeredAt, lastAck) =>
          m shouldBe member
          registeredAt shouldBe ts1
          lastAck shouldBe None
      }
      inside(SequencerMemberStatus.fromProtoV30(olderProto(enabled = false)).value) {
        case Disabled(m, registeredAt, lastAck) =>
          m shouldBe member
          registeredAt shouldBe Some(ts1)
          lastAck shouldBe None
      }
    }

    "be able to parse v31 protobuf message onboarding" in {
      val proto = v31.SequencerMemberStatus(
        member = member.toProtoPrimitive,
        status = v31.SequencerMemberStatus.Status.Onboarding(
          v31.SequencerMemberStatus.Onboarding()
        ),
      )

      inside(SequencerMemberStatus.fromProtoV31(proto).value) { case Onboarding(m) =>
        m shouldBe member
      }
    }

    "be able to parse v31 protobuf message active" in {
      val proto = v31.SequencerMemberStatus(
        member = member.toProtoPrimitive,
        status = v31.SequencerMemberStatus.Status.Active(
          v31.SequencerMemberStatus.Active(
            registeredAt = ts1.toProtoPrimitive,
            lastAcknowledged = Some(ts2.toProtoPrimitive),
          )
        ),
      )

      inside(SequencerMemberStatus.fromProtoV31(proto).value) { case Active(m, rt, at) =>
        m shouldBe member
        rt shouldBe ts1
        at shouldBe Some(ts2)
      }
    }

    "be able to parse v31 protobuf message disabled" in {
      val proto = v31.SequencerMemberStatus(
        member = member.toProtoPrimitive,
        status = v31.SequencerMemberStatus.Status.Disabled(
          v31.SequencerMemberStatus.Disabled(
            registeredAt = Some(ts1.toProtoPrimitive),
            lastAcknowledged = Some(ts2.toProtoPrimitive),
          )
        ),
      )

      inside(SequencerMemberStatus.fromProtoV31(proto).value) { case Disabled(m, rt, at) =>
        m shouldBe member
        rt shouldBe Some(ts1)
        at shouldBe Some(ts2)
      }
    }

    "be able to produce proto v30 messages" in {
      active.toProtoV30.value shouldBe v30.SequencerMemberStatus(
        member = member.toProtoPrimitive,
        registeredAt = active.registeredAt.toProtoPrimitive,
        lastAcknowledged = active.lastAcknowledged.map(_.toProtoPrimitive),
        enabled = true,
      )

      disabled.toProtoV30.value shouldBe v30.SequencerMemberStatus(
        member = member.toProtoPrimitive,
        registeredAt = disabled.registeredAt.value.toProtoPrimitive,
        lastAcknowledged = disabled.lastAcknowledged.map(_.toProtoPrimitive),
        enabled = false,
      )
    }

    // v30 has no representation for a member without a registration timestamp, and such members
    // are only created from PV37 onwards, where the snapshot uses v31 instead.
    "refuse to produce a proto v30 message for an onboarding member" in {
      onboarding.toProtoV30.left.value should include("Cannot serialize the onboarding member")
    }

    "refuse to produce a proto v30 message for a member disabled while onboarding" in {
      disabledWhileOnboarding.toProtoV30.left.value should include(
        "Cannot serialize the disabled member"
      )
    }

    "be able to produce proto v31 messages" in {
      onboarding.toProtoV31.status shouldBe v31.SequencerMemberStatus.Status.Onboarding(
        v31.SequencerMemberStatus.Onboarding()
      )
      active.toProtoV31.status shouldBe v31.SequencerMemberStatus.Status.Active(
        v31.SequencerMemberStatus.Active(
          active.registeredAt.toProtoPrimitive,
          active.lastAcknowledged.map(_.toProtoPrimitive),
        )
      )
      disabled.toProtoV31.status shouldBe v31.SequencerMemberStatus.Status.Disabled(
        v31.SequencerMemberStatus.Disabled(
          disabled.registeredAt.map(_.toProtoPrimitive),
          disabled.lastAcknowledged.map(_.toProtoPrimitive),
        )
      )
      disabledWhileOnboarding.toProtoV31.status shouldBe v31.SequencerMemberStatus.Status.Disabled(
        v31.SequencerMemberStatus.Disabled(registeredAt = None, lastAcknowledged = None)
      )
    }
  }
}
