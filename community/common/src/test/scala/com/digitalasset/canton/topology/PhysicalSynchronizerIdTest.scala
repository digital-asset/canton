// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt.{one, two, zero}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.version.ProtocolVersion.{latest, v35, v36}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class PhysicalSynchronizerIdTest extends AnyWordSpec with EitherValues with Matchers {
  "PhysicalSynchronizerId" should {
    "parsed from string" in {
      val namespace: Namespace = Namespace(Fingerprint.tryFromString("default"))
      val lsid1: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da", namespace))
      val lsid2: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da-second", namespace))
      val pv = latest

      val str1 = s"da::default::${pv.toString}-0"
      val str2 = s"da-second::default::${pv.toString}-1"

      PhysicalSynchronizerId.fromString(str1).value shouldBe PhysicalSynchronizerId(lsid1, zero, pv)
      PhysicalSynchronizerId.fromString(str2).value shouldBe PhysicalSynchronizerId(lsid2, one, pv)
    }

    "be properly ordered" in {
      val namespace: Namespace = Namespace(Fingerprint.tryFromString("default"))
      val lsid1: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da1", namespace))
      val lsid2: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da2", namespace))

      val inCorrectOrder = List(
        PhysicalSynchronizerId(lsid1, zero, v35),
        PhysicalSynchronizerId(lsid1, zero, v36),
        PhysicalSynchronizerId(lsid1, two, v35),
        PhysicalSynchronizerId(lsid1, two, v36),
        PhysicalSynchronizerId(lsid2, zero, v35),
        PhysicalSynchronizerId(lsid2, zero, v36),
        PhysicalSynchronizerId(lsid2, one, v35),
        PhysicalSynchronizerId(lsid2, one, v36),
      )
      Random.shuffle(inCorrectOrder).sorted shouldBe inCorrectOrder

      val inCorrectOptionOrder = None :: inCorrectOrder.map(Some(_))
      Random.shuffle(inCorrectOptionOrder).sorted shouldBe inCorrectOptionOrder
    }

    "parse non-supported PVs with OpaquePhysicalSynchronizerId" in {
      val namespace: Namespace = Namespace(Fingerprint.tryFromString("default"))
      val lsid1: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da1", namespace))
      val lsid2: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da2", namespace))

      val opaqueSupported = OpaquePhysicalSynchronizerId(lsid1, zero, latest.v)
      val opaqueSupportedParsed = opaqueSupported.parseAsPhysical
      opaqueSupportedParsed shouldBe Right(PhysicalSynchronizerId(lsid1, zero, latest))
      opaqueSupportedParsed.value.opaque == opaqueSupported shouldBe true

      val opaqueUnsupported = OpaquePhysicalSynchronizerId(lsid2, one, 9999)
      val opaqueUnsupportedParsed = opaqueUnsupported.parseAsPhysical
      opaqueUnsupportedParsed.isLeft shouldBe true
    }
  }
}
