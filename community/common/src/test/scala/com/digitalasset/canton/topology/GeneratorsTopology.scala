// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.Fingerprint
import magnolify.scalacheck.auto.*
import org.scalacheck.Arbitrary

object GeneratorsTopology {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.version.GeneratorsVersion.*

  implicit val fingerprintArb: Arbitrary[Fingerprint] = Arbitrary(
    string68Arb.arbitrary.map(Fingerprint.tryFromString)
  )
  implicit val namespaceArb: Arbitrary[Namespace] = Arbitrary(
    fingerprintArb.arbitrary.map(Namespace(_))
  )
  implicit val uniqueIdentifierArb: Arbitrary[UniqueIdentifier] = Arbitrary(
    for {
      id <- string185Arb.arbitrary
      fp <- string68Arb.arbitrary
    } yield UniqueIdentifier.tryCreate(id.str, fp.str)
  )
  implicit val identityArb: Arbitrary[Identity] = genArbitrary
  implicit val synchronizerIdArb: Arbitrary[SynchronizerId] = genArbitrary
  implicit val mediatorIdArb: Arbitrary[MediatorId] = genArbitrary
  implicit val sequencerIdArb: Arbitrary[SequencerId] = genArbitrary
  implicit val memberArb: Arbitrary[Member] = genArbitrary
  implicit val partyIdArb: Arbitrary[PartyId] = genArbitrary

  implicit val physicalSynchronizerIdArb: Arbitrary[PhysicalSynchronizerId] = Arbitrary(for {
    synchronizerId <- synchronizerIdArb.arbitrary
    protocolVersion <- protocolVersionArb.arbitrary
  } yield PhysicalSynchronizerId(synchronizerId, protocolVersion))
}
