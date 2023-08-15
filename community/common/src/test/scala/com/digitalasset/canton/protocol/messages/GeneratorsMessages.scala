// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.CantonTimestampSecond
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsMessages {
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.data.GeneratorsData.*

  implicit val acsCommitmentArb = Arbitrary(
    for {
      domainId <- implicitly[Arbitrary[DomainId]].arbitrary
      sender <- implicitly[Arbitrary[ParticipantId]].arbitrary
      counterParticipant <- implicitly[Arbitrary[ParticipantId]].arbitrary

      periodFrom <- implicitly[Arbitrary[CantonTimestampSecond]].arbitrary
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = CommitmentPeriod(periodFrom, periodDuration)

      commitment <- byteStringArb.arbitrary
      protocolVersion <- representativeProtocolVersionGen(AcsCommitment)
    } yield AcsCommitment.create(
      domainId,
      sender,
      counterParticipant,
      period,
      commitment,
      protocolVersion.representative,
    )
  )
}
