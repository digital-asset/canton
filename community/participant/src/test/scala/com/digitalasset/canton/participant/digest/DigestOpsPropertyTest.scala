// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.digest

import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.AcsUpdate
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.topology.GeneratorsTopology
import com.digitalasset.canton.{BaseTest, LedgerParticipantId, LfPartyId, ReassignmentCounter}
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DigestOpsPropertyTest
    extends AnyPropSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Inside {

  import DigestOpsPropertyTest.*

  private lazy val generatorsTopology =
    new GeneratorsTopology(BaseTest.testedProtocolVersion)

  import generatorsTopology.*

  private val consistencyTestInputs: Gen[ConsistencyTestInput] = for {
    participant <- participantIdArb.arbitrary
    counterParticipant <- participantIdArb.arbitrary
    otherCounterParticipant <-
      participantIdArb.arbitrary // Not used for property check, but added to the dataset to make it more realistic
    parties <- Gen.listOfN(5, partyIdArb.arbitrary)

    participantParties <- Gen.atLeastOne(parties)
    counterParticipantParties <- Gen.atLeastOne(parties)
    otherCounterParticipantParties <- Gen.atLeastOne(parties)
  } yield ConsistencyTestInput(
    stakeholders = DigestOps
      .invertMap(
        Map(
          participant.toLf -> participantParties.map(_.toLf).toSet,
          counterParticipant.toLf -> counterParticipantParties.map(_.toLf).toSet,
          otherCounterParticipant.toLf -> otherCounterParticipantParties.map(_.toLf).toSet,
        )
      )
      .map { case (k, v) => k -> v.toSeq },
    participant = participant.toLf,
    counterParticipant = counterParticipant.toLf,
  )

  property("consistency between counter-participants") {
    forAll(consistencyTestInputs) { input =>
      val updateOnParticipant = AcsUpdate(
        stakeholders = input.stakeholders,
        locallyHostedStakeholders = input.locallyHostedStakeholders(input.participant),
        cid = contractId,
        rc = ReassignmentCounter.Genesis,
        isActivation = true,
      )

      val updateOnCounterParticipant = AcsUpdate(
        stakeholders = input.stakeholders,
        locallyHostedStakeholders = input.locallyHostedStakeholders(input.counterParticipant),
        cid = contractId,
        rc = ReassignmentCounter.Genesis,
        isActivation = true,
      )

      val deltaFromParticipantOpt = DigestOps
        .computeDeltas(
          input.participant,
          updateOnParticipant,
        )
        .collectFirst {
          case delta: DigestDelta.Participant if delta.participantId == input.counterParticipant =>
            delta
        }

      val deltaFromCounterParticipantOpt = DigestOps
        .computeDeltas(
          input.counterParticipant,
          updateOnCounterParticipant,
        )
        .collectFirst {
          case delta: DigestDelta.Participant if delta.participantId == input.participant => delta
        }

      inside((deltaFromParticipantOpt, deltaFromCounterParticipantOpt)) {
        case (Some(deltaFromParticipant), Some(deltaFromCounterParticipant)) =>
          deltaFromParticipant.digest shouldBe deltaFromCounterParticipant.digest
      }

    }
  }
}

object DigestOpsPropertyTest {

  private val contractId = ExampleTransactionFactory.unsuffixedId(1)

  final case class ConsistencyTestInput(
      stakeholders: Map[LfPartyId, Seq[LedgerParticipantId]],
      participant: LedgerParticipantId,
      counterParticipant: LedgerParticipantId,
  ) {
    def locallyHostedStakeholders(participant: LedgerParticipantId): Seq[LfPartyId] =
      stakeholders.collect {
        case (party, participants) if participants.contains(participant) =>
          party
      }.toSeq
  }
}
