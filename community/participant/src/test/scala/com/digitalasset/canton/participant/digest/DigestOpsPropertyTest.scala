// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.digest

import cats.syntax.traverse.*
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  ContractChange,
  ContractChangeBatch,
}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.topology.GeneratorsTopology
import com.digitalasset.canton.{
  BaseTest,
  LedgerParticipantId,
  LfPartyId,
  ReassignmentCounter,
  TestEssentials,
}
import org.scalacheck.Gen
import org.scalacheck.cats.instances.GenInstances.*
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DigestOpsPropertyTest
    extends AnyPropSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Inside
    with TestEssentials {

  private lazy val generatorsTopology =
    new GeneratorsTopology(BaseTest.testedProtocolVersion)

  import generatorsTopology.*

  private def contractChangesForParticipants(
      stakeholders: Map[LfPartyId, Set[LedgerParticipantId]],
      participant: LedgerParticipantId,
      counterparticipant: LedgerParticipantId,
      contractId: LfContractId.V1,
  ): Gen[(ContractChange, ContractChange)] = {
    def locallyHostedStakeholders(participant: LedgerParticipantId): Seq[LfPartyId] =
      stakeholders.collect {
        case (party, participants) if participants.contains(participant) =>
          party
      }.toSeq
    (
      ContractChange(
        stakeholders = stakeholders.keySet,
        locallyHostedStakeholders = locallyHostedStakeholders(participant),
        cid = contractId,
        rc = ReassignmentCounter.Genesis,
        isActivation = true,
      ),
      ContractChange(
        stakeholders = stakeholders.keySet,
        locallyHostedStakeholders = locallyHostedStakeholders(counterparticipant),
        cid = contractId,
        rc = ReassignmentCounter.Genesis,
        isActivation = true,
      ),
    )
  }

  private val consistencyTestInputs =
    for {
      participant <- participantIdArb.arbitrary
      counterparticipant <- participantIdArb.arbitrary
      // Not used for property check, but added to the dataset to make it more realistic
      otherCounterParticipant <- participantIdArb.arbitrary

      parties <- Gen.listOfN(5, partyIdArb.arbitrary)
      participantParties <- Gen.atLeastOne(parties)
      counterParticipantParties <- Gen.atLeastOne(parties)
      otherCounterParticipantParties <- Gen.atLeastOne(parties)

      allParties = DigestOps
        .invertMap(
          Map(
            participant.toLf -> participantParties.map(_.toLf).toSet,
            counterparticipant.toLf -> counterParticipantParties.map(_.toLf).toSet,
            otherCounterParticipant.toLf -> otherCounterParticipantParties.map(_.toLf).toSet,
          )
        )

      cids = Iterator.from(1)
      numContracts <- Gen.chooseNum(1, 10)
      changes <- cids.take(numContracts).toSeq.traverse { idx =>
        val cid = ExampleTransactionFactory.unsuffixedId(idx)
        for {
          localParties <- Gen.atLeastOne(participantParties)
          remoteParties <- Gen.atLeastOne(counterParticipantParties)
          otherParties <- Gen.atLeastOne(otherCounterParticipantParties)
          stakeholdersOfContract = (localParties.toSet ++ remoteParties ++ otherParties).map(_.toLf)
          change <- contractChangesForParticipants(
            allParties.view.filterKeys(stakeholdersOfContract).toMap,
            participant.toLf,
            counterparticipant.toLf,
            cid,
          )
        } yield change
      }
    } yield {
      val (forParticipant, forCounterparticipant) = changes.unzip
      val usedStakeholders = forParticipant.flatMap(_.stakeholders).toSet
      val partyHostings = allParties.view.filterKeys(usedStakeholders).toMap
      (
        participant.toLf -> ContractChangeBatch.tryCreate(partyHostings, forParticipant*),
        counterparticipant.toLf -> ContractChangeBatch.tryCreate(
          partyHostings,
          forCounterparticipant*
        ),
      )
    }

  property("consistency between counter-participants") {
    forAll(consistencyTestInputs) {
      case (
            (participantId, updateOnParticipant),
            (counterparticipantId, updateOnCounterParticipant),
          ) =>
        val deltaFromParticipantOpt = DigestOps
          .computeDeltas(
            updateOnParticipant,
            traceChanges = false,
          )
          .collectFirst {
            case delta: DigestDelta.Participant if delta.participantId == counterparticipantId =>
              delta
          }

        val deltaFromCounterParticipantOpt = DigestOps
          .computeDeltas(
            updateOnCounterParticipant,
            traceChanges = false,
          )
          .collectFirst {
            case delta: DigestDelta.Participant if delta.participantId == participantId => delta
          }

        inside((deltaFromParticipantOpt, deltaFromCounterParticipantOpt)) {
          case (Some(deltaFromParticipant), Some(deltaFromCounterParticipant)) =>
            deltaFromParticipant.digest shouldBe deltaFromCounterParticipant.digest
        }

    }
  }
}

object DigestOpsPropertyTest {

  final case class ConsistencyTestInput(
      stakeholders: Map[LfPartyId, Set[LedgerParticipantId]],
      participant: LedgerParticipantId,
      counterParticipant: LedgerParticipantId,
  ) {}
}
