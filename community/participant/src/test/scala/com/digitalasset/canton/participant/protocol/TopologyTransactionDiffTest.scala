// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SynchronizerId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

class TopologyTransactionDiffTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  private lazy val topologyFactory =
    new TestingOwnerWithKeys(sequencerId, loggerFactory, executorService)

  private lazy val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::mysync")
  ).toPhysical

  private def ptp(
      partyId: PartyId,
      participants: List[(ParticipantId, ParticipantPermission)],
  ): SignedTopologyTransaction[Replace, PartyToParticipant] =
    ptpOB(partyId, participants.map { case (pid, perm) => pid -> (perm, false) })

  private def ptpOB(
      partyId: PartyId,
      participants: List[(ParticipantId, (ParticipantPermission, Boolean))],
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = {

    val mapping = PartyToParticipant.tryCreate(
      partyId,
      PositiveInt.one,
      participants.map { case (participant, (permission, onboarding)) =>
        HostingParticipant(participant, permission, onboarding)
      },
    )

    val tx: TopologyTransaction[Replace, PartyToParticipant] = TopologyTransaction(
      Replace,
      PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

    topologyFactory.mkTrans[Replace, PartyToParticipant](trans = tx)
  }

  private def synchronizerTrustCertificate(
      participantId: ParticipantId
  ): SignedTopologyTransaction[Replace, SynchronizerTrustCertificate] = {

    val tx: TopologyTransaction[Replace, SynchronizerTrustCertificate] = TopologyTransaction(
      Replace,
      PositiveInt.one,
      SynchronizerTrustCertificate(participantId, synchronizerId),
      testedProtocolVersion,
    )

    topologyFactory.mkTrans[Replace, SynchronizerTrustCertificate](trans = tx)
  }

  "TopologyTransactionDiff" should {
    val p1 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant1"))
    val p2 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant2"))

    "compute adds and removes" in {

      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::bob"))
      val charlie = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::charlie"))
      val donald = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::donald"))

      /*
        Initial topology:
          alice -> p1, p2
          bob -> p1
          charlie -> p2
       */
      val initialTxs = List(
        ptp(
          alice,
          List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
        ),
        ptp(bob, List(p1 -> ParticipantPermission.Submission)),
        ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
      )

      def diffInitialWith(
          newState: Seq[SignedTopologyTransaction[Replace, TopologyMapping]]
      ) = TopologyTransactionDiff(
        synchronizerId,
        initialTxs,
        newState,
        p1,
      )
        .map { case TopologyTransactionDiff(events, _, _, _) =>
          events
        }
      // Same transactions
      diffInitialWith(initialTxs) shouldBe None

      // Empty target -> everything is removed
      diffInitialWith(Nil).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked),
        PartyToParticipantAuthorization(bob.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(charlie.toLf, p2.toLf, Revoked),
      )

      diffInitialWith(
        List(
          ptp(alice, List(p2 -> ParticipantPermission.Submission)), // no p1

          ptp(bob, List(p1 -> ParticipantPermission.Submission)),
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked)
      )

      diffInitialWith(
        List(
          ptp(alice, List()), // nobody
          ptp(bob, List(p1 -> ParticipantPermission.Submission)),
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked),
      )

      diffInitialWith(
        List(
          ptp(
            alice,
            List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
          ),
          ptp(
            bob,
            List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
          ), // p2 added
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(bob.toLf, p2.toLf, Added(Submission))
      )

      diffInitialWith(
        List(
          ptp(donald, List(p1 -> ParticipantPermission.Submission)) // new
        ) ++ initialTxs
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(donald.toLf, p1.toLf, Added(Submission))
      )

      diffInitialWith(
        List(
          synchronizerTrustCertificate(p1), // new
          synchronizerTrustCertificate(p2), // new
        ) ++ initialTxs
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(p1.adminParty.toLf, p1.toLf, Added(Submission)),
        PartyToParticipantAuthorization(p2.adminParty.toLf, p2.toLf, Added(Submission)),
      )

      diffInitialWith(
        List(
          ptp(
            bob,
            List(p1 -> ParticipantPermission.Confirmation, p2 -> ParticipantPermission.Observation),
          ), // p2 added, p1 overridden
          ptp(
            alice,
            List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
          ),
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(bob.toLf, p1.toLf, ChangedTo(Confirmation)),
        PartyToParticipantAuthorization(bob.toLf, p2.toLf, Added(Observation)),
      )
    }

    "compute adds and removes with onboarding flag" in {
      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val Subm = (ParticipantPermission.Submission, false)
      val SubmOB = (ParticipantPermission.Submission, true)

      val isAtLeastV35 = testedProtocolVersion > ProtocolVersion.v34

      def diff(
          beforeState: Seq[SignedTopologyTransaction[Replace, TopologyMapping]],
          afterState: Seq[SignedTopologyTransaction[Replace, TopologyMapping]],
          localParticipant: ParticipantId,
      ) = TopologyTransactionDiff(
        synchronizerId,
        beforeState,
        afterState,
        localParticipant,
      )

      def authOnboarding(participantId: ParticipantId) = Set(
        PartyToParticipantAuthorization(
          alice.toLf,
          participantId.toLf,
          if (isAtLeastV35) Onboarding(Submission)
          else Added(Submission),
        )
      )

      def authClearOnboarding(
          participantId: ParticipantId
      ): Option[Set[PartyToParticipantAuthorization]] =
        Option.when(isAtLeastV35)(
          Set(PartyToParticipantAuthorization(alice.toLf, participantId.toLf, Added(Submission)))
        )

      // Non-local onboarding
      val res1 = diff(
        List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        p1,
      )
      res1.value.topologyEvents.forgetNE should contain theSameElementsAs authOnboarding(p2)
      res1.value.onboardingLocalParty shouldBe false
      res1.value.clearingOnboardingLocalParty shouldBe false

      // Non-local clearing of onboarding
      val res2 = diff(
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
        p1,
      )
      res2.map(_.topologyEvents) shouldBe authClearOnboarding(p2)
      res2.foreach { d =>
        d.onboardingLocalParty shouldBe false
        d.clearingOnboardingLocalParty shouldBe false
      }

      // Non-local onboarding removal considered removed
      val res3 = diff(
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
        p1,
      )
      res3.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked)
      )
      res3.value.onboardingLocalParty shouldBe false
      res3.value.clearingOnboardingLocalParty shouldBe false

      // Non-local transition from not onboarding to onboarding considered no-op
      // This is not an expected transition but tested for completeness.
      diff(
        List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        p1,
      ) shouldBe None

      // Local participant onboarding
      val res4 = diff(
        List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        p2,
      )
      res4.value.topologyEvents.forgetNE should contain theSameElementsAs authOnboarding(p2)
      res4.value.onboardingLocalParty shouldBe isAtLeastV35
      res4.value.clearingOnboardingLocalParty shouldBe false

      // Local participant clearing of onboarding
      val res5 = diff(
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
        p2,
      )
      res5.map(_.topologyEvents) shouldBe authClearOnboarding(p2)
      res5.foreach { d =>
        d.onboardingLocalParty shouldBe false
        d.clearingOnboardingLocalParty shouldBe true
      }

      // Local participant onboarding removal considered removed
      val res6 = diff(
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
        p2,
      )
      res6.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked)
      )
      res6.value.onboardingLocalParty shouldBe false
      res6.value.clearingOnboardingLocalParty shouldBe false

      // Local participant transition from not onboarding to onboarding considered no-op
      // This is not an expected transition but tested for completeness.
      diff(
        List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
        List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
        p2,
      ) shouldBe None
    }
  }
}
