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
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
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

/** Tests the computation of topology state differences.
  *
  * The primary goal is to verify that transitioning from an old set of topology transactions to a
  * new set correctly generates the appropriate authorization events (`Added`, `Revoked`,
  * `ChangedTo`, `Onboarding`) for parties hosted on various participants.
  *
  * Terminology:
  *   - **Local Participant (`localParticipantId`)**: The specific participant node evaluating this
  *     diff. This is critical for computing flags like `abortingOnboardingLocalParty`, which
  *     explicitly track whether an interrupted onboarding process directly affects the node
  *     currently running the code.
  *   - **Onboarding**: A transient state (`Onboarding` event has only been introduced in Protocol
  *     Version 35) indicating that a party has been granted an authorization on a participant, but
  *     their state is still synchronizing.
  *   - **Admin Parties**: Participants implicitly have an admin party representation via their
  *     `SynchronizerTrustCertificate`. The diff logic maps these to a default `Submission`
  *     authorization level.
  */
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

  /** Determines the expected value of onboarding-related local party flags based on the protocol
    * version.
    *
    * The flags `onboardingLocalParty`, `clearingOnboardingLocalParty`, and
    * `abortingOnboardingLocalParty` track the lifecycle of a party's "onboarding" state on a
    * participant. Because the concept of an onboarding state was only introduced in Protocol
    * Version 35 (PV35), these flags must always strictly evaluate to `false` in PV34 and earlier,
    * regardless of what the transaction's contents attempt to specify.
    *
    * @param ifSupportedToBe
    *   The expected boolean value if the protocol version natively supports onboarding (PV35+).
    * @return
    *   `true` only if the protocol version is >= PV35 AND the expected value is true.
    */
  private def expectedOnboardingFlag(ifSupportedToBe: Boolean): Boolean = {
    val supportsOnboarding = testedProtocolVersion > ProtocolVersion.v34
    supportsOnboarding && ifSupportedToBe
  }

  /** Determines the expected authorization event when a participant mapping is added with an
    * "onboarding" state flag.
    *
    * In PV35 and later, adding a participant with the onboarding flag set to true yields an
    * explicit `Onboarding` topology event. However, in PV34 and earlier, the participant completely
    * ignores the onboarding flag during state computation. Therefore, the transition falls back to
    * `Added` event.
    *
    * @param partyId
    *   The party being authorized.
    * @param participantId
    *   The participant hosting the party.
    * @param permission
    *   The base authorization level (e.g., Submission, Confirmation).
    * @return
    *   An `Onboarding` event for PV35+, or an `Added` event for PV34-.
    */
  private def expectedOnboardingEvent(
      partyId: PartyId,
      participantId: ParticipantId,
      permission: AuthorizationLevel,
  ): PartyToParticipantAuthorization =
    if (testedProtocolVersion > ProtocolVersion.v34) {
      PartyToParticipantAuthorization(partyId.toLf, participantId.toLf, Onboarding(permission))
    } else {
      PartyToParticipantAuthorization(partyId.toLf, participantId.toLf, Added(permission))
    }

  "TopologyTransactionDiff" should {
    val p1 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant1"))
    val p2 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant2"))

    "compute adds and removes" should {
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
      ).map(_.topologyEvents)

      "return None for identical transactions" in {
        diffInitialWith(initialTxs) shouldBe None
      }

      "revoke everything when the target state is empty" in {
        diffInitialWith(Nil).value.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked),
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked),
          PartyToParticipantAuthorization(bob.toLf, p1.toLf, Revoked),
          PartyToParticipantAuthorization(charlie.toLf, p2.toLf, Revoked),
        )
      }

      "revoke specific participant mapping" in {
        diffInitialWith(
          List(
            ptp(alice, List(p2 -> ParticipantPermission.Submission)), // no p1
            ptp(bob, List(p1 -> ParticipantPermission.Submission)),
            ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
          )
        ).value.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked)
        )
      }

      "revoke all mappings for a specific party" in {
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
      }

      "add a new participant mapping for an existing party" in {
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
      }

      "add a mapping for a completely new party" in {
        diffInitialWith(
          List(
            ptp(donald, List(p1 -> ParticipantPermission.Submission)) // new
          ) ++ initialTxs
        ).value.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(donald.toLf, p1.toLf, Added(Submission))
        )
      }

      "add mappings for synchronizer trust certificates (admin parties)" in {
        diffInitialWith(
          List(
            synchronizerTrustCertificate(p1), // new
            synchronizerTrustCertificate(p2), // new
          ) ++ initialTxs
        ).value.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(p1.adminParty.toLf, p1.toLf, Added(Submission)),
          PartyToParticipantAuthorization(p2.adminParty.toLf, p2.toLf, Added(Submission)),
        )
      }

      "revoke an admin party mapping" in {
        TopologyTransactionDiff(
          synchronizerId,
          List(synchronizerTrustCertificate(p1)), // old state has admin
          Nil, // new state revokes admin
          p1,
        ).value.topologyEvents.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(p1.adminParty.toLf, p1.toLf, Revoked)
        )
      }

      "change permissions and add mappings simultaneously" in {
        diffInitialWith(
          List(
            ptp(
              bob,
              List(
                p1 -> ParticipantPermission.Confirmation,
                p2 -> ParticipantPermission.Observation,
              ),
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
    }

    "compute the abortingOnboardingLocalParty flag" should {
      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val Subm = (ParticipantPermission.Submission, false)
      val SubmOB = (ParticipantPermission.Submission, true)

      val activeBeforeState = List(
        ptp(
          alice,
          List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
        )
      )

      val onboardingBeforeState = List(
        ptpOB(
          alice,
          List(p1 -> SubmOB, p2 -> Subm),
        )
      )

      val afterStateP1Revoked = List(ptp(alice, List(p2 -> ParticipantPermission.Submission)))

      "evaluate to true when a party's authorization on the local participant is revoked while still in onboarding state" in {
        TopologyTransactionDiff(
          synchronizerId,
          onboardingBeforeState,
          afterStateP1Revoked,
          p1,
        ).value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = true)
      }

      "evaluate to false when the local participant revokes a party's mapping but the party was already fully active" in {
        TopologyTransactionDiff(
          synchronizerId,
          activeBeforeState,
          afterStateP1Revoked,
          p1,
        ).value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
      }

      "evaluate to false when an onboarding authorization is revoked, but not from the local participant" in {
        TopologyTransactionDiff(
          synchronizerId,
          onboardingBeforeState,
          afterStateP1Revoked,
          p2,
        ).value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
      }

      "return None when no party authorizations are changed or revoked on the local participant" in {
        TopologyTransactionDiff(
          synchronizerId,
          onboardingBeforeState,
          onboardingBeforeState,
          p1,
        ) shouldBe None
      }

      "evaluate to false when a party is granted a new mapping to another participant, but their local onboarding mapping remains intact" in {
        val p3 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant3"))
        TopologyTransactionDiff(
          synchronizerId,
          onboardingBeforeState,
          List(
            ptpOB(
              alice,
              List(
                p1 -> SubmOB,
                p2 -> Subm,
                p3 -> Subm, // Alice is granted authorization on p3
              ),
            )
          ),
          p1,
        ).value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
      }
    }

    "compute adds and removes with onboarding flag" should {
      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val Subm = (ParticipantPermission.Submission, false)
      val SubmOB = (ParticipantPermission.Submission, true)
      val ConfOB = (ParticipantPermission.Confirmation, true)

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

      def authClearOnboarding(
          participantId: ParticipantId
      ): Option[Set[PartyToParticipantAuthorization]] =
        // Clearing onboarding only emits an event if onboarding was supported to begin with
        Option.when(expectedOnboardingFlag(ifSupportedToBe = true))(
          Set(PartyToParticipantAuthorization(alice.toLf, participantId.toLf, Added(Submission)))
        )

      "handle non-local onboarding" in {
        val res = diff(
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          p1,
        )
        res.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
          expectedOnboardingEvent(alice, p2, Submission)
        )
        res.value.onboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        res.value.clearingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        res.value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
      }

      "handle non-local clearing of onboarding" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
          p1,
        )
        res.map(_.topologyEvents) shouldBe authClearOnboarding(p2)

        if (expectedOnboardingFlag(ifSupportedToBe = true)) {
          val d = res.value
          d.onboardingLocalParty shouldBe false
          d.clearingOnboardingLocalParty shouldBe false
          d.abortingOnboardingLocalParty shouldBe false
        } else {
          // In PV34, this transition is a no-op, so no diff is generated
          res shouldBe None
        }
      }

      "handle non-local onboarding removal considered removed" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          p1,
        )
        res.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked)
        )
        res.value.onboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        res.value.clearingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        res.value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
      }

      "treat non-local transition from not onboarding to onboarding as no-op" in {
        // This is not an expected transition but tested for completeness.
        diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          p1,
        ) shouldBe None
      }

      "handle local participant onboarding" in {
        val res = diff(
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          p2,
        )
        res.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
          expectedOnboardingEvent(alice, p2, Submission)
        )
        res.value.onboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = true)
        res.value.clearingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        res.value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
      }

      "handle local participant clearing of onboarding" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
          p2,
        )
        res.map(_.topologyEvents) shouldBe authClearOnboarding(p2)

        if (expectedOnboardingFlag(ifSupportedToBe = true)) {
          val d = res.value
          d.onboardingLocalParty shouldBe false
          d.clearingOnboardingLocalParty shouldBe true
          d.abortingOnboardingLocalParty shouldBe false
        } else {
          // In PV34, this transition is a no-op, so no diff is generated
          res shouldBe None
        }
      }

      "handle local participant onboarding removal considered removed" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          p2,
        )
        res.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked)
        )
        res.value.onboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        res.value.clearingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = false)
        // Triggered because p2 (local) was revoked.
        res.value.abortingOnboardingLocalParty shouldBe
          expectedOnboardingFlag(ifSupportedToBe = true)
      }

      "treat local participant transition from not onboarding to onboarding as no-op" in {
        // This is not an expected transition but tested for completeness.
        diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          p2,
        ) shouldBe None
      }

      "handle permission change while still onboarding" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> ConfOB))),
          p2,
        )
        res.value.topologyEvents.forgetNE should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, ChangedTo(Confirmation))
        )
      }
    }

    "generate deterministic updateIds" should {
      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::bob"))

      val tx1 = ptp(alice, List(p1 -> ParticipantPermission.Submission))
      val tx2 = ptp(bob, List(p1 -> ParticipantPermission.Submission))
      val tx3 = ptp(alice, List(p1 -> ParticipantPermission.Confirmation))

      val oldState = List(tx1, tx2)
      val newState = List(tx2, tx3)

      "be independent of sequence order" in {
        val updateId1 = TopologyTransactionDiff.updateId(synchronizerId, oldState, newState)
        val updateIdReversed =
          TopologyTransactionDiff.updateId(synchronizerId, oldState.reverse, newState.reverse)
        updateId1 shouldBe updateIdReversed
      }

      "yield different ids for different state transitions" in {
        val updateId1 = TopologyTransactionDiff.updateId(synchronizerId, oldState, newState)
        val updateIdDifferent =
          TopologyTransactionDiff.updateId(synchronizerId, oldState, List(tx1, tx3))
        updateId1 should not be updateIdDifferent
      }

      "yield different ids when swapping old and new state" in {
        val updateId1 = TopologyTransactionDiff.updateId(synchronizerId, oldState, newState)
        val updateIdSwapped = TopologyTransactionDiff.updateId(synchronizerId, newState, oldState)
        updateId1 should not be updateIdSwapped
      }
    }
  }
}
