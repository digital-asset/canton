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
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
  GenericTopologyEvent,
  TopologyEvent,
}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactions.PositiveSignedTopologyTransactions
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
final class TopologyTransactionDiffTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  import TopologyTransactionDiffTest.*

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
  ): SignedTopologyTransaction[Replace, PartyToParticipant] =
    ptpHosts(
      partyId,
      participants.map { case (participant, (permission, onboarding)) =>
        HostingParticipant(participant, permission, onboarding)
      },
    )

  private def ptpHosts(
      partyId: PartyId,
      hosts: List[HostingParticipant],
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = {

    val mapping = PartyToParticipant.tryCreate(partyId, PositiveInt.one, hosts)

    val tx: TopologyTransaction[Replace, PartyToParticipant] = TopologyTransaction
      .create(
        Replace,
        PositiveInt.one,
        mapping,
        testedProtocolVersion,
      )
      .valueOrFail("creating topology transaction")

    topologyFactory.mkTrans[Replace, PartyToParticipant](trans = tx)
  }

  private def synchronizerTrustCertificate(
      participantId: ParticipantId
  ): SignedTopologyTransaction[Replace, SynchronizerTrustCertificate] = {

    val tx: TopologyTransaction[Replace, SynchronizerTrustCertificate] =
      TopologyTransaction.tryCreate(
        Replace,
        PositiveInt.one,
        SynchronizerTrustCertificate(participantId, synchronizerId),
        testedProtocolVersion,
      )

    topologyFactory.mkTrans[Replace, SynchronizerTrustCertificate](trans = tx)
  }

  private def synchronizerParametersState(
      reconciliationInterval: PositiveSeconds
  ): SignedTopologyTransaction[Replace, SynchronizerParametersState] = {
    val tx: TopologyTransaction[Replace, SynchronizerParametersState] =
      TopologyTransaction.tryCreate(
        Replace,
        PositiveInt.one,
        SynchronizerParametersState(
          synchronizerId,
          DynamicSynchronizerParameters
            .defaultValues(testedProtocolVersion)
            .update(reconciliationInterval = reconciliationInterval),
        ),
        testedProtocolVersion,
      )

    topologyFactory.mkTrans[Replace, SynchronizerParametersState](trans = tx)
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
    PartyToParticipantAuthorization(partyId.toLf, participantId.toLf, Onboarding(permission))

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
      ) = {
        val diff = TopologyTransactionDiff(
          synchronizerId,
          initialTxs,
          newState,
          p1,
        )
        diff.foreach(_.genericTopologyEvents shouldBe empty)
        diff.map(_.topologyEvents)
      }

      "return None for identical transactions" in {
        diffInitialWith(initialTxs) shouldBe None
      }

      "revoke everything when the target state is empty" in {
        diffInitialWith(Nil).value should contain theSameElementsAs Set(
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
        ).value should contain theSameElementsAs Set(
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
        ).value should contain theSameElementsAs Set(
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
        ).value should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(bob.toLf, p2.toLf, Added(Submission))
        )
      }

      "add a mapping for a completely new party" in {
        diffInitialWith(
          List(
            ptp(donald, List(p1 -> ParticipantPermission.Submission)) // new
          ) ++ initialTxs
        ).value should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(donald.toLf, p1.toLf, Added(Submission))
        )
      }

      "add mappings for synchronizer trust certificates (admin parties)" in {
        diffInitialWith(
          List(
            synchronizerTrustCertificate(p1), // new
            synchronizerTrustCertificate(p2), // new
          ) ++ initialTxs
        ).value should contain theSameElementsAs Set(
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
        ).value.topologyEvents should contain theSameElementsAs Set(
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
        ).value should contain theSameElementsAs Set(
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
        ).value.abortingOnboardingLocalParty shouldBe true
      }

      "evaluate to false when the local participant revokes a party's mapping but the party was already fully active" in {
        TopologyTransactionDiff(
          synchronizerId,
          activeBeforeState,
          afterStateP1Revoked,
          p1,
        ).value.abortingOnboardingLocalParty shouldBe false
      }

      "evaluate to false when an onboarding authorization is revoked, but not from the local participant" in {
        TopologyTransactionDiff(
          synchronizerId,
          onboardingBeforeState,
          afterStateP1Revoked,
          p2,
        ).value.abortingOnboardingLocalParty shouldBe false
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
        ).value.abortingOnboardingLocalParty shouldBe false
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

      "handle non-local onboarding" in {
        val res = diff(
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          p1,
        )
        res.value.topologyEvents should contain theSameElementsAs Set(
          expectedOnboardingEvent(alice, p2, Submission)
        )
        res.value.onboardingLocalParty shouldBe false
        res.value.clearingOnboardingLocalParty shouldBe false
        res.value.abortingOnboardingLocalParty shouldBe false
      }

      "handle non-local clearing of onboarding" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
          p1,
        )
        res.map(_.topologyEvents).value shouldBe Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Added(Submission))
        )

        val d = res.value
        d.onboardingLocalParty shouldBe false
        d.clearingOnboardingLocalParty shouldBe false
        d.abortingOnboardingLocalParty shouldBe false

      }

      "handle non-local onboarding removal considered removed" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          p1,
        )
        res.value.topologyEvents should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked)
        )
        res.value.onboardingLocalParty shouldBe false
        res.value.clearingOnboardingLocalParty shouldBe false
        res.value.abortingOnboardingLocalParty shouldBe false
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
        res.value.topologyEvents should contain theSameElementsAs Set(
          expectedOnboardingEvent(alice, p2, Submission)
        )
        res.value.onboardingLocalParty shouldBe true
        res.value.clearingOnboardingLocalParty shouldBe false
        res.value.abortingOnboardingLocalParty shouldBe false
      }

      "handle local participant clearing of onboarding" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptpOB(alice, List(p1 -> Subm, p2 -> Subm))),
          p2,
        )
        res.map(_.topologyEvents).value shouldBe Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Added(Submission))
        )

        val d = res.value
        d.onboardingLocalParty shouldBe false
        d.clearingOnboardingLocalParty shouldBe true
        d.abortingOnboardingLocalParty shouldBe false
      }

      "handle local participant onboarding removal considered removed" in {
        val res = diff(
          List(ptpOB(alice, List(p1 -> Subm, p2 -> SubmOB))),
          List(ptp(alice, List(p1 -> ParticipantPermission.Submission))),
          p2,
        )
        res.value.topologyEvents should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked)
        )
        res.value.onboardingLocalParty shouldBe false
        res.value.clearingOnboardingLocalParty shouldBe false
        // Triggered because p2 (local) was revoked.
        res.value.abortingOnboardingLocalParty shouldBe true
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
        res.value.topologyEvents should contain theSameElementsAs Set(
          PartyToParticipantAuthorization(alice.toLf, p2.toLf, ChangedTo(Confirmation))
        )
      }
    }

    "derive events from hosting states" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels when {
      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))

      // Alice is Active on p1 throughout; only p2's entry changes.
      def aliceWithP2(
          state: Option[HostingState],
          permission: ParticipantPermission = ParticipantPermission.Confirmation,
      ): List[SignedTopologyTransaction[Replace, TopologyMapping]] =
        List(
          ptpHosts(
            alice,
            HostingParticipant(p1, ParticipantPermission.Submission, HostingState.Active) ::
              state.map(HostingParticipant(p2, permission, _)).toList,
          )
        )

      def p2Event(event: AuthorizationEvent): Set[TopologyEvent] =
        Set(PartyToParticipantAuthorization(alice.toLf, p2.toLf, event))

      val noEntry = Option.empty[HostingState]
      val nominated = Some(HostingState.Nominated)
      val freezing = Some(HostingState.Freezing)
      val onboarding = Some(HostingState.Onboarding)
      val active = Some(HostingState.Active)

      // One case per forward transition of p2's entry, sorted by `before`, then `after`.
      // Backward moves (e.g. Active -> Onboarding, Onboarding -> Freezing) are not covered:
      // topology validation is to reject them (#36492).
      val transitions = Seq(
        HostingStateTransition(noEntry, nominated, None),
        HostingStateTransition(noEntry, freezing, None),
        HostingStateTransition(noEntry, onboarding, Some(Onboarding(Confirmation))),
        HostingStateTransition(noEntry, active, Some(Added(Confirmation))),
        HostingStateTransition(nominated, noEntry, None),
        HostingStateTransition(nominated, freezing, None),
        HostingStateTransition(nominated, onboarding, Some(Onboarding(Confirmation))),
        HostingStateTransition(nominated, active, Some(Added(Confirmation))),
        HostingStateTransition(freezing, noEntry, None),
        HostingStateTransition(freezing, onboarding, Some(Onboarding(Confirmation))),
        HostingStateTransition(freezing, active, Some(Added(Confirmation))),
        HostingStateTransition(onboarding, noEntry, Some(Revoked)),
        HostingStateTransition(onboarding, active, Some(Added(Confirmation))),
        HostingStateTransition(active, noEntry, Some(Revoked)),
      )

      transitions.foreach { transition =>
        s"emit ${transition.expected.fold("no event")(_.toString)} for ${transition.describe}" in {
          TopologyTransactionDiff(
            synchronizerId,
            aliceWithP2(transition.before),
            aliceWithP2(transition.after),
            p1,
          ).map(_.topologyEvents) shouldBe transition.expected.map(p2Event)
        }
      }

      "emit ChangedTo for a permission change of an Onboarding or Active entry" in {
        forAll(Seq(HostingState.Onboarding, HostingState.Active)) { state =>
          TopologyTransactionDiff(
            synchronizerId,
            aliceWithP2(Some(state), ParticipantPermission.Confirmation),
            aliceWithP2(Some(state), ParticipantPermission.Observation),
            p1,
          ).map(_.topologyEvents) shouldBe Some(p2Event(ChangedTo(Observation)))
        }
      }

      "emit no event for a permission change of a Nominated or Freezing entry" in {
        forAll(Seq(HostingState.Nominated, HostingState.Freezing)) { state =>
          TopologyTransactionDiff(
            synchronizerId,
            aliceWithP2(Some(state), ParticipantPermission.Confirmation),
            aliceWithP2(Some(state), ParticipantPermission.Observation),
            p1,
          ) shouldBe None
        }
      }

      "flag the local onboarding on Freezing -> Onboarding" in {
        val diff = TopologyTransactionDiff(
          synchronizerId,
          aliceWithP2(Some(HostingState.Freezing)),
          aliceWithP2(Some(HostingState.Onboarding)),
          p2,
        ).value
        diff.onboardingLocalParty shouldBe true
        diff.abortingOnboardingLocalParty shouldBe false
      }
    }

    "synchronizer parameters changes" should {
      val tx1 = synchronizerParametersState(PositiveSeconds.tryOfSeconds(1))
      val tx2 = synchronizerParametersState(PositiveSeconds.tryOfSeconds(2))

      "be reflected in the diff if something has changed" onlyRunWithOrGreaterThan (ProtocolVersion.v36) in {
        val oldState = List(tx1)
        val newState = List(tx2)

        TopologyTransactionDiff(
          synchronizerId,
          oldState,
          newState,
          p1,
        ).value.genericTopologyEvents.loneElement shouldBe GenericTopologyEvent
          .SynchronizerParametersState(tx2.transaction.toByteStringChecked)
      }

      "not trigger an update if no values have changed" onlyRunWithOrGreaterThan (ProtocolVersion.v36) in {
        val state = List(tx1)
        TopologyTransactionDiff(synchronizerId, state, state, p1) shouldBe None
      }

      "be computed alongside party changes" onlyRunWithOrGreaterThan (ProtocolVersion.v36) in {
        val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
        val alicePtp = ptp(alice, List(p1 -> ParticipantPermission.Submission))

        val oldState = List(tx1)
        val newState = List(tx2, alicePtp)

        def checkDiff(
            oldState: PositiveSignedTopologyTransactions,
            newState: PositiveSignedTopologyTransactions,
        ) = {
          val diff = TopologyTransactionDiff(synchronizerId, oldState, newState, p1).value
          diff.topologyEvents should not be empty
          diff.genericTopologyEvents should not be empty
        }

        checkDiff(oldState, newState)
        checkDiff(newState, oldState)
      }
    }

    "generate deterministic updateIds" should {
      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::bob"))

      val tx1 = ptp(alice, List(p1 -> ParticipantPermission.Submission))
      val tx2 = ptp(bob, List(p1 -> ParticipantPermission.Submission))
      val tx3 = ptp(alice, List(p1 -> ParticipantPermission.Confirmation))
      val paramsTx = synchronizerParametersState(PositiveSeconds.tryOfSeconds(1))

      val oldState = List(tx1, tx2)
      val newState = List(tx2, tx3, paramsTx)

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

      "take synchronizer parameters into account" in {
        val updateIdWithParams =
          TopologyTransactionDiff.updateId(synchronizerId, oldState, List(tx2, tx3, paramsTx))
        val updateIdWithoutParams =
          TopologyTransactionDiff.updateId(synchronizerId, oldState, List(tx2, tx3))
        updateIdWithParams should not be updateIdWithoutParams
      }
    }
  }
}

object TopologyTransactionDiffTest {
  private final case class HostingStateTransition(
      before: Option[HostingState],
      after: Option[HostingState],
      expected: Option[AuthorizationEvent],
  ) {
    def describe: String = {
      def show(state: Option[HostingState]) = state.fold("<>")(_.toString)
      s"${show(before)} -> ${show(after)}"
    }
  }
}
