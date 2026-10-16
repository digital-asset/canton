// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeysWithThreshold, SigningPublicKey}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.TopologyMapping.RequiredAuth.RequiredNamespaces
import com.digitalasset.canton.topology.transaction.TopologyMapping.{
  ReferencedAuthorizations,
  RequiredAuth,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Namespace,
  ParticipantId,
  PartyId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class PartyToParticipantRequiredAuthTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private def namespace(name: String): Namespace = Namespace(Fingerprint.tryFromString(name))

  private val partyNamespace = namespace("party")
  private val participant1Namespace = namespace("participant1")
  private val participant2Namespace = namespace("participant2")
  private val participant3Namespace = namespace("participant3")

  private val party = PartyId(UniqueIdentifier.tryCreate("alice", partyNamespace))
  private val participant1Id = ParticipantId(
    UniqueIdentifier.tryCreate("p1", participant1Namespace)
  )
  private val participant2Id = ParticipantId(
    UniqueIdentifier.tryCreate("p2", participant2Namespace)
  )
  private val participant3Id = ParticipantId(
    UniqueIdentifier.tryCreate("p3", participant3Namespace)
  )

  private def ptp(
      participants: Seq[HostingParticipant],
      threshold: PositiveInt = PositiveInt.one,
  ): PartyToParticipant =
    PartyToParticipant.tryCreate(party, threshold, participants)

  private lazy val testingKeys =
    new TestingOwnerWithKeys(
      DefaultTestIdentities.participant1,
      loggerFactory,
      parallelExecutionContext,
    )

  // requiredAuth does not depend on the protocol version. This one can serialize every hosting state.
  private def stored(
      mapping: PartyToParticipant
  ): TopologyTransaction[TopologyChangeOp, TopologyMapping] =
    TopologyTransaction.tryCreate(
      TopologyChangeOp.Replace,
      PositiveInt.one,
      mapping,
      ProtocolVersion.offPRWithSequencerChannels,
    )

  private def requiredAuth(
      mapping: PartyToParticipant,
      previous: Option[PartyToParticipant],
  ): RequiredAuth =
    mapping.requiredAuth(previous.map(stored))

  private def satisfiedBy(auth: RequiredAuth, signers: Set[Namespace]): Boolean =
    auth.satisfiedByActualAuthorizers(ReferencedAuthorizations(namespaces = signers)).isRight

  "PartyToParticipant.requiredAuth" when {

    "a single entry changes its hosting state" should {
      import HostingState.*

      val partySigns = Set(partyNamespace)
      val participantSigns = Set(participant2Namespace)
      val bothSign = partySigns ++ participantSigns

      val requiresParty = Set(partySigns, bothSign)
      val requiresParticipant = Set(participantSigns, bothSign)
      val requiresBoth = Set(bothSign)
      val requiresEither = Set(partySigns, participantSigns, bothSign)

      // From and to state of participant2's entry, and the signer sets that authorize the change.
      // None is no entry or a removed entry. An unchanged entry falls back to the party.
      val matrix: Seq[(Option[HostingState], Option[HostingState], Set[Set[Namespace]])] = Seq(
        (None, Some(Nominated), requiresParty),
        (None, Some(Freezing), requiresBoth),
        (None, Some(Onboarding), requiresBoth),
        (None, Some(Active), requiresBoth),
        (Some(Nominated), None, requiresEither),
        (Some(Nominated), Some(Nominated), requiresParty),
        (Some(Nominated), Some(Freezing), requiresParticipant),
        (Some(Nominated), Some(Onboarding), requiresParticipant),
        (Some(Nominated), Some(Active), requiresParticipant),
        (Some(Freezing), None, requiresEither),
        (Some(Freezing), Some(Nominated), requiresParty),
        (Some(Freezing), Some(Freezing), requiresParty),
        (Some(Freezing), Some(Onboarding), requiresParticipant),
        (Some(Freezing), Some(Active), requiresParticipant),
        (Some(Onboarding), None, requiresEither),
        (Some(Onboarding), Some(Nominated), requiresParty),
        (Some(Onboarding), Some(Freezing), requiresParty),
        (Some(Onboarding), Some(Onboarding), requiresParty),
        (Some(Onboarding), Some(Active), requiresParticipant),
        (Some(Active), None, requiresEither),
        (Some(Active), Some(Nominated), requiresParty),
        (Some(Active), Some(Freezing), requiresParty),
        (Some(Active), Some(Onboarding), requiresParty),
        (Some(Active), Some(Active), requiresParty),
      )

      def withParticipant2In(state: Option[HostingState]): PartyToParticipant =
        ptp(
          HostingParticipant(participant1Id, Submission) +:
            state.map(s => HostingParticipant(participant2Id, Confirmation, s)).toList
        )

      "require the signatures of the authorization matrix" in {
        forEvery(matrix) { case (from, to, authorizingSignerSets) =>
          val auth = requiredAuth(withParticipant2In(to), Some(withParticipant2In(from)))
          forEvery(Seq(partySigns, participantSigns, bothSign)) { signers =>
            withClue(s"$from -> $to signed by $signers") {
              satisfiedBy(auth, signers) shouldBe authorizingSignerSets.contains(signers)
            }
          }
        }
      }
    }

    "there is no previous mapping" should {
      "require party and all hosting participants" in {
        val current =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Observation),
            )
          )

        requiredAuth(current, None) shouldBe RequiredNamespaces(
          Set(partyNamespace, participant1Namespace, participant2Namespace)
        )
      }

      "not require Nominated participants" in {
        val current =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Observation, HostingState.Nominated),
            )
          )

        requiredAuth(current, None) shouldBe RequiredNamespaces(
          Set(partyNamespace, participant1Namespace)
        )
      }
    }

    "threshold changes" should {
      "require only the party" in {
        val previous =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation),
            )
          )
        val current = previous.tryCopy(threshold = PositiveInt.tryCreate(2))

        requiredAuth(current, Some(previous)) shouldBe RequiredNamespaces(Set(partyNamespace))
      }
    }

    "a permission is upgraded" should {
      "require the party and the participant" in {
        val previous = ptp(Seq(HostingParticipant(participant1Id, Observation)))
        val current = ptp(Seq(HostingParticipant(participant1Id, Submission)))

        requiredAuth(current, Some(previous)) shouldBe RequiredNamespaces(
          Set(partyNamespace, participant1Namespace)
        )
      }

      "require only the party with the legacy permission upgrade requirements" in {
        val previous = ptp(Seq(HostingParticipant(participant1Id, Observation)))
        val current = ptp(Seq(HostingParticipant(participant1Id, Submission)))

        current.requiredAuthBackwardsCompatible(Some(stored(previous))) shouldBe
          RequiredNamespaces(Set(partyNamespace))
      }
    }

    "the whole mapping is removed" should {
      // a REMOVE carries the stored mapping, so nothing changes
      "require only the party" in {
        val mapping =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation, HostingState.Onboarding),
            )
          )

        requiredAuth(mapping, Some(mapping)) shouldBe RequiredNamespaces(Set(partyNamespace))
      }
    }

    "the party's signing keys change" should {
      "require the party and every added key" in {
        import testingKeys.SigningKeys.{key1, key2, key3}

        def withSigningKeys(keys: SigningPublicKey*): PartyToParticipant =
          PartyToParticipant.tryCreate(
            party,
            PositiveInt.one,
            Seq(HostingParticipant(participant1Id, Submission)),
            NonEmpty.from(keys).map(SigningKeysWithThreshold.tryCreate(_, PositiveInt.one)),
          )

        // key1 stays, key2 is removed, key3 is added
        requiredAuth(withSigningKeys(key1, key3), Some(withSigningKeys(key1, key2))) shouldBe
          RequiredNamespaces(Set(partyNamespace), extraKeys = Set(key3.fingerprint))
      }
    }

    "a permission is downgraded" should {
      "require either the party or the participant" in {
        val previous = ptp(Seq(HostingParticipant(participant1Id, Submission)))
        val current = ptp(Seq(HostingParticipant(participant1Id, Observation)))

        val auth = requiredAuth(current, Some(previous))
        auth shouldBe RequiredNamespaces(Set(partyNamespace)).or(
          RequiredNamespaces(Set(participant1Namespace))
        )

        satisfiedBy(auth, Set(partyNamespace)) shouldBe true
        satisfiedBy(auth, Set(participant1Namespace)) shouldBe true
        satisfiedBy(auth, Set(participant2Namespace)) shouldBe false
      }
    }

    "multiple changes are combined" should {
      "require all individual authorizations" in {
        val previous =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Submission),
            )
          )
        val current = ptp(
          Seq(
            HostingParticipant(participant1Id, Submission),
            HostingParticipant(participant3Id, Confirmation),
          ),
          threshold = PositiveInt.tryCreate(2),
        )

        val auth = requiredAuth(current, Some(previous))

        // the threshold change and the addition of p3 require the party and p3,
        // the removal of p2 requires the party or p2
        satisfiedBy(auth, Set(partyNamespace, participant3Namespace)) shouldBe true
        satisfiedBy(auth, Set(partyNamespace)) shouldBe false
        satisfiedBy(auth, Set(participant2Namespace, participant3Namespace)) shouldBe false
        auth.referenced.namespaces shouldBe Set(
          partyNamespace,
          participant2Namespace,
          participant3Namespace,
        )
      }

      "require the moved participant and the party or the removed participant" in {
        val previous =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation, HostingState.Nominated),
              HostingParticipant(participant3Id, Confirmation),
            )
          )
        val current =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation),
            )
          )

        val auth = requiredAuth(current, Some(previous))

        satisfiedBy(auth, Set(participant2Namespace, partyNamespace)) shouldBe true
        satisfiedBy(auth, Set(participant2Namespace, participant3Namespace)) shouldBe true
        satisfiedBy(auth, Set(participant2Namespace)) shouldBe false
        satisfiedBy(auth, Set(partyNamespace, participant3Namespace)) shouldBe false
      }

      "require the party for a backward move next to a removal" in {
        // P3 removes itself and moves P2 from Active to Onboarding: P3 alone must not suffice
        val previous =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation),
              HostingParticipant(participant3Id, Confirmation),
            )
          )
        val current =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation, HostingState.Onboarding),
            )
          )

        val auth = requiredAuth(current, Some(previous))

        satisfiedBy(auth, Set(partyNamespace)) shouldBe true
        satisfiedBy(auth, Set(participant3Namespace)) shouldBe false
        satisfiedBy(auth, Set(participant2Namespace, participant3Namespace)) shouldBe false
      }

      "require the party and the participant for a threshold change and a forward move" in {
        val previous =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation, HostingState.Nominated),
            )
          )
        val current =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Confirmation, HostingState.Freezing),
            ),
            threshold = PositiveInt.tryCreate(2),
          )

        val auth = requiredAuth(current, Some(previous))

        satisfiedBy(auth, Set(partyNamespace, participant2Namespace)) shouldBe true
        satisfiedBy(auth, Set(partyNamespace)) shouldBe false
        satisfiedBy(auth, Set(participant2Namespace)) shouldBe false
      }

      "require the party and the participant for a permission upgrade and a forward move" in {
        val previous =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Observation, HostingState.Nominated),
            )
          )
        val current =
          ptp(
            Seq(
              HostingParticipant(participant1Id, Submission),
              HostingParticipant(participant2Id, Submission),
            )
          )

        val auth = requiredAuth(current, Some(previous))

        satisfiedBy(auth, Set(partyNamespace, participant2Namespace)) shouldBe true
        satisfiedBy(auth, Set(partyNamespace)) shouldBe false
        satisfiedBy(auth, Set(participant2Namespace)) shouldBe false
      }
    }
  }
}
