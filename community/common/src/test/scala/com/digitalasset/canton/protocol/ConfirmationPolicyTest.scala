// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{ConfirmingParty, PlainInformee}
import com.digitalasset.canton.protocol.ConfirmationPolicy.Signatory
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  signatoryParticipant,
  submittingParticipant,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class ConfirmationPolicyTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private lazy val gen = new ExampleTransactionFactory()(confirmationPolicy = Signatory)

  private lazy val alice: LfPartyId = LfPartyId.assertFromString("alice")
  private lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")
  private lazy val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  private lazy val david: LfPartyId = LfPartyId.assertFromString("david")

  "Choice of a confirmation policy" when {
    "nodes with a signatory without confirming participant" should {
      "fail to provide a valid confirming policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        val tx = gen
          .SingleExerciseWithNonstakeholderActor(ExampleTransactionFactory.lfHash(0))
          .versionedUnsuffixedTransaction

        when(
          topologySnapshot.activeParticipantsOfPartiesWithAttributes(any[Seq[LfPartyId]])(
            anyTraceContext
          )
        )
          .thenAnswer[Seq[LfPartyId]] { parties =>
            Future.successful(parties.map {
              case ExampleTransactionFactory.signatory =>
                // Give the signatory Observation permission, which shouldn't be enough to get a valid confirmation policy
                ExampleTransactionFactory.signatory -> Map(
                  signatoryParticipant -> ParticipantAttributes(Observation)
                )
              case otherParty =>
                otherParty -> Map(
                  submittingParticipant -> ParticipantAttributes(Submission)
                )
            }.toMap)
          }

        val policies = ConfirmationPolicy
          .choose(tx, topologySnapshot)
          .futureValue

        assert(policies == Seq.empty)
      }
    }

    "nodes without confirming parties" should {
      "fail to provide a valid confirming policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        val tx = gen
          .SingleExerciseWithoutConfirmingParties(ExampleTransactionFactory.lfHash(0))
          .versionedUnsuffixedTransaction

        when(
          topologySnapshot.activeParticipantsOfPartiesWithAttributes(any[Seq[LfPartyId]])(
            anyTraceContext
          )
        )
          .thenAnswer[Seq[LfPartyId]](parties =>
            Future.successful(
              parties
                .map(
                  _ -> Map(
                    submittingParticipant -> ParticipantAttributes(Submission)
                  )
                )
                .toMap
            )
          )

        val policies = ConfirmationPolicy
          .choose(tx, topologySnapshot)
          .futureValue

        assert(policies == Seq.empty)
      }
    }

    "some views have no VIP participant" should {
      "fall back to Signatory policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(
          topologySnapshot.activeParticipantsOfPartiesWithAttributes(any[Seq[LfPartyId]])(
            anyTraceContext
          )
        )
          .thenAnswer[Seq[LfPartyId]](parties =>
            Future.successful(
              parties
                .map(
                  _ -> Map(
                    submittingParticipant -> ParticipantAttributes(Submission)
                  )
                )
                .toMap
            )
          )
        val policies = gen.standardHappyCases
          .map(_.versionedUnsuffixedTransaction)
          .filter(
            _.nodes.nonEmpty
          ) // TODO (M12, i1046) handling of empty transaction remains a bit murky
          .map(ConfirmationPolicy.choose(_, topologySnapshot).futureValue)
        assert(policies.forall(_.headOption === Some(Signatory)))
      }
    }

    "a view's VIPs are not stakeholders" should {
      "fall back to Signatory policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(
          topologySnapshot.activeParticipantsOfPartiesWithAttributes(any[Seq[LfPartyId]])(
            anyTraceContext
          )
        )
          .thenAnswer[Seq[LfPartyId]](parties =>
            Future.successful(
              parties.map {
                case ExampleTransactionFactory.submitter =>
                  ExampleTransactionFactory.submitter -> Map(
                    submittingParticipant -> ParticipantAttributes(Submission)
                  )
                case ExampleTransactionFactory.signatory =>
                  ExampleTransactionFactory.signatory -> Map(
                    signatoryParticipant -> ParticipantAttributes(Submission)
                  )
                case ExampleTransactionFactory.observer =>
                  ExampleTransactionFactory.observer -> Map(
                    submittingParticipant -> ParticipantAttributes(Submission)
                  )
                case otherwise => sys.error(s"unexpected party: $otherwise")
              }.toMap
            )
          )

        val policies = ConfirmationPolicy
          .choose(
            gen
              .SingleExerciseWithNonstakeholderActor(ExampleTransactionFactory.lfHash(0))
              .versionedUnsuffixedTransaction,
            topologySnapshot,
          )
          .futureValue
        assert(policies == Seq(Signatory))
      }
    }
  }

  "The signatory policy" when {
    "adding a submitting admin party" should {
      "correctly update informees and thresholds" in {
        val oldInformees = Set(
          PlainInformee(alice),
          ConfirmingParty(bob, PositiveInt.one),
          ConfirmingParty(charlie, PositiveInt.one),
        )
        val oldThreshold = NonNegativeInt.tryCreate(2)

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(None)(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(alice))(oldInformees, oldThreshold) shouldBe
          Set(
            ConfirmingParty(alice, PositiveInt.one),
            ConfirmingParty(bob, PositiveInt.one),
            ConfirmingParty(charlie, PositiveInt.one),
          ) -> NonNegativeInt.tryCreate(3)

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(bob))(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(charlie))(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(david))(oldInformees, oldThreshold) shouldBe
          oldInformees + ConfirmingParty(david, PositiveInt.one) ->
          NonNegativeInt.tryCreate(3)
      }
    }
  }
}
