// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.client.BaseTopologySnapshot.HostingView
import com.digitalasset.canton.topology.client.BaseTopologySnapshotTest.EffectiveConfirmationThresholdExample
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantAttributes,
  PartyToParticipant,
}
import com.digitalasset.canton.topology.{ParticipantId, TestingTopology}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

class BaseTopologySnapshotTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ProtocolVersionChecksAnyWordSpec {

  private def mapping(threshold: PositiveInt, hosts: HostingParticipant*): PartyToParticipant =
    PartyToParticipant.tryCreate(party1, threshold, hosts)

  private val allSubmission = Map(
    participant1 -> ParticipantAttributes(Submission),
    participant2 -> ParticipantAttributes(Submission),
  )

  private def partyInfo(
      mapping: PartyToParticipant,
      view: HostingView,
      participantAttributes: Map[ParticipantId, ParticipantAttributes] = allSubmission,
      protocolVersion: ProtocolVersion = testedProtocolVersion,
  ): Option[PartyInfo] =
    PartyInfo.fromMapping(mapping, participantAttributes, view, protocolVersion)

  /** For mappings without a Freezing entry, where both views must agree. */
  private def partyInfoInBothViews(
      mapping: PartyToParticipant,
      participantAttributes: Map[ParticipantId, ParticipantAttributes] = allSubmission,
  ): Option[PartyInfo] = {
    val result = partyInfo(mapping, HostingView.Default, participantAttributes)
    partyInfo(mapping, HostingView.IgnoringFreeze, participantAttributes) shouldBe result
    result
  }

  "PartyInfo.fromMapping" should {
    "lower each entry's permission to the participant's synchronizer permission" in {
      val loginAfter = Some(CantonTimestamp.ofEpochSecond(1))
      partyInfoInBothViews(
        mapping(
          PositiveInt.one,
          HostingParticipant(participant1, Submission),
          HostingParticipant(participant2, Observation),
        ),
        Map(
          participant1 -> ParticipantAttributes(Confirmation, loginAfter),
          participant2 -> ParticipantAttributes(Submission),
        ),
      ).value.participants shouldBe Map(
        participant1 -> ParticipantAttributes(Confirmation, loginAfter),
        participant2 -> ParticipantAttributes(Observation),
      )
    }

    "drop entries of participants that left the synchronizer" in {
      val ptp = mapping(
        PositiveInt.one,
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant3, Submission),
      )
      partyInfoInBothViews(ptp).value.participants.keySet shouldBe Set(participant1)
      partyInfoInBothViews(ptp, Map(participant2 -> ParticipantAttributes(Submission))) shouldBe
        None
    }

    "use the effective confirmation threshold only from ProtocolVersion.offPRWithSequencerChannels on" in {
      val withOnboardingEntry = mapping(
        PositiveInt.two,
        HostingParticipant(participant1, Confirmation),
        HostingParticipant(participant2, Confirmation, HostingState.Onboarding),
      )
      def threshold(protocolVersion: ProtocolVersion): PositiveInt =
        partyInfo(
          withOnboardingEntry,
          HostingView.Default,
          protocolVersion = protocolVersion,
        ).value.threshold

      forEvery(Seq[ProtocolVersion](ProtocolVersion.v35, ProtocolVersion.v36)) { protocolVersion =>
        threshold(protocolVersion) shouldBe PositiveInt.two
      }
      threshold(ProtocolVersion.offPRWithSequencerChannels) shouldBe PositiveInt.one
    }

    "count entries with a lower synchronizer permission or whose participant left the synchronizer in the effective confirmation threshold" in {
      // Excluding participant2 would lower the threshold to 1.
      val ptp = mapping(
        PositiveInt.two,
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Confirmation),
        HostingParticipant(participant3, Confirmation, HostingState.Onboarding),
      )
      val allHosts = allSubmission + (participant3 -> ParticipantAttributes(Submission))
      def threshold(participantAttributes: Map[ParticipantId, ParticipantAttributes]): PositiveInt =
        partyInfo(
          ptp,
          HostingView.Default,
          participantAttributes,
          ProtocolVersion.offPRWithSequencerChannels,
        ).value.threshold

      threshold(allHosts) shouldBe PositiveInt.two
      withClue("lower synchronizer permission")(
        threshold(allHosts + (participant2 -> ParticipantAttributes(Observation))) shouldBe
          PositiveInt.two
      )
      withClue("left the synchronizer")(threshold(allHosts - participant2) shouldBe PositiveInt.two)
    }

    "not let Onboarding entries confirm" in {
      val attributes = partyInfoInBothViews(
        mapping(
          PositiveInt.one,
          HostingParticipant(participant1, Confirmation, HostingState.Onboarding),
        )
      ).value.participants(participant1)
      attributes.onboarding shouldBe true
      attributes.canConfirm shouldBe false
    }

    "let Active entries with a confirming permission confirm" in {
      partyInfoInBothViews(
        mapping(PositiveInt.one, HostingParticipant(participant1, Confirmation))
      ).value
        .participants(participant1)
        .canConfirm shouldBe true
    }

    "not let Nominated entries host" in {
      partyInfoInBothViews(
        mapping(
          PositiveInt.one,
          HostingParticipant(participant1, Submission),
          HostingParticipant(participant2, Confirmation, HostingState.Nominated),
        )
      ).value.participants.keySet shouldBe Set(participant1)
    }

    "leave a party with only Nominated entries unhosted" in {
      partyInfoInBothViews(
        mapping(
          PositiveInt.one,
          HostingParticipant(participant1, Submission, HostingState.Nominated),
          HostingParticipant(participant2, Confirmation, HostingState.Nominated),
        )
      ) shouldBe None
    }

    "leave a party with a Freezing entry without hosts in the default view" in {
      val frozen = mapping(
        PositiveInt.one,
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Confirmation, HostingState.Freezing),
      )
      partyInfo(frozen, HostingView.Default) shouldBe None
    }

    "keep the other hosts of a party with a Freezing entry in the freeze-ignoring view" in {
      val frozen = mapping(
        PositiveInt.one,
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Confirmation, HostingState.Freezing),
      )
      partyInfo(frozen, HostingView.IgnoringFreeze).value.participants.keySet shouldBe
        Set(participant1)
    }

    "restore the hosts once the Freezing entry is removed or moves to Onboarding" in {
      val active = HostingParticipant(participant1, Submission)
      def p2In(state: HostingState) = HostingParticipant(participant2, Confirmation, state)

      partyInfo(mapping(PositiveInt.one, active, p2In(HostingState.Freezing)), HostingView.Default)
        .shouldBe(None)
      partyInfoInBothViews(mapping(PositiveInt.one, active)).value.participants.keySet shouldBe
        Set(participant1)
      partyInfoInBothViews(
        mapping(PositiveInt.one, active, p2In(HostingState.Onboarding))
      ).value.participants.keySet shouldBe Set(participant1, participant2)
    }
  }

  "the effective confirmation threshold" should {
    val loop = participant1
    val da1 = participant2
    val mpch1 = participant3
    val allHosts = allSubmission + (participant3 -> ParticipantAttributes(Submission))

    def active(participant: ParticipantId) = HostingParticipant(participant, Confirmation)
    def nominated(participant: ParticipantId) =
      HostingParticipant(participant, Confirmation, HostingState.Nominated)
    def onboarding(participant: ParticipantId) =
      HostingParticipant(participant, Confirmation, HostingState.Onboarding)

    def effectiveConfirmationThreshold(mapping: PartyToParticipant): Int =
      PartyInfo.effectiveConfirmationThreshold(mapping).unwrap
    def withConfirmingPermission(mapping: PartyToParticipant): Seq[HostingParticipant] =
      mapping.participants.filter(_.permission != Observation)
    def numConfirming(mapping: PartyToParticipant): Int = withConfirmingPermission(mapping).size
    def numConfirmingActive(mapping: PartyToParticipant): Int =
      withConfirmingPermission(mapping).count(_.state == HostingState.Active)
    def threshold(mapping: PartyToParticipant): Int = mapping.threshold.unwrap

    val hostingStates: Seq[HostingState] = Seq(
      HostingState.Nominated,
      HostingState.Freezing,
      HostingState.Onboarding,
      HostingState.Active,
    )

    /* Every mapping with up to three entries in any state and permission, with threshold 1 to 4. */
    lazy val smallMappings: Seq[PartyToParticipant] = {
      def entryChoices(participant: ParticipantId): Seq[Option[HostingParticipant]] =
        None +: (for {
          state <- hostingStates
          permission <- Seq(Observation, Confirmation, Submission)
        } yield Some(HostingParticipant(participant, permission, state)))
      for {
        entry1 <- entryChoices(participant1)
        entry2 <- entryChoices(participant2)
        entry3 <- entryChoices(participant3)
        thresholdValue <- 1 to 4
      } yield mapping(PositiveInt.tryCreate(thresholdValue), Seq(entry1, entry2, entry3).flatten*)
    }

    def withEntry(
        mapping: PartyToParticipant,
        entry: HostingParticipant,
        replacement: Option[HostingParticipant],
    ): PartyToParticipant =
      this.mapping(
        mapping.threshold,
        mapping.participants.flatMap(other =>
          if (other.participantId == entry.participantId) replacement else Some(other)
        )*
      )

    "be as expected in replication scenarios" in {
      val examples = Seq(
        EffectiveConfirmationThresholdExample(
          "1. Two replications running: Loop confirms alone",
          PositiveInt.two,
          Seq(active(loop), nominated(da1), nominated(mpch1)),
          Some(PositiveInt.one),
        ),
        EffectiveConfirmationThresholdExample(
          "2. DA1 finished: intent already reached",
          PositiveInt.two,
          Seq(active(loop), active(da1), nominated(mpch1)),
          Some(PositiveInt.two),
        ),
        EffectiveConfirmationThresholdExample(
          "3. DA1 aborted and removed",
          PositiveInt.two,
          Seq(active(loop), nominated(mpch1)),
          Some(PositiveInt.one),
        ),
        EffectiveConfirmationThresholdExample(
          "4. Both removed, threshold not reset: unconfirmable until the party lowers it",
          PositiveInt.two,
          Seq(active(loop)),
          Some(PositiveInt.two),
        ),
        EffectiveConfirmationThresholdExample(
          "5. Both complete",
          PositiveInt.two,
          Seq(active(loop), active(da1), active(mpch1)),
          Some(PositiveInt.two),
        ),
        EffectiveConfirmationThresholdExample(
          "6. A target in Onboarding at threshold = numConfirming: no longer blocks",
          PositiveInt.three,
          Seq(active(loop), active(da1), onboarding(mpch1)),
          Some(PositiveInt.two),
        ),
        EffectiveConfirmationThresholdExample(
          "7. Host left while one pending, threshold = numConfirming: unconfirmable (P4)",
          PositiveInt.three,
          Seq(active(loop), nominated(mpch1)),
          Some(PositiveInt.two),
        ),
        EffectiveConfirmationThresholdExample(
          "8. No active host: unhosted (P5)",
          PositiveInt.two,
          Seq(nominated(da1), nominated(mpch1)),
          None,
        ),
        EffectiveConfirmationThresholdExample(
          "9. Host left while one pending, threshold < numConfirming: before the departure",
          PositiveInt.two,
          Seq(active(loop), active(da1), nominated(mpch1)),
          Some(PositiveInt.two),
        ),
        EffectiveConfirmationThresholdExample(
          "9. Host left while one pending, threshold < numConfirming: quorum halved (gap in P4)",
          PositiveInt.two,
          Seq(active(loop), nominated(mpch1)),
          Some(PositiveInt.one),
        ),
      )
      forEvery(examples) { example =>
        withClue(example.name) {
          partyInfo(
            mapping(example.threshold, example.entries*),
            HostingView.Default,
            allHosts,
            ProtocolVersion.offPRWithSequencerChannels,
          ).map(_.threshold) shouldBe example.expectedEffectiveConfirmationThreshold
        }
      }
    }

    "P1: never exceed the threshold" in {
      forEvery(smallMappings)(mapping =>
        effectiveConfirmationThreshold(mapping) should be <= threshold(mapping)
      )
    }

    "P2: be the threshold if all entries are Active and threshold <= numConfirming" in {
      forEvery(
        smallMappings.filter(mapping =>
          mapping.participants.forall(_.state == HostingState.Active) &&
            threshold(mapping) <= numConfirming(mapping)
        )
      )(mapping => effectiveConfirmationThreshold(mapping) shouldBe threshold(mapping))
    }

    "P3: stay unchanged when a confirming Nominated entry is added without changing the threshold, if threshold <= numConfirming before" in {
      forEvery(smallMappings.filter(mapping => threshold(mapping) <= numConfirming(mapping))) {
        mapping =>
          Seq(participant1, participant2, participant3)
            .find(participant => !mapping.participants.exists(_.participantId == participant))
            .foreach { free =>
              val nominatedAdded =
                this.mapping(mapping.threshold, (mapping.participants :+ nominated(free))*)
              effectiveConfirmationThreshold(
                nominatedAdded
              ) shouldBe effectiveConfirmationThreshold(mapping)
            }
      }
    }

    "P4: exceed numConfirmingActive if threshold > numConfirming, so the party is unconfirmable" in {
      forEvery(smallMappings.filter(mapping => threshold(mapping) > numConfirming(mapping)))(
        mapping => effectiveConfirmationThreshold(mapping) should be > numConfirmingActive(mapping)
      )
    }

    "P5: leave a party without Active confirming entry unhosted or without confirmer" in {
      val notFrozenWithoutActiveConfirmer = smallMappings.filter(mapping =>
        numConfirmingActive(mapping) == 0 && !mapping.participants.exists(
          _.state == HostingState.Freezing
        )
      )
      forEvery(notFrozenWithoutActiveConfirmer) { mapping =>
        val info =
          partyInfo(
            mapping,
            HostingView.Default,
            allHosts,
            ProtocolVersion.offPRWithSequencerChannels,
          )
        if (mapping.participants.exists(_.state.canHost)) {
          info.value.participants.values.exists(_.canConfirm) shouldBe false
          // without an Active confirming entry, numConfirmingPending = numConfirming
          info.value.threshold.unwrap shouldBe math.max(
            1,
            threshold(mapping) - numConfirming(mapping),
          )
        } else info shouldBe None
      }
    }

    "P6: leave a frozen party without hosts" in {
      forEvery(smallMappings.filter(_.participants.exists(_.state == HostingState.Freezing))) {
        mapping =>
          partyInfo(
            mapping,
            HostingView.Default,
            allHosts,
            ProtocolVersion.offPRWithSequencerChannels,
          ) shouldBe
            None
      }
    }

    "P7: drop only when an Active entry leaves the confirming set" in {
      import scala.math.Ordering.Implicits.*

      forEvery(smallMappings) { mapping =>
        forEvery(mapping.participants) { entry =>
          def after(replacement: Option[HostingParticipant]): Int =
            effectiveConfirmationThreshold(withEntry(mapping, entry, replacement))
          // the participant alone can make every forward move, including those that skip states
          forEvery(hostingStates.filter(_ > entry.state)) { state =>
            val moved = after(Some(entry.copy(state = state)))
            if (state == HostingState.Active)
              moved should be >= effectiveConfirmationThreshold(mapping)
            else moved shouldBe effectiveConfirmationThreshold(mapping)
          }
          if (entry.state != HostingState.Active) {
            after(None) should be >= effectiveConfirmationThreshold(mapping)
            after(
              Some(entry.copy(permission = Observation))
            ) should be >= effectiveConfirmationThreshold(mapping)
          }
        }
      }
    }

    "P8: be the threshold if no entry has a confirming permission" in {
      forEvery(smallMappings.filter(numConfirming(_) == 0))(mapping =>
        effectiveConfirmationThreshold(mapping) shouldBe threshold(mapping)
      )
    }
  }

  "a topology snapshot" should {
    // Topology validation only accepts the explicit allocation of an admin party: its own
    // participant as the single host with Submission. The hosting state is not restricted.
    "keep admin parties hosted while their mapping has a Freezing entry" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      val adminParty = participant1.adminParty.toLf
      val snapshot = TestingTopology()
        .withReversedTopology(
          Map(
            participant1 -> Map(adminParty -> Submission, party1.toLf -> Submission),
            participant2 -> Map(party1.toLf -> Confirmation),
          )
        )
        .withHostingStates(
          Map(
            adminParty -> Map(participant1 -> HostingState.Freezing),
            party1.toLf -> Map(participant2 -> HostingState.Freezing),
          )
        )
        .value
        .build(loggerFactory)
        .topologySnapshot()

      snapshot.activeParticipantsOf(adminParty).futureValueUS.keySet shouldBe Set(participant1)
      snapshot.activeParticipantsOf(party1.toLf).futureValueUS shouldBe empty
    }

    "let a single Submission host submit at threshold 2 exactly when the effective confirmation threshold is 1 and the party is not frozen" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      def canSubmit(secondHostState: HostingState): Boolean =
        TestingTopology()
          .withThreshold(
            Map(
              party1.toLf -> (PositiveInt.two, Seq(
                participant1 -> Submission,
                participant2 -> Confirmation,
              ))
            )
          )
          .withHostingStates(Map(party1.toLf -> Map(participant2 -> secondHostState)))
          .value
          .build(loggerFactory)
          .topologySnapshot()
          .canNotSubmit(participant1, Seq(party1.toLf))
          .futureValueUS
          .isEmpty

      withClue("effective confirmation threshold = 2")(
        canSubmit(HostingState.Active) shouldBe false
      )
      withClue("effective confirmation threshold = 1")(
        canSubmit(HostingState.Nominated) shouldBe true
      )
      withClue("effective confirmation threshold = 1")(
        canSubmit(HostingState.Onboarding) shouldBe true
      )
      withClue("frozen")(canSubmit(HostingState.Freezing) shouldBe false)
    }
  }
}

object BaseTopologySnapshotTest {

  /** A party's mapping and the threshold its hosting view reports.
    *
    * @param name
    *   The scenario, numbered for reference.
    * @param threshold
    *   The mapping's threshold.
    * @param entries
    *   The mapping's entries.
    * @param expectedEffectiveConfirmationThreshold
    *   The threshold of the party's info in the default hosting view, or `None` if the party is
    *   unhosted.
    */
  private final case class EffectiveConfirmationThresholdExample(
      name: String,
      threshold: PositiveInt,
      entries: Seq[HostingParticipant],
      expectedEffectiveConfirmationThreshold: Option[PositiveInt],
  )
}
