// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.checks.OptionalTopologyMappingChecks
import com.digitalasset.canton.topology.{DefaultTestIdentities, MediatorId, SequencerId}
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.nowarn

@nowarn("msg=match may not be exhaustive")
class OptionalTopologyMappingChecksTest
    extends BaseTopologyMappingChecksTest[OptionalTopologyMappingChecks] {

  override protected def mkChecks(
      store: TopologyStore[SynchronizerStore]
  ): OptionalTopologyMappingChecks =
    new OptionalTopologyMappingChecks(
      store,
      loggerFactory,
    )

  "OptionalTopologyMappingChecks" when {
    import factory.TestingTransactions.*

    "validating MediatorSynchronizerState" should {

      "report MembersCannotRejoinSynchronizer for mediators that are being re-onboarded" in {
        val (checks, store) = mk()
        val (Seq(med1, med2, med3), transactions) = generateMemberIdentities(3, MediatorId(_))

        val Seq(group0, group1) = mkMediatorGroups(
          PositiveInt.one,
          NonNegativeInt.zero -> Seq(med1, med3),
          NonNegativeInt.one -> Seq(med2, med3),
        )

        addToStore(store, (transactions :+ group0 :+ group1)*)

        val Seq(group0RemoveMed1, group1RemoveMed2) = mkMediatorGroups(
          PositiveInt.two,
          NonNegativeInt.zero -> Seq(med3),
          NonNegativeInt.one -> Seq(med3),
        )

        store
          .update(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            removals = Map(
              group0.mapping.uniqueKey -> (Some(PositiveInt.one), Set.empty),
              group1.mapping.uniqueKey -> (Some(PositiveInt.one), Set.empty),
            ),
            additions = Seq(
              ValidatedTopologyTransaction(group0RemoveMed1),
              ValidatedTopologyTransaction(group1RemoveMed2),
            ),
          )
          .futureValueUS

        val Seq(med1RejoinsGroup0, med2RejoinsGroup0) = mkMediatorGroups(
          PositiveInt.three,
          // try joining the same group
          NonNegativeInt.zero -> Seq(med1, med3),
          // try joining another group
          NonNegativeInt.zero -> Seq(med2, med3),
        )

        checkTransaction(checks, med1RejoinsGroup0, Some(group0RemoveMed1)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(Seq(med1))
        )

        checkTransaction(checks, med2RejoinsGroup0, Some(group0RemoveMed1)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(Seq(med2))
        )
      }

    }

    "validating SequencerSynchronizerState" should {

      "report MembersCannotRejoinSynchronizer for sequencers that are being re-onboarded" in {
        val (checks, store) = mk()
        val (Seq(seq1, seq2), transactions) = generateMemberIdentities(2, SequencerId(_))

        val sds_S1_S2 = makeSynchronizerState(
          PositiveInt.one,
          seq1,
          seq2,
        )

        addToStore(store, (transactions :+ sds_S1_S2)*)

        val sds_S1 = makeSynchronizerState(PositiveInt.two, seq1)

        store
          .update(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            removals = Map(
              sds_S1.mapping.uniqueKey -> (Some(PositiveInt.one), Set.empty)
            ),
            additions = Seq(
              ValidatedTopologyTransaction(sds_S1)
            ),
          )
          .futureValueUS

        val sds_S1_rejoining_S2 = makeSynchronizerState(PositiveInt.three, seq1, seq2)

        checkTransaction(checks, sds_S1_rejoining_S2, Some(sds_S1)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(Seq(seq2))
        )
      }

    }

    "validating PartyToParticipant HostingState transitions" should {
      import DefaultTestIdentities.participant1
      import scala.math.Ordering.Implicits.*

      "reject change from Active to Onboarding state in every topology transaction version" in {
        val (checks, _) = mk()
        val versions = ProtocolVersion.v36 +: Option
          .when(testedProtocolVersion >= ProtocolVersion.v37)(ProtocolVersion.v37)
          .toList

        for {
          inStoreVersion <- versions
          toValidateVersion <- versions
        } withClue(s"in store: $inStoreVersion, proposal: $toValidateVersion") {
          val inStore = mkPtp(PositiveInt.one, inStoreVersion, participant1 -> HostingState.Active)
          val toValidate =
            mkPtp(PositiveInt.two, toValidateVersion, participant1 -> HostingState.Onboarding)
          checkTransaction(checks, toValidate, Some(inStore)) shouldBe
            invalidTransitions((participant1, HostingState.Active, HostingState.Onboarding))
        }
      }

      "allow unchanged and forward hosting state transitions" in {
        val (checks, _) = mk()

        for {
          from <- supportedStates
          to <- supportedStates if to >= from
        } withClue(s"$from -> $to") {
          val inStore = mkPtp(PositiveInt.one, participant1 -> from)
          val toValidate = mkPtp(PositiveInt.two, participant1 -> to)
          checkTransaction(checks, toValidate, Some(inStore)) shouldBe Either.unit
        }
      }

      "allow unchanged and forward hosting state transitions across v30 and v31 transactions" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        val (checks, _) = mk()
        val v30States = Seq(HostingState.Onboarding, HostingState.Active)

        for {
          (inStoreVersion, inStoreStates, toValidateVersion, toValidateStates) <- Seq(
            (ProtocolVersion.v36, v30States, ProtocolVersion.v36, v30States),
            (ProtocolVersion.v36, v30States, ProtocolVersion.v37, supportedStates),
            (ProtocolVersion.v37, supportedStates, ProtocolVersion.v36, v30States),
          )
          from <- inStoreStates
          to <- toValidateStates if to >= from
        } withClue(s"in store: $inStoreVersion $from, proposal: $toValidateVersion $to") {
          val inStore = mkPtp(PositiveInt.one, inStoreVersion, participant1 -> from)
          val toValidate = mkPtp(PositiveInt.two, toValidateVersion, participant1 -> to)
          checkTransaction(checks, toValidate, Some(inStore)) shouldBe Either.unit
        }
      }
    }

  }

}
