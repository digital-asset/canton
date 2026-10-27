// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.protocol.{DynamicSynchronizerParameters, SynchronizerParameters}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.{
  ParticipantAttributes,
  ParticipantPermission,
  PartyToParticipant,
  TopologyTransaction,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class TestingIdentityFactoryTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ProtocolVersionChecksAnyWordSpec {

  import DefaultTestIdentities.*

  private def getMyHash(hashOps: HashOps, message: String = "dummySignature"): Hash =
    hashOps.build(TestHash.testHashPurpose).addWithoutLengthPrefix(message).finish()
  def await(eitherT: EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]) =
    eitherT.value.futureValueUS

  private def increaseConfirmationResponseTimeout(old: DynamicSynchronizerParameters) =
    old.tryUpdate(confirmationResponseTimeout =
      old.confirmationResponseTimeout + NonNegativeFiniteDuration.tryOfSeconds(1)
    )

  private val synchronizerParameters1 = SynchronizerParameters.WithValidity(
    CantonTimestamp.Epoch,
    Some(CantonTimestamp.ofEpochSecond(10)),
    increaseConfirmationResponseTimeout(defaultDynamicSynchronizerParameters),
  )

  private val synchronizerParameters2 = SynchronizerParameters.WithValidity(
    CantonTimestamp.ofEpochSecond(10),
    None,
    increaseConfirmationResponseTimeout(synchronizerParameters1.parameter),
  )

  val synchronizerParameters = List(synchronizerParameters1, synchronizerParameters2)

  "testing topology" when {

    def compare(setup: TestingIdentityFactory): Unit = {
      val p1 = setup.forOwnerAndSynchronizer(participant1)
      val p2 = setup.forOwnerAndSynchronizer(participant2)
      val hash = getMyHash(p1.pureCrypto)
      val hash2 = getMyHash(p1.pureCrypto, "somethingElse")

      val signature =
        Await
          .result(
            p1.currentSnapshotApproximation.futureValueUS
              .sign(
                hash,
                SigningKeyUsage.ProtocolOnly,
                signingTimestampOverrides =
                  None, // not needed for unit tests; session signing keys disabled
              )
              .value,
            10.seconds,
          )
          .failOnShutdown
          .valueOr(err => fail(s"Failed to sign: $err"))

      "signature of participant1 is verifiable by participant1" in {
        await(
          p1.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash, participant1, signature, SigningKeyUsage.ProtocolOnly)
        ) shouldBe Either.unit
      }
      "signature of participant1 is verifiable by participant2" in {
        await(
          p2.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash, participant1, signature, SigningKeyUsage.ProtocolOnly)
        ) shouldBe Either.unit
      }
      "signature verification fails for wrong key owner" in {
        await(
          p1.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash, participant2, signature, SigningKeyUsage.ProtocolOnly)
        ).left.value shouldBe a[SignatureCheckError.SignerHasNoValidKeys]
      }
      "signature fails for invalid hash" in {
        await(
          p1.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash2, participant1, signature, SigningKeyUsage.ProtocolOnly)
        ).left.value shouldBe a[SignatureCheckError]
        await(
          p1.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash2, participant2, signature, SigningKeyUsage.ProtocolOnly)
        ).left.value shouldBe a[SignatureCheckError]
      }
      "signature fails for invalid key usage" in {
        await(
          p1.currentSnapshotApproximation.futureValueUS
            .verifySignature(
              hash,
              participant1,
              signature,
              SigningKeyUsage.SequencerAuthenticationOnly,
            )
        ).left.value shouldBe a[SignatureCheckError.SignatureWithWrongKey]
      }
      "participant1 is active" in {
        Seq(p1, p2).foreach(
          _.currentSnapshotApproximation.futureValueUS.ipsSnapshot
            .isParticipantActive(participant1)
            .futureValueUS shouldBe true
        )
      }
      "party1 is active" in {
        p1.currentSnapshotApproximation.futureValueUS.ipsSnapshot
          .activeParticipantsOf(party1.toLf)
          .futureValueUS shouldBe Map(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation)
        )
      }
      "participant2 can't sign messages without appropriate keys" in {
        Await
          .result(
            p2.currentSnapshotApproximation.futureValueUS
              .sign(
                hash,
                SigningKeyUsage.ProtocolOnly,
                signingTimestampOverrides =
                  None, // not needed for unit tests; session signing keys disabled
              )
              .value,
            10.seconds,
          )
          .failOnShutdown
          .left
          .value shouldBe a[SyncCryptoError]
      }

      def checkSynchronizerKeys(
          sequencers: Seq[SequencerId],
          mediators: Seq[MediatorId],
          expectedLength: Int,
      ): Unit = {
        val allMembers = sequencers ++ mediators
        val membersToKeys = p1.currentSnapshotApproximation.futureValueUS.ipsSnapshot
          .signingKeys(allMembers, SigningKeyUsage.All)
          .futureValueUS
        allMembers
          .flatMap(membersToKeys.get(_))
          .foreach(_ should have length expectedLength.toLong)
      }

      "synchronizer entities have keys" in {
        val sequencers = p1.currentSnapshotApproximation.futureValueUS.ipsSnapshot
          .sequencerGroup()
          .futureValueUS
          .valueOrFail("did not find SequencerSynchronizerState")
          .active

        val mediators =
          p1.currentSnapshotApproximation.futureValueUS.ipsSnapshot
            .mediatorGroups()
            .futureValueUS
            .flatMap(_.all)
        // We expect two different signing keys: one for protocol signing and one for sequencer authentication.
        checkSynchronizerKeys(sequencers, mediators, 2)
      }
      "invalid synchronizer entities don't have keys" in {
        val did = participant2.uid
        require(did != DefaultTestIdentities.synchronizerId.unwrap)
        checkSynchronizerKeys(
          sequencers = Seq(SequencerId(participant2.uid.tryChangeId("fake-sequencer"))),
          mediators = Seq(MediatorId(participant2.uid.tryChangeId("fake-mediator"))),
          0,
        )
      }

      "serve synchronizer parameters corresponding to correct timestamp" in {
        def getParameters(ts: CantonTimestamp): DynamicSynchronizerParameters =
          p1.ips
            .awaitSnapshot(ts)
            .flatMap(_.findDynamicSynchronizerParametersOrDefault(testedProtocolVersion))
            .futureValueUS

        val transitionTs = synchronizerParameters1.validUntil.value

        getParameters(CantonTimestamp.Epoch) shouldBe defaultDynamicSynchronizerParameters
        getParameters(transitionTs.minusMillis(1)) shouldBe synchronizerParameters1.parameter

        getParameters(
          transitionTs
        ) shouldBe synchronizerParameters1.parameter // validFrom is exclusive

        getParameters(transitionTs.plusMillis(1)) shouldBe synchronizerParameters2.parameter
      }

    }

    "initialised directly" should {
      val topology = Map(
        party1.toLf -> Map(
          participant1 -> ParticipantPermission.Confirmation
        )
      )
      val setup = TestingTopology
        .from(
          topology = topology,
          synchronizerParameters = synchronizerParameters,
          participants = Map(
            participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation)
          ),
        )
        .build()
      compare(setup)
      // extend with admin parties should give participant2 a signing key
      val crypto2 = TestingTopology
        .from(topology = topology, synchronizerParameters = synchronizerParameters)
        .withParticipants(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation),
          participant2 -> ParticipantAttributes(ParticipantPermission.Submission),
        )
        .build()
      val p1 = crypto2.forOwnerAndSynchronizer(participant1)
      val p2 = crypto2.forOwnerAndSynchronizer(participant2)

      "extending with admin parties works" in {
        def check(p: ParticipantId) =
          p1.currentSnapshotApproximation.futureValueUS.ipsSnapshot
            .activeParticipantsOf(p.adminParty.toLf)
            .futureValueUS
            .keys shouldBe Set(p)
        check(participant1)
        check(participant2)

      }

      val hash = getMyHash(p2.currentSnapshotApproximation.futureValueUS.pureCrypto)

      val signature =
        Await
          .result(
            p2.currentSnapshotApproximation.futureValueUS
              .sign(
                hash,
                SigningKeyUsage.ProtocolOnly,
                signingTimestampOverrides =
                  None, // not needed for unit tests; session signing keys disabled
              )
              .value,
            10.seconds,
          )
          .failOnShutdown
          .valueOr(err => fail(s"Failed to sign: $err"))

      "participant2 signatures are valid" in {
        await(
          p2.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash, participant2, signature, SigningKeyUsage.ProtocolOnly)
        ) shouldBe Either.unit
        await(
          p1.currentSnapshotApproximation.futureValueUS
            .verifySignature(hash, participant1, signature, SigningKeyUsage.ProtocolOnly)
        ).left.value shouldBe a[SignatureCheckError]
      }

    }

    "using reverse topology" should {
      val setup = TestingTopology(synchronizerParameters = synchronizerParameters)
        .withReversedTopology(
          Map(participant1 -> Map(party1.toLf -> ParticipantPermission.Confirmation))
        )
        .withParticipants(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation)
        )
        .build()
      compare(setup)

      "preserve topology and permissions" in {
        val syncCryptoApi =
          TestingTopology()
            .withReversedTopology(
              Map(
                participant1 -> Map(
                  party1.toLf -> ParticipantPermission.Observation,
                  party2.toLf -> ParticipantPermission.Confirmation,
                ),
                participant2 -> Map(party1.toLf -> ParticipantPermission.Submission),
              )
            )
            .build()
            .forOwnerAndSynchronizer(participant1)
            .currentSnapshotApproximation
            .futureValueUS
        def ol(permission: ParticipantPermission) =
          ParticipantAttributes(permission)
        syncCryptoApi.ipsSnapshot.activeParticipantsOf(party1.toLf).futureValueUS shouldBe Map(
          participant1 -> ol(ParticipantPermission.Observation),
          participant2 -> ol(ParticipantPermission.Submission),
        )
        syncCryptoApi.ipsSnapshot.activeParticipantsOf(party3.toLf).futureValueUS shouldBe Map()
        syncCryptoApi.ipsSnapshot.activeParticipantsOf(party2.toLf).futureValueUS shouldBe Map(
          participant1 -> ol(ParticipantPermission.Confirmation)
        )
      }
    }

    "withTopology" should {
      val setup = TestingTopology(synchronizerParameters = synchronizerParameters)
        .withTopology(
          Map(party1.toLf -> participant1),
          ParticipantPermission.Confirmation,
        )
        .withParticipants(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation)
        )
        .build()
      compare(setup)
    }

  }

  "withHostingStates" should {
    def storedHostingStates(topology: TestingTopology): Map[ParticipantId, HostingState] =
      topology
        .build(loggerFactory)
        .partyToParticipantTxs()
        .toSeq
        .flatMap { tx =>
          // Bytes the testing topology just produced, so the limits are never read.
          TopologyTransaction
            .fromTrustedByteString(
              testedTopologyTransactionContext
            )(tx.transaction.toByteStringChecked)
            .value
            .selectMapping[PartyToParticipant]
        }
        .flatMap(_.mapping.participants.map(host => host.participantId -> host.state))
        .toMap

    def supportsAllHostingStates(pv: ProtocolVersion): Boolean =
      TestingTopology.supportsAllHostingStates(pv).value

    val confirmingTopology = TestingTopology.from(topology =
      Map(
        party1.toLf -> Map(
          participant1 -> ParticipantPermission.Confirmation,
          participant2 -> ParticipantPermission.Confirmation,
        )
      )
    )

    val onboardingTopology = TestingTopology(topology =
      Map(
        party1.toLf -> PartyInfo.forTesting(
          PositiveInt.one,
          Map(
            participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation),
            participant2 ->
              ParticipantAttributes(ParticipantPermission.Confirmation, onboarding = true),
          ),
        )
      )
    )

    "store each entry with exactly its hosting state" onlyRunWhen supportsAllHostingStates in {
      val states = Map(
        participant1 -> HostingState.Nominated,
        participant2 -> HostingState.Freezing,
        participant3 -> HostingState.Onboarding,
        participant4 -> HostingState.Active,
      )
      val topology = TestingTopology
        .from(topology = Map(party1.toLf -> states.fmap(_ => ParticipantPermission.Confirmation)))
        .withHostingStates(Map(party1.toLf -> states))
        .value

      storedHostingStates(topology) shouldBe states
    }

    "store entries with onboarding = true as Onboarding" in {
      storedHostingStates(onboardingTopology) shouldBe Map(
        participant1 -> HostingState.Active,
        participant2 -> HostingState.Onboarding,
      )
    }

    "reject states for entries missing from the topology" in {
      confirmingTopology
        .withHostingStates(Map(party1.toLf -> Map(participant3 -> HostingState.Active)))
        .left
        .value should include("missing from the topology")
    }

    "reject states that conflict with onboarding = true" in {
      onboardingTopology
        .withHostingStates(Map(party1.toLf -> Map(participant2 -> HostingState.Active)))
        .left
        .value should include("conflict with onboarding = true")
    }

    "reject Nominated and Freezing on a protocol version without them" onlyRunWhen (pv =>
      !supportsAllHostingStates(pv)
    ) in {
      Seq(HostingState.Nominated, HostingState.Freezing).foreach { state =>
        confirmingTopology
          .withHostingStates(Map(party1.toLf -> Map(participant1 -> state)))
          .left
          .value should include("require PartyToParticipant v31")
      }
    }
  }
}
