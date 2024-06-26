// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.PartyExceedsHostingLimit
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TopologyStoreX,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Namespace,
  ParticipantId,
  TestingOwnerWithKeysX,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class ValidatingTopologyMappingXChecksTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  private lazy val factory = new TestingOwnerWithKeysX(
    DefaultTestIdentities.mediatorIdX,
    loggerFactory,
    initEc = parallelExecutionContext,
  )

  def mk() = {
    val store = new InMemoryTopologyStoreX(AuthorizedStore, loggerFactory, timeouts)
    val check = new ValidatingTopologyMappingXChecks(store, loggerFactory)
    (check, store)
  }

  "TopologyMappingXChecks" when {
    import DefaultTestIdentities.*
    import factory.TestingTransactions.*

    implicit def toHostingParticipant(
        participantToPermission: (ParticipantId, ParticipantPermission)
    ): HostingParticipant =
      HostingParticipant(participantToPermission._1, participantToPermission._2)

    "validating DecentralizedNamespaceDefinition" should {
      "reject namespaces not derived from their owners' namespaces" in {
        val (checks, store) = mk()
        val keys = NonEmpty.mk(
          Set,
          factory.SigningKeys.key1,
          factory.SigningKeys.key2,
          factory.SigningKeys.key3,
        )
        val (namespaces, rootCerts) =
          keys.map { key =>
            val namespace = Namespace(key.fingerprint)
            namespace -> factory.mkAdd(
              NamespaceDelegationX.tryCreate(
                namespace,
                key,
                isRootDelegation = true,
              ),
              signingKey = key,
            )
          }.unzip

        addToStore(store, rootCerts.toSeq*)

        val dns = factory.mkAddMultiKey(
          DecentralizedNamespaceDefinitionX
            .create(
              Namespace(Fingerprint.tryCreate("bogusNamespace")),
              PositiveInt.one,
              NonEmpty.from(namespaces).value.toSet,
            )
            .value,
          signingKeys = keys,
          // using serial=2 here to test that we don't special case serial=1
          serial = PositiveInt.two,
        )

        checks
          .checkTransaction(EffectiveTime.MaxValue, dns, None)
          .value
          .futureValue should matchPattern {
          case Left(TopologyTransactionRejection.InvalidTopologyMapping(err))
              if err.contains("not derived from the owners") =>
        }
      }

      // TODO(#19716) how does one produce a key with a specific hash? by using symbolic crypto?
      "reject if a root certificate with the same namespace already exists" ignore {
        fail("TODO(#19716)")
      }
    }

    "validating NamespaceDelegation" should {
      // TODO(#19715) how does one produce a key with a specific hash? by using symbolic crypto?
      "reject a root certificate if a decentralized namespace with the same namespace already exists" ignore {
        fail("TODO(#19715)")
      }
    }

    "validating PartyToParticipantX" should {

      "reject when participants don't have a DTC" in {
        val (checks, store) = mk()
        addToStore(store, p2_dtc)

        val failureCases = Seq(Seq(participant1), Seq(participant1, participant2))

        failureCases.foreach { participants =>
          val ptp = factory.mkAdd(
            PartyToParticipantX.tryCreate(
              party1,
              None,
              PositiveInt.one,
              participants.map[HostingParticipant](_ -> Submission),
              groupAddressing = false,
            )
          )
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue shouldBe Left(
            TopologyTransactionRejection.UnknownMembers(Seq(participant1))
          )
        }
      }

      "reject when participants don't have a valid encryption or signing key" in {
        val (checks, store) = mk()
        val p2MissingEncKey = factory.mkAdd(
          OwnerToKeyMappingX(participant2, None, NonEmpty(Seq, factory.SigningKeys.key1))
        )
        val p3MissingSigningKey = factory.mkAdd(
          OwnerToKeyMappingX(participant3, None, NonEmpty(Seq, factory.EncryptionKeys.key1))
        )

        addToStore(store, p1_dtc, p2_dtc, p3_dtc, p2MissingEncKey, p3MissingSigningKey)

        val missingKeyCases = Seq(participant1, participant2, participant3)

        missingKeyCases.foreach { participant =>
          val ptp = factory.mkAdd(
            PartyToParticipantX.tryCreate(
              party1,
              None,
              PositiveInt.one,
              Seq(participant -> Submission),
              groupAddressing = false,
            )
          )
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue shouldBe Left(
            TopologyTransactionRejection.InsufficientKeys(Seq(participant))
          )
        }
      }

      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc, p3_otk, p3_dtc)

        val validCases = Seq[(PositiveInt, Seq[HostingParticipant])](
          PositiveInt.one -> Seq(participant1 -> Confirmation),
          PositiveInt.one -> Seq(participant1 -> Submission),
          PositiveInt.one -> Seq(participant1 -> Observation, participant2 -> Confirmation),
          PositiveInt.two -> Seq(participant1 -> Confirmation, participant2 -> Submission),
          PositiveInt.two -> Seq(
            participant1 -> Observation,
            participant2 -> Submission,
            participant3 -> Submission,
          ),
        )

        validCases.foreach { case (threshold, participants) =>
          val ptp = factory.mkAdd(
            PartyToParticipantX.tryCreate(
              party1,
              None,
              threshold,
              participants,
              groupAddressing = false,
            )
          )
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue shouldBe Right(())
        }
      }

      "reject when the party exceeds the explicitly issued PartyHostingLimits" in {
        def mkPTP(numParticipants: Int) = {
          val hostingParticipants = Seq[HostingParticipant](
            participant1 -> Observation,
            participant2 -> Submission,
            participant3 -> Submission,
          )
          factory.mkAdd(
            PartyToParticipantX.tryCreate(
              partyId = party1,
              domainId = None,
              threshold = PositiveInt.one,
              participants = hostingParticipants.take(numParticipants),
              groupAddressing = false,
            )
          )
        }

        val (checks, store) = mk()
        val limits = factory.mkAdd(PartyHostingLimitsX(domainId, party1, 2))
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc, p3_otk, p3_dtc, limits)

        // 2 participants are at the limit
        val twoParticipants = mkPTP(numParticipants = 2)
        checks
          .checkTransaction(EffectiveTime.MaxValue, twoParticipants, None)
          .value
          .futureValue shouldBe Right(())

        // 3 participants exceed the limit imposed by the domain
        val threeParticipants = mkPTP(numParticipants = 3)
        checks
          .checkTransaction(EffectiveTime.MaxValue, threeParticipants, None)
          .value
          .futureValue shouldBe Left(
          PartyExceedsHostingLimit(party1, 2, 3)
        )
      }
    }

    "validating DomainTrustCertificateX" should {
      "reject a removal when the participant still hosts a party" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          PartyToParticipantX.tryCreate(
            party1,
            None,
            PositiveInt.one,
            Seq(participant1 -> Submission),
            groupAddressing = false,
          )
        )
        addToStore(
          store,
          ptp,
        )
        val prior = factory.mkAdd(DomainTrustCertificateX(participant1, domainId, false, Seq.empty))

        val dtc =
          factory.mkRemove(DomainTrustCertificateX(participant1, domainId, false, Seq.empty))

        val result = checks.checkTransaction(EffectiveTime.MaxValue, dtc, Some(prior))
        result.value.futureValue shouldBe Left(
          TopologyTransactionRejection.ParticipantStillHostsParties(participant1, Seq(party1))
        )

      }

      "reject the addition if the domain is locked" in {
        Seq(OnboardingRestriction.RestrictedLocked, OnboardingRestriction.UnrestrictedLocked)
          .foreach { restriction =>
            val (checks, store) = mk()
            val ptp = factory.mkAdd(
              DomainParametersStateX(
                domainId,
                DynamicDomainParameters
                  .defaultValues(testedProtocolVersion)
                  .tryUpdate(onboardingRestriction = restriction),
              )
            )
            addToStore(store, ptp)

            val dtc =
              factory.mkAdd(DomainTrustCertificateX(participant1, domainId, false, Seq.empty))

            val result = checks.checkTransaction(EffectiveTime.MaxValue, dtc, None)
            result.value.futureValue shouldBe Left(
              TopologyTransactionRejection.OnboardingRestrictionInPlace(
                participant1,
                restriction,
                None,
              )
            )
          }
      }

      "reject the addition if the domain is restricted" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          DomainParametersStateX(
            domainId,
            DynamicDomainParameters
              .defaultValues(testedProtocolVersion)
              .tryUpdate(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
          )
        )
        addToStore(
          store,
          ptp,
          factory.mkAdd(
            ParticipantDomainPermissionX(
              domainId,
              participant1,
              ParticipantPermission.Submission,
              None,
              None,
            )
          ),
        )

        val dtc =
          factory.mkAdd(DomainTrustCertificateX(participant2, domainId, false, Seq.empty))

        val result1 = checks.checkTransaction(EffectiveTime.MaxValue, dtc, None)
        result1.value.futureValue shouldBe Left(
          TopologyTransactionRejection.OnboardingRestrictionInPlace(
            participant2,
            OnboardingRestriction.RestrictedOpen,
            None,
          )
        )

        val result2 = checks.checkTransaction(
          EffectiveTime.MaxValue,
          factory.mkAdd(DomainTrustCertificateX(participant1, domainId, false, Seq.empty)),
          None,
        )
        result2.value.futureValue shouldBe Right(())

      }

    }

    "validating TrafficControlStateX" should {
      def trafficControlState(limit: Int): TrafficControlStateX =
        TrafficControlStateX
          .create(domainId, participant1, PositiveLong.tryCreate(limit.toLong))
          .getOrElse(sys.error("Error creating TrafficControlStateX"))

      val limit5 = factory.mkAdd(trafficControlState(5))
      val limit10 = factory.mkAdd(trafficControlState(10))
      val removal10 = factory.mkRemove(trafficControlState(10))

      "reject non monotonically increasing extra traffict limits" in {
        val (checks, _) = mk()

        val result =
          checks.checkTransaction(
            EffectiveTime.MaxValue,
            toValidate = limit5,
            inStore = Some(limit10),
          )
        result.value.futureValue shouldBe
          Left(
            TopologyTransactionRejection.ExtraTrafficLimitTooLow(
              participant1,
              PositiveLong.tryCreate(5),
              PositiveLong.tryCreate(10),
            )
          )

      }

      "report no errors for valid mappings" in {
        val (checks, _) = mk()

        def runSuccessfulCheck(
            toValidate: SignedTopologyTransactionX[TopologyChangeOpX, TrafficControlStateX],
            inStore: Option[SignedTopologyTransactionX[TopologyChangeOpX, TrafficControlStateX]],
        ) =
          checks
            .checkTransaction(EffectiveTime.MaxValue, toValidate, inStore)
            .value
            .futureValue shouldBe Right(())

        // first limit for member
        runSuccessfulCheck(limit10, None)

        // increase limit
        runSuccessfulCheck(limit10, Some(limit5))

        // same limit
        runSuccessfulCheck(limit5, Some(limit5))

        // reset monotonicity after removal
        runSuccessfulCheck(limit5, Some(removal10))

        // remove traffic control state for member
        runSuccessfulCheck(removal10, Some(limit10))
      }
    }
  }

  private def addToStore(
      store: TopologyStoreX[AuthorizedStore],
      transactions: GenericSignedTopologyTransactionX*
  ): Unit = {
    store
      .bootstrap(
        StoredTopologyTransactionsX(
          transactions.map(tx =>
            StoredTopologyTransactionX(SequencedTime.MinValue, EffectiveTime.MinValue, None, tx)
          )
        )
      )
      .futureValue
  }

}
