// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import cats.syntax.traverse.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.console.commands.PartiesAdministration
import com.digitalasset.canton.crypto.KeyPurpose.Signing
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningKeysWithThreshold}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.ExternalPartyAlreadyExists
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.topology.transaction.{
  DelegationRestriction,
  HostingParticipant,
  MultiTransactionSignature,
  NamespaceDelegation,
  ParticipantPermission,
  PartyToKeyMapping,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  Namespace,
  PartyId,
  TopologyManagerError,
}
import com.digitalasset.nonempty.NonEmpty

import java.util.UUID
import scala.concurrent.Future
import scala.util.Try

trait ExternalPartyOnboardingIntegrationTestSetup
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.foreach(_.dars.upload(CantonExamplesPath))

        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters.propose_update(
            synchronizer.synchronizerId,
            // Lower the confirmation response timeout to observe quickly rejections due to confirming
            // participants failing to respond in time
            _.update(confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(3)),
          )
        )
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
}

class ExternalPartyOnboardingIntegrationTest extends ExternalPartyOnboardingIntegrationTestSetup {
  "External party onboarding" should {
    "allocate a simple external party" in { implicit env =>
      import env.*

      val patrick = participant1.parties.testing.external.enable("Patrick")
      participant1.parties
        .list(filterParty = patrick.filterString)
        .loneElement
        .party shouldBe patrick.partyId
    }

    "sign the external party transaction for the participant" when {
      // This test case checks that a participant signs external party allocations only for itself, even if
      // the participant owns a signing key with a delegation from another participant. This restriction
      // is in place to avoid a loophole where a participant might not want to host an external party,
      // but through a namespace delegation, the external party might try to acquire the participant's signature
      // through other means (e.g. by submitting the allocation via a participant with a namespace delegation in place).
      "the participant also has a delegated key for another participant" in { implicit env =>
        import env.*

        // set up the key delegation from participant2 to a key owned by participant1
        val delegatedKey =
          participant1.keys.secret.generate_signing_key(usage = Set(SigningKeyUsage.Namespace))
        participant2.topology.namespace_delegations.propose_delegation(
          participant2.namespace,
          delegatedKey,
          DelegationRestriction.CanSignSpecificMappings(PartyToParticipant.code),
          store = daId,
        )

        eventually() {
          participant1.topology.namespace_delegations
            .list(
              daId,
              filterNamespace = participant2.namespace.filterString,
              filterTargetKey = Some(delegatedKey.fingerprint),
            )
            .loneElement
            .discard
        }
        // validate that the delegation from participant2 to participant1's key actually works for PTPs
        participant1.parties.enable(
          "party-on-p2",
          namespace = participant2.namespace,
          synchronizer = daName,
        )

        val partyId = allocateExternalParty().partyId

        val ptp = eventually() {
          participant2.topology.party_to_participant_mappings
            .list(daId, proposals = true, filterParty = partyId)
            .loneElement
        }
        // future-proofing the test: ensure that the PTP has both participants as hosting participants (and therefore eligigle for signing it)
        ptp.item.participantIds should contain theSameElementsAs Seq(
          participant1.id,
          participant2.id,
        )
        // check that participant1 only signed with the key for participant1, even though it owns a delegated key for participant2
        ptp.context.signedBy.forgetNE.loneElement shouldBe participant1.fingerprint

        // clean up: revoke the delegation again
        participant2.topology.namespace_delegations.propose_revocation(
          participant2.namespace,
          targetKey = delegatedKey,
          store = daId,
        )

        eventually() {
          participant1.topology.namespace_delegations.list(
            daId,
            filterNamespace = participant2.namespace.filterString,
            filterTargetKey = Some(delegatedKey.fingerprint),
          ) shouldBe empty
        }

      }
      "the participant has an offline root key" in { implicit env =>
        import env.*

        // download the root key so that we can restore it at the end of the test
        val rootNamespaceKey = participant1.keys.secret.download(participant1.fingerprint)

        // delete the root namespace key to make it "offline"
        participant1.keys.secret.delete(participant1.fingerprint, force = true)

        // try to allocate the external party without any valid topology signing key
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          allocateExternalParty(),
          _.errorMessage should include(
            "Could not find an appropriate signing key to issue the topology transaction"
          ),
        )

        // temporarily restore the root namespace key and issue a namespace delegation
        // for an intermediate key
        participant1.keys.secret.upload(rootNamespaceKey, name = None)

        val intermediateKey =
          participant1.keys.secret.generate_signing_key(usage = Set(SigningKeyUsage.Namespace))
        participant1.topology.namespace_delegations.propose_delegation(
          participant1.namespace,
          intermediateKey,
          CanSignAllButNamespaceDelegations,
          store = daId,
        )

        eventually() {
          participant1.topology.namespace_delegations
            .list(daId, filterTargetKey = Some(intermediateKey.fingerprint))
            .loneElement
        }

        // delete the root namespace key again
        participant1.keys.secret.delete(participant1.fingerprint, force = true)

        // allocating the external party should work now
        val externalPartyId = allocateExternalParty().partyId

        // validate that the mapping for the external party exists,
        // and that it was only signed by intermediate key
        eventually() {
          participant1.topology.party_to_participant_mappings
            .list(
              daId,
              proposals = true,
              filterParty = externalPartyId,
            )
            .loneElement
            .context
            .signedBy
            .forgetNE should contain theSameElementsAs Seq(intermediateKey.fingerprint)
        }

        participant1.keys.secret.upload(rootNamespaceKey, name = None)
      }
    }

    /** Allocates an external party with a random name in sequencer1's namespace with participant1
      * and participant2 as the hosting participants.
      */
    def allocateExternalParty()(implicit env: TestConsoleEnvironment) = {
      import env.*
      participant1.ledger_api.parties
        .allocate_external(
          daId,
          Seq(
            TopologyTransaction.tryCreate(
              TopologyChangeOp.Replace,
              PositiveInt.one,
              PartyToParticipant.tryCreate(
                PartyId.tryCreate(UUID.randomUUID().toString, sequencer1.namespace),
                PositiveInt.one,
                Seq(
                  HostingParticipant(participant1.id, ParticipantPermission.Confirmation),
                  HostingParticipant(participant2.id, ParticipantPermission.Confirmation),
                ),
                partySigningKeysWithThreshold = Some(
                  SigningKeysWithThreshold(
                    NonEmpty(
                      Set,
                      // pick some key as the party's signing key
                      sequencer1.keys.public
                        .list(
                          filterPurpose = Set(Signing),
                          filterUsage = Set(SigningKeyUsage.Protocol),
                        )
                        .head
                        .publicKey
                        .asSigningKey
                        .value,
                    ),
                    PositiveInt.one,
                  )
                ),
              ),
              testedProtocolVersion,
            ) -> Seq.empty
          ),
          Seq.empty,
        )
    }

    "allocate a party with a PartyToKeyMapping" in { implicit env =>
      import env.*
      val namespaceKey = global_secret.keys.secret
        .generate_keys(PositiveInt.one, usage = NonEmpty.mk(Set, SigningKeyUsage.Namespace))
        .head
      val protocolKey = global_secret.keys.secret
        .generate_keys(PositiveInt.one, usage = NonEmpty.mk(Set, SigningKeyUsage.Protocol))
        .head
      val partyId = PartyId.tryCreate("Alice", namespaceKey.fingerprint)

      val namespaceDelegation = TopologyTransaction.tryCreate(
        mapping = NamespaceDelegation.tryCreate(
          Namespace(namespaceKey.fingerprint),
          namespaceKey,
          CanSignAllMappings,
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val partyToParticipant = TopologyTransaction.tryCreate(
        mapping = PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          participants = Seq(HostingParticipant(participant1, ParticipantPermission.Confirmation)),
          partySigningKeysWithThreshold = Option.empty,
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val partyToKeyMapping = TopologyTransaction.tryCreate(
        mapping = PartyToKeyMapping.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          signingKeys = NonEmpty.mk(Seq, protocolKey),
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val multihash = MultiTransactionSignature.computeCombinedHash(
        NonEmpty.mk(Set, namespaceDelegation.hash, partyToParticipant.hash, partyToKeyMapping.hash),
        tryGlobalCrypto.pureCrypto,
      )

      val signedMultiHash = global_secret.sign(
        multihash.getCryptographicEvidence,
        namespaceKey.fingerprint,
        SigningKeyUsage.NamespaceOnly,
      )
      val signedPtkWithProtocolKey = global_secret.sign(
        partyToKeyMapping.hash.hash.getCryptographicEvidence,
        protocolKey.fingerprint,
        SigningKeyUsage.ProofOfOwnershipOnly,
      )

      participant1.ledger_api.parties.allocate_external(
        synchronizer1Id,
        Seq(
          namespaceDelegation -> Seq.empty,
          partyToParticipant -> Seq.empty,
          partyToKeyMapping -> Seq(signedPtkWithProtocolKey),
        ),
        multiSignatures = Seq(signedMultiHash),
      )

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1),
        synchronizerId = synchronizer1Id.logical,
      )
    }

    "allocate a party with an explicit NamespaceDelegation" in { implicit env =>
      import env.*
      val namespaceKey = global_secret.keys.secret
        .generate_keys(PositiveInt.one, usage = NonEmpty.mk(Set, SigningKeyUsage.Namespace))
        .head
      val protocolKey = global_secret.keys.secret
        .generate_keys(PositiveInt.one, usage = NonEmpty.mk(Set, SigningKeyUsage.Protocol))
        .head
      val partyId = PartyId.tryCreate("Alice", namespaceKey.fingerprint)

      val namespaceDelegation = TopologyTransaction.tryCreate(
        mapping = NamespaceDelegation.tryCreate(
          Namespace(namespaceKey.fingerprint),
          namespaceKey,
          CanSignAllMappings,
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val partyToParticipant = TopologyTransaction.tryCreate(
        mapping = PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          participants = Seq(HostingParticipant(participant1, ParticipantPermission.Confirmation)),
          partySigningKeysWithThreshold = Some(
            SigningKeysWithThreshold.tryCreate(
              NonEmpty.mk(Seq, protocolKey),
              PositiveInt.one,
            )
          ),
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val multihash = MultiTransactionSignature.computeCombinedHash(
        NonEmpty.mk(Set, namespaceDelegation.hash, partyToParticipant.hash),
        tryGlobalCrypto.pureCrypto,
      )

      val signedMultiHash = global_secret.sign(
        multihash.getCryptographicEvidence,
        namespaceKey.fingerprint,
        SigningKeyUsage.NamespaceOnly,
      )
      val signedPtpWithProtocolKey = global_secret.sign(
        partyToParticipant.hash.hash.getCryptographicEvidence,
        protocolKey.fingerprint,
        SigningKeyUsage.ProofOfOwnershipOnly,
      )

      participant1.ledger_api.parties.allocate_external(
        synchronizer1Id,
        Seq(
          namespaceDelegation -> Seq.empty,
          partyToParticipant -> Seq(signedPtpWithProtocolKey),
        ),
        multiSignatures = Seq(signedMultiHash),
      )

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1),
        synchronizerId = synchronizer1Id.logical,
      )
    }

    "handle a party's threshold being higher than its number of hosting nodes" in { implicit env =>
      import env.*
      val (onboardingTransactions, externalParty) =
        participant1.parties.testing.external
          .onboarding_transactions(
            "Alice",
            additionalConfirming = Seq(participant2),
            confirmationThreshold = PositiveInt.two,
            preferredHashingSchemeVersion = testedHashingSchemeVersion,
          )
          .futureValueUS
          .value

      Seq(participant1, participant2).map { hostingNode =>
        hostingNode.ledger_api.parties.allocate_external(
          synchronizer1Id,
          onboardingTransactions.transactionsWithSingleSignature,
          multiSignatures = onboardingTransactions.multiTransactionSignatures,
        )
      }

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = externalParty.partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1, participant2, participant3),
        synchronizerId = synchronizer1Id.logical,
      )

      // P2 removes itself unilaterally - fails without the force flag
      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant2.topology.party_to_participant_mappings
          .propose_delta(
            externalParty.partyId,
            removes = Seq(participant2),
            store = synchronizer1Id.logical,
          ),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCommandFailure(TopologyManagerError.ConfirmingThresholdCannotBeReached),
              "expected command failure",
            )
          )
        ),
      )

      // P2 removes itself unilaterally - with the force flag
      participant2.topology.party_to_participant_mappings
        .propose_delta(
          externalParty.partyId,
          removes = Seq(participant2),
          store = synchronizer1Id.logical,
          forceFlags = ForceFlags(ForceFlag.AllowConfirmingThresholdCanBeMet),
          mustFullyAuthorize = true,
        )

      eventually() {
        participant1.topology.party_to_participant_mappings
          .list(
            synchronizerId = synchronizer1Id.logical,
            filterParty = externalParty.filterString,
          )
          .loneElement
          .item
          .participants
          .size shouldBe 1
      }

      // Threshold cannot be reached because there's not enough confirming nodes
      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant1.ledger_api.commands.submit(
          Seq(externalParty),
          Seq(createCycleCommand(externalParty, UUID.randomUUID().toString)),
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCommandFailure(MediatorError.Timeout),
              "expected transaction timeout",
            )
          )
        ),
      )
    }

    "host parties on multiple participants with a threshold" in { implicit env =>
      import env.*
      val (onboardingTransactions, externalParty) =
        participant1.parties.testing.external
          .onboarding_transactions(
            "Alice",
            additionalConfirming = Seq(participant2),
            observing = Seq(participant3),
            confirmationThreshold = PositiveInt.two,
          )
          .futureValueUS
          .value

      Seq(participant1, participant2, participant3).map { hostingNode =>
        hostingNode.ledger_api.parties.allocate_external(
          synchronizer1Id,
          onboardingTransactions.transactionsWithSingleSignature,
          multiSignatures = onboardingTransactions.multiTransactionSignatures,
        )
      }

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = externalParty.partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1, participant2, participant3),
        synchronizerId = synchronizer1Id.logical,
      )
    }

    "allocate a party from one of their observing nodes" in { implicit env =>
      import env.*

      val (onboardingTransactions, externalParty) = participant1.parties.testing.external
        .onboarding_transactions(
          "Bob",
          observing = Seq(participant2),
        )
        .futureValueUS
        .value
      val partyId = externalParty.partyId

      participant2.ledger_api.parties.allocate_external(
        synchronizer1Id,
        onboardingTransactions.transactionsWithSingleSignature,
        onboardingTransactions.multiTransactionSignatures,
      )

      // Use the admin API to authorize the hosting in this test, but it can also be done via the
      // allocateExternalParty endpoint on the admin API
      // See multi hosted decentralized party below for an example
      val partyToParticipantProposal = eventually() {
        participant1.topology.party_to_participant_mappings
          .list(
            synchronizer1Id,
            proposals = true,
            filterParty = partyId.toProtoPrimitive,
          )
          .loneElement
      }
      val transactionHash = partyToParticipantProposal.context.transactionHash
      participant1.topology.transactions.authorize[PartyToParticipant](
        transactionHash,
        mustBeFullyAuthorized = false,
        store = TopologyStoreId.Synchronizer(synchronizer1Id),
      )

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = externalParty.partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1, participant2),
        synchronizerId = synchronizer1Id.logical,
      )
    }

    "allocate a decentralized multi-hosted multi-sig external party" in { implicit env =>
      import env.*

      // Create the namespace owners first
      val namespace1 = participant1.parties.testing.external.create_external_namespace()
      val namespace2 = participant1.parties.testing.external.create_external_namespace()
      val namespace3 = participant1.parties.testing.external.create_external_namespace()
      val namespaceOwners = NonEmpty.mk(Set, namespace1, namespace2, namespace3)

      val confirmationThreshold = PositiveInt.two
      val keysCount = PositiveInt.three
      val keysThreshold = PositiveInt.two
      val namespaceThreshold = PositiveInt.three

      // Generate the corresponding onboarding transactions
      val onboardingData = participant1.parties.testing.external.onboarding_transactions(
        name = "Emily",
        additionalConfirming = Seq(participant2),
        observing = Seq(participant3),
        confirmationThreshold = confirmationThreshold,
        keysCount = keysCount,
        keysThreshold = keysThreshold,
        decentralizedNamespaceOwners = namespaceOwners.forgetNE,
        namespaceThreshold = namespaceThreshold,
      )

      val (onboardingTransactions, emilyE) = onboardingData.futureValueUS.value

      // Start by having the extra hosting nodes authorize the hosting
      // We can do that even before the party namespace is authorized
      Seq(participant2, participant3).map { hostingNode =>
        hostingNode.ledger_api.parties.allocate_external(
          synchronizer1Id,
          Seq(onboardingTransactions.partyToParticipant.transaction -> Seq.empty),
          multiSignatures = Seq.empty,
        )
      }

      // Then load all transactions via the allocate endpoint
      participant1.ledger_api.parties.allocate_external(
        synchronizer1Id,
        onboardingTransactions.transactionsWithSingleSignature,
        multiSignatures = onboardingTransactions.multiTransactionSignatures,
      )

      // Eventually everything should be authorized correctly
      eventually() {
        val ptp = participant1.topology.party_to_participant_mappings
          .list(filterParty = emilyE.partyId.filterString, synchronizerId = synchronizer1Id)

        ptp.loneElement.item.partyId shouldBe emilyE.partyId
        ptp.loneElement.item.threshold shouldBe confirmationThreshold
        ptp.loneElement.item.participants contains HostingParticipant(participant1, Confirmation)
        ptp.loneElement.item.participants contains HostingParticipant(participant2, Confirmation)
        ptp.loneElement.item.participants contains HostingParticipant(participant3, Observation)

        ptp.loneElement.item.partySigningKeysWithThreshold.value.threshold shouldBe keysThreshold
        ptp.loneElement.item.partySigningKeys
          .map(_.fingerprint) should contain theSameElementsAs emilyE.signingFingerprints.forgetNE
      }

      eventually() {
        val dnd = participant1.topology.decentralized_namespaces.list(
          filterNamespace = emilyE.partyId.namespace.filterString,
          store = synchronizer1Id,
        )

        dnd.loneElement.item.namespace shouldBe emilyE.partyId.uid.namespace
        dnd.loneElement.item.threshold shouldBe namespaceThreshold
        dnd.loneElement.item.owners.forgetNE shouldBe namespaceOwners.forgetNE
      }
    }

    "provide useful error message when the participant is not connected to the synchronizer" in {
      implicit env =>
        import env.*
        val (onboardingTransactions, _) =
          participant1.parties.testing.external.onboarding_transactions("Alice").futureValueUS.value

        participant1.synchronizers.disconnect_all()

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.parties.allocate_external(
            synchronizer1Id,
            onboardingTransactions.transactionsWithSingleSignature,
            onboardingTransactions.multiTransactionSignatures,
          ),
          _.errorMessage should include(
            s"This node is not connected to the requested synchronizer ${synchronizer1Id.logical}."
          ),
        )

        participant1.synchronizers.reconnect_all()
    }

    "provide useful error message when onboarding the same party twice" in { implicit env =>
      import env.*

      val (onboardingTransactions, partyE) =
        participant1.parties.testing.external.onboarding_transactions("Alice").futureValueUS.value

      def allocate() =
        participant1.ledger_api.parties.allocate_external(
          synchronizer1Id,
          onboardingTransactions.transactionsWithSingleSignature,
          onboardingTransactions.multiTransactionSignatures,
        )

      // Allocate once
      allocate()
      participant1.ledger_api.parties.list().find(_.party == partyE.partyId) shouldBe defined

      // Allocate a second time
      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        allocate(),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                ExternalPartyAlreadyExists.Failure(partyE.partyId, synchronizer1Id).cause
              ),
              "Expected party already exists error",
            )
          )
        ),
      )
    }

    "provide useful error message when concurrently retrying onboarding requests for the same party" in {
      implicit env =>
        import env.*

        val (onboardingTransactions, partyE) =
          participant1.parties.testing.external.onboarding_transactions("Alice").futureValueUS.value

        def allocate() =
          participant1.ledger_api.parties.allocate_external(
            synchronizer1Id,
            onboardingTransactions.transactionsWithSingleSignature,
            onboardingTransactions.multiTransactionSignatures,
          )

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            val results = timeouts.default.await("Waiting for concurrent allocation attempts")(
              Seq
                .fill(10)(
                  Future(allocate()).map(_ => Right(())).recover { case ex => Left(ex) }
                )
                .sequence
            )
            // Only one of them should be a success
            results.count(_.isRight) shouldBe 1
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.errorMessage should include(
                  s"Party ${partyE.partyId.toProtoPrimitive} is in the process of being allocated on this node."
                ),
                "Expected party already exists error",
              )
            ),
            // It's not impossible that one of the calls gets in late when the party is already fully allocated and
            // when no other call is in flight, so catch that case here
            Seq(
              _.errorMessage should include(
                ExternalPartyAlreadyExists.Failure(partyE.partyId, synchronizer1Id).cause
              )
            ),
          ),
        )

        // Check the party was still allocated
        participant1.ledger_api.parties.list().find(_.party == partyE.partyId) shouldBe defined
    }

    "allow concurrent onboarding of several party with same hint but different namespace" in {
      implicit env =>
        import env.*

        /*
         * This allocates a new external party on every call via
         * participant1.parties.testing.external.onboarding_transactions
         * which generates a new key pair and therefore party namespace
         */
        def allocate() = {
          val (onboardingTransactions, _) =
            participant1.parties.testing.external.onboarding_transactions("Flo").futureValueUS.value

          participant1.ledger_api.parties.allocate_external(
            synchronizer1Id,
            onboardingTransactions.transactionsWithSingleSignature,
            onboardingTransactions.multiTransactionSignatures,
          )
        }

        val results = timeouts.default.await("Waiting for concurrent allocation attempts")(
          Seq
            .fill(10)(
              Future(Try(allocate()).toEither)
            )
            .sequence
        )
        // All of them should succeeed
        results.count(_.isRight) shouldBe 10

        // Check the parties were allocated
        participant1.ledger_api.parties
          .list()
          .filter(_.party.uid.identifier.unwrap == "Flo") should have size 10
    }
  }
}
