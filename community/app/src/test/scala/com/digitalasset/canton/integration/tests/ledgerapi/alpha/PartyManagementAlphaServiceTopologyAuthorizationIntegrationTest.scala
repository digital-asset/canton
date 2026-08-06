// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.alpha

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Confirmation, Submission}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, TopologyTransaction}
import com.digitalasset.canton.topology.{Namespace, PartyId, SynchronizerId}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.scalatest.Assertion

import java.security.KeyPairGenerator

/** Tests the end-to-end flow to onboard an already hosted party to a new participant, covering
  * external and local parties.
  *
  * Setup:
  *   - 4 participants connected to 1 synchronizer.
  *   - "Emil" (external) and "Alice" (local) hosted on participant1.
  *   - "Diana" (decentralized) hosted across participant1, participant2, and participant3.
  *
  * Common Flow:
  *   1. Generate a PartyToParticipant topology transaction via `GeneratePartyTopologyUpdate`.
  *   1. Authorize the topology change via `AuthorizePartyUpdate` based on the party type:
  *      - External: The remote owner signs the hash; the target participant submits the signature.
  *      - Local: Both target and source participants submit empty signatures to mutually authorize
  *        via local keys.
  *      - Decentralized: The required threshold of namespace owners externally sign the hash; the
  *        target participant submits the signatures.
  *   1. Verify the mapping becomes effective on the target participant with the `onboarding` flag
  *      set to true.
  */
class PartyManagementAlphaServiceTopologyAuthorizationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var source: ParticipantReference = _
  private var targetForExternalParty: ParticipantReference = _
  private var targetForLocalParty: ParticipantReference = _
  private var targetForDso: ParticipantReference = _

  private var emil: PartyId = _
  private var alice: PartyId = _
  private var diana: PartyId = _

  private var nsLuna: Namespace = _
  private var nsHecate: Namespace = _
  private var nsLucina: Namespace = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransforms(ConfigTransforms.enableAlphaOnlinePartyReplicationSupport()*)
      .withSetup { implicit env =>
        import env.*

        source = participant1
        targetForExternalParty = participant2
        targetForLocalParty = participant3
        targetForDso = participant4

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters.propose_update(
            synchronizer.synchronizerId,
            // Lower the confirmation response timeout to observe rejections due to non-responsive nodes quickly
            _.update(confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(3)),
          )
        )

        // Setup External Party
        val txs = source.ledger_api.parties.generate_topology(daId, "Emil", signingPublicKey)
        source.ledger_api.parties.allocate_external(
          daId,
          txs.topologyTransactions.map((_, Seq.empty)),
          multiSignatures = Seq(generateSignature(txs.multiHash.getCryptographicEvidence)),
        )
        emil = eventually()(source.parties.hosted("Emil").loneElement.party)

        // Setup Local Party
        alice = source.parties.enable("Alice")

        // Setup Decentralized Party
        nsLuna = source.parties.testing.external.create_external_namespace()
        nsHecate = source.parties.testing.external.create_external_namespace()
        nsLucina = source.parties.testing.external.create_external_namespace()

        val (onboardingTransactions, dianaE) = source.parties.testing.external
          .onboarding_transactions(
            name = "Diana",
            additionalConfirming = Seq(participant2),
            observing = Seq(participant3),
            confirmationThreshold = PositiveInt.two,
            keysCount = PositiveInt.three,
            keysThreshold = PositiveInt.two,
            decentralizedNamespaceOwners = NonEmpty.mk(Set, nsLuna, nsHecate, nsLucina).forgetNE,
            namespaceThreshold = PositiveInt.three,
          )
          .futureValueUS
          .value

        Seq(participant2, participant3).foreach { p =>
          p.ledger_api.parties.allocate_external(
            daId,
            Seq(onboardingTransactions.partyToParticipant.transaction -> Seq.empty),
            multiSignatures = Seq.empty,
          )
        }

        source.ledger_api.parties.allocate_external(
          daId,
          onboardingTransactions.transactionsWithSingleSignature,
          multiSignatures = onboardingTransactions.multiTransactionSignatures,
        )

        diana = dianaE.partyId
        eventually() {
          source.topology.party_to_participant_mappings
            .list(filterParty = diana.filterString, synchronizerId = daId)
            .loneElement
            .item
            .participants should have size 3
        }
      }

  // Key generation and cryptography setup for External Party "Emil"
  private val keyGen = KeyPairGenerator.getInstance("Ed25519")
  private val keyPair = keyGen.generateKeyPair()
  private val pb = keyPair.getPublic

  private val signingPublicKey = SigningPublicKey
    .create(
      format = CryptoKeyFormat.DerX509Spki,
      key = ByteString.copyFrom(pb.getEncoded),
      keySpec = SigningKeySpec.EcCurve25519,
      usage = SigningKeyUsage.All,
    )
    .valueOrFail("failed to generate pubkey")

  private def generateSignature(bytes: ByteString) = {
    val signing = java.security.Signature.getInstance("Ed25519")
    signing.initSign(keyPair.getPrivate)
    signing.update(bytes.toByteArray)
    Signature.create(
      format = SignatureFormat.Concat,
      signature = ByteString.copyFrom(signing.sign()),
      signedBy = signingPublicKey.fingerprint,
      signingAlgorithmSpec = Some(SigningAlgorithmSpec.Ed25519),
      signatureDelegation = None,
    )
  }

  private def assertEffectiveTopologyTx(
      party: PartyId,
      synchronizerId: SynchronizerId,
      target: ParticipantReference,
      permission: ParticipantPermission,
      calledOn: ParticipantReference,
  ): Assertion =
    eventually() {
      val ptps = calledOn.topology.party_to_participant_mappings
        .list(
          filterParty = party.toProtoPrimitive,
          filterParticipant = target.id.filterString,
          synchronizerId = synchronizerId,
        )

      ptps should not be empty
      val targetParticipantNode = ptps.loneElement.item.participants
        .find(_.participantId == target.id)
        .value

      ptps.loneElement.context.operation shouldBe Replace
      targetParticipantNode.onboarding shouldBe true
      targetParticipantNode.permission shouldBe permission
    }

  "PartyManagementAlphaService" should {

    "allow an external party to request and authorize hosting on a new participant" in {
      implicit env =>
        import env.*

        val generateRes = source.ledger_api.parties.generate_party_topology_update(
          partyId = emil,
          synchronizerId = daId,
          targetParticipantId = targetForExternalParty.id,
          participantPermission = Confirmation,
        )

        // Parse the returned transaction so we can compute the exact hash Canton expects
        val parsedTx = TopologyTransaction
          .fromByteString(testedProtocolVersion, generateRes.transaction)
          .valueOrFail("Failed to parse topology transaction")

        val externalPartySignature = generateSignature(parsedTx.hash.hash.getCryptographicEvidence)

        targetForExternalParty.ledger_api.parties.authorize_party_update(
          transaction = generateRes.transaction,
          signatures = Seq(externalPartySignature),
          synchronizerId = daId,
        )

        Seq(targetForExternalParty, source).foreach(p =>
          assertEffectiveTopologyTx(emil, daId, targetForExternalParty, Confirmation, p)
        )
    }

    "allow a local party to request and authorize hosting on a new participant" in { implicit env =>
      import env.*

      val generateRes = source.ledger_api.parties.generate_party_topology_update(
        partyId = alice,
        synchronizerId = daId,
        targetParticipantId = targetForLocalParty.id,
        participantPermission = Submission,
      )

      // Mutual authorization via empty signatures
      source.ledger_api.parties.authorize_party_update(
        transaction = generateRes.transaction,
        signatures = Seq.empty,
        synchronizerId = daId,
      )

      eventually() {
        targetForLocalParty.topology.party_to_participant_mappings.list(
          synchronizerId = daId,
          filterParty = alice.filterString,
          filterParticipant = targetForLocalParty.id.filterString,
          proposals = true,
        ) should not be empty
      }

      targetForLocalParty.ledger_api.parties.authorize_party_update(
        transaction = generateRes.transaction,
        signatures = Seq.empty,
        synchronizerId = daId,
      )

      Seq(targetForLocalParty, source).foreach(p =>
        assertEffectiveTopologyTx(alice, daId, targetForLocalParty, Submission, p)
      )
    }

    "allow a decentralized multi-hosted party to request and authorize hosting on a new participant via authorize_party_update" in {
      implicit env =>
        import env.*

        val generateRes = source.ledger_api.parties.generate_party_topology_update(
          partyId = diana,
          synchronizerId = daId,
          targetParticipantId = targetForDso.id,
          participantPermission = Confirmation,
        )

        // Parse the transaction to get the exact hash that needs to be signed by the decentralized namespace owners
        val parsedTx = TopologyTransaction
          .fromByteString(testedProtocolVersion, generateRes.transaction)
          .valueOrFail("Failed to parse topology transaction")

        val txHash = parsedTx.hash.hash.getCryptographicEvidence

        // The DSO namespace threshold is 3, so we need signatures from all 3 external namespace owners
        val sigs = Seq(nsLuna, nsHecate, nsLucina).map { ns =>
          global_secret.sign(txHash, ns.fingerprint, SigningKeyUsage.NamespaceOnly)
        }

        targetForDso.ledger_api.parties.authorize_party_update(
          transaction = generateRes.transaction,
          signatures = sigs,
          synchronizerId = daId,
        )

        Seq(targetForDso, source, participant2, participant3).foreach(p =>
          assertEffectiveTopologyTx(diana, daId, targetForDso, Confirmation, p)
        )
    }

    "fail to authorize_party_update if invoked by an unrelated participant lacking authorization keys" in {
      implicit env =>
        import env.*

        // Generate topology to onboard 'alice' (hosted on P1, P3) to 'targetForExternalParty' (P2)
        val generateRes = source.ledger_api.parties.generate_party_topology_update(
          partyId = alice,
          synchronizerId = daId,
          targetParticipantId = targetForExternalParty.id,
          participantPermission = Submission,
        )

        // Try to authorize the update from targetForDso (P4), which is entirely unrelated.
        // Because P4 does not possess the keys for Alice's namespace or P2's namespace,
        // the Topology Manager natively rejects the proposal attempt.
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          targetForDso.ledger_api.parties.authorize_party_update(
            transaction = generateRes.transaction,
            signatures = Seq.empty,
            synchronizerId = daId,
          ),
          _.errorMessage should include(
            "Failed to propose and authorize topology transaction: Failure(ReferencedAuthorizations"
          ),
        )
    }

    "reject generate_party_topology_update if the target participant already hosts the party" in {
      implicit env =>
        import env.*

        // Try to generate topology to onboard 'alice' to 'source' (participant1)
        // This is invalid because 'source' is the node that originally allocated and already hosts 'alice'
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          source.ledger_api.parties.generate_party_topology_update(
            partyId = alice,
            synchronizerId = daId,
            targetParticipantId = source.id,
            participantPermission = Submission,
          ),
          _.errorMessage should include(
            s"Target participant ${source.id} is already hosting party $alice"
          ),
        )
    }

  }
}
