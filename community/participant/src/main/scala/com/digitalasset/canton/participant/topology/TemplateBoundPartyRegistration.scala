// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, HashOps}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, TopologyManager}
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TemplateBoundPartyMapping,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Orchestrates template-bound party registration:
  *   1. Optionally allocate the party on this participant (permissionless mode)
  *   2. Verify signing key exists
  *   3. Create the TemplateBoundPartyMapping
  *   4. Submit the topology transaction (signed with the party's key)
  *   5. Destroy the signing private key
  *
  * After step 5, the party's signing key is permanently unavailable.
  * The party can only act through auto-confirmation on allowed templates.
  *
  * IMPORTANT: The key is destroyed AFTER the topology transaction is accepted.
  * If submission fails, the key is preserved and the caller can retry.
  * Once the transaction is accepted and the key is destroyed, the
  * TemplateBoundPartyChecks immutability enforcement prevents any
  * subsequent modification.
  */
class TemplateBoundPartyRegistration(
    topologyManager: TopologyManager[TopologyStoreId, _],
    privateStore: CryptoPrivateStore,
    hashOps: HashOps,
    protocolVersion: ProtocolVersion,
    permissionlessTbpHosting: Boolean,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  def register(
      partyId: PartyId,
      participantId: ParticipantId,
      allowedTemplateIds: Set[String],
      signingKeyFingerprint: Fingerprint,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, TemplateBoundPartyMapping] = {
    val signingKeyHash = hashOps
      .digest(
        com.digitalasset.canton.crypto.HashPurpose.TopologyTransactionSignature,
        ByteString.copyFrom(signingKeyFingerprint.unwrap.getBytes),
      )
      .getCryptographicEvidence

    val mapping = TemplateBoundPartyMapping(
      partyId = partyId,
      hostingParticipantId = participantId,
      allowedTemplateIds = allowedTemplateIds,
      signingKeyHash = signingKeyHash,
    )

    for {
      // Step 1: Verify the signing key exists
      keyExists <- privateStore
        .existsPrivateKey(signingKeyFingerprint, com.digitalasset.canton.crypto.KeyPurpose.Signing)
        .leftMap(e => s"Failed to check signing key: $e")
      _ <- EitherT.cond[FutureUnlessShutdown](
        keyExists,
        (),
        s"Signing key $signingKeyFingerprint does not exist in the private store",
      )

      // Step 2: If permissionless hosting is enabled, ensure the party is allocated
      // on this participant. The participant co-signs because this is running on
      // its node. The party signs with its key (which will be destroyed after).
      _ <- if (permissionlessTbpHosting) {
        ensurePartyHosted(partyId, participantId, signingKeyFingerprint)
      } else {
        EitherT.rightT[FutureUnlessShutdown, String](())
      }

      // Step 3+4: Submit the topology transaction, signed with the party's key.
      _ <- topologyManager
        .proposeAndAuthorize(
          op = TopologyChangeOp.Replace,
          mapping = mapping,
          serial = Some(PositiveInt.one),
          signingKeys = Seq(signingKeyFingerprint),
          protocolVersion = protocolVersion,
          expectFullAuthorization = true,
          waitToBecomeEffective = None,
        )
        .leftMap(e => s"Failed to submit topology transaction: $e")

      _ = logger.info(
        s"Template-bound party topology transaction accepted for $partyId. " +
          s"Proceeding to destroy signing key $signingKeyFingerprint."
      )

      // Step 5: DESTROY THE KEY — point of no return.
      _ <- privateStore
        .removePrivateKey(signingKeyFingerprint)
        .leftMap(e => s"Failed to destroy signing key: $e")

      _ = logger.info(
        s"Destroyed signing key $signingKeyFingerprint for template-bound party $partyId. " +
          s"Allowed templates: ${allowedTemplateIds.mkString(", ")}. " +
          s"This party can now only act through auto-confirmation on these templates."
      )
    } yield mapping
  }

  /** Ensure the party is hosted on this participant via a PartyToParticipant mapping.
    * If the mapping already exists, this is a no-op. If not, creates one.
    * Both the party and participant sign (participant signs because this runs
    * on the participant's topology manager).
    */
  private def ensurePartyHosted(
      partyId: PartyId,
      participantId: ParticipantId,
      signingKeyFingerprint: Fingerprint,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val partyToParticipant = PartyToParticipant.tryCreate(
      partyId = partyId,
      threshold = PositiveInt.one,
      participants = Seq(
        HostingParticipant(participantId, ParticipantPermission.Confirmation)
      ),
    )

    topologyManager
      .proposeAndAuthorize(
        op = TopologyChangeOp.Replace,
        mapping = partyToParticipant,
        serial = None, // auto-determine
        signingKeys = Seq(signingKeyFingerprint), // party's key signs; participant signs via its own key automatically
        protocolVersion = protocolVersion,
        expectFullAuthorization = true,
        waitToBecomeEffective = None,
      )
      .bimap(
        e => s"Failed to allocate party on participant: $e",
        _ => (),
      )
  }
}
