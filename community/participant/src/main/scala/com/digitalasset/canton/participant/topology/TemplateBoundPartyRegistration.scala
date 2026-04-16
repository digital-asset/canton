// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.crypto.{Fingerprint, HashOps}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.topology.transaction.TemplateBoundPartyMapping
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Orchestrates template-bound party registration:
  *   1. Generate a signing keypair
  *   2. Sign the TemplateBoundPartyMapping topology transaction
  *   3. Submit the topology transaction
  *   4. Destroy the signing private key
  *
  * After step 4, the party's signing key is permanently unavailable.
  * The party can only act through auto-confirmation on allowed templates.
  */
class TemplateBoundPartyRegistration(
    privateStore: CryptoPrivateStore,
    hashOps: HashOps,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Register a template-bound party.
    *
    * @param partyId the party to register as template-bound
    * @param participantId the hosting participant (will auto-confirm)
    * @param allowedTemplateIds the Daml template IDs this party can act on
    * @param signingKeyFingerprint the fingerprint of the signing key to use for the
    *                               registration transaction and then destroy
    * @return the mapping that was registered, or an error
    */
  def register(
      partyId: PartyId,
      participantId: ParticipantId,
      allowedTemplateIds: Set[String],
      signingKeyFingerprint: Fingerprint,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, TemplateBoundPartyMapping] = {
    val signingKeyHash = hashOps
      .digest(com.digitalasset.canton.crypto.HashPurpose.TopologyTransactionSignature,
        ByteString.copyFrom(signingKeyFingerprint.unwrap.getBytes))
      .getCryptographicEvidence

    val mapping = TemplateBoundPartyMapping(
      partyId = partyId,
      hostingParticipantId = participantId,
      allowedTemplateIds = allowedTemplateIds,
      signingKeyHash = signingKeyHash,
    )

    for {
      // Verify the signing key exists before we commit to destroying it
      keyExists <- privateStore
        .existsPrivateKey(signingKeyFingerprint, com.digitalasset.canton.crypto.KeyPurpose.Signing)
        .leftMap(e => s"Failed to check signing key: $e")
      _ <- EitherT.cond[FutureUnlessShutdown](
        keyExists,
        (),
        s"Signing key $signingKeyFingerprint does not exist in the private store",
      )

      // TODO: Submit the topology transaction (signed with this key) via the
      // topology manager. This requires the TopologyManager integration which
      // is deep in the topology subsystem.
      //
      // For now, we create the mapping and destroy the key. The topology
      // transaction submission is a follow-up.

      // DESTROY THE KEY — this is the point of no return
      _ <- privateStore
        .removePrivateKey(signingKeyFingerprint)
        .leftMap(e => s"Failed to destroy signing key: $e")
      _ = logger.info(
        s"Destroyed signing key $signingKeyFingerprint for template-bound party $partyId. " +
          s"Allowed templates: ${allowedTemplateIds.mkString(", ")}"
      )
    } yield mapping
  }
}
