// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, HashOps}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, TopologyManager, TopologyManagerError}
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{TemplateBoundPartyMapping, TopologyChangeOp}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Orchestrates template-bound party registration:
  *   1. Verify signing key exists
  *   2. Create the TemplateBoundPartyMapping
  *   3. Submit the topology transaction (signed with the party's key)
  *   4. Destroy the signing private key
  *
  * After step 4, the party's signing key is permanently unavailable.
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

      // Step 2+3: Submit the topology transaction, signed with the party's key.
      // This is the last use of the signing key. The topology manager builds the
      // transaction, signs it with the specified key, and submits it to the store.
      _ <- topologyManager
        .proposeAndAuthorize(
          op = TopologyChangeOp.Replace,
          mapping = mapping,
          serial = Some(PositiveInt.one), // first (and only) version
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

      // Step 4: DESTROY THE KEY — point of no return.
      // The topology transaction is accepted. The TemplateBoundPartyChecks
      // immutability enforcement prevents any future modification.
      // Destroying the key makes this permanent and auditable.
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
}
