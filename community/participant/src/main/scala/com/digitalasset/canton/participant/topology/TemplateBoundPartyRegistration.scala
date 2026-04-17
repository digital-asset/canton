// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.traverse.*
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
  *
  *   1. Verify signing key exists
  *   2. Verify all hosting participants have live PartyToParticipant mappings
  *      (or allocate on this participant if permissionless mode is enabled)
  *   3. Submit the TemplateBoundPartyMapping topology transaction
  *   4. Destroy the signing private key — point of no return
  *
  * Multi-participant flow:
  *   - The caller sets up PartyToParticipant on each hosting participant
  *     independently (standard Canton topology proposal/accept)
  *   - Then calls register() with all participant IDs
  *   - Registration verifies every participant is hosting before proceeding
  *   - Key is only destroyed once the TBP mapping is accepted
  *
  * Single-participant permissionless flow:
  *   - If permissionlessTbpHosting is enabled and only this participant is
  *     listed, automatically create the PartyToParticipant mapping
  */
class TemplateBoundPartyRegistration(
    localParticipantId: ParticipantId,
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
      hostingParticipantIds: Seq[ParticipantId],
      allowedTemplateIds: Set[String],
      signingKeyFingerprint: Fingerprint,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, TemplateBoundPartyMapping] = {

    require(hostingParticipantIds.nonEmpty, "At least one hosting participant is required")

    val signingKeyHash = hashOps
      .digest(
        com.digitalasset.canton.crypto.HashPurpose.TopologyTransactionSignature,
        ByteString.copyFrom(signingKeyFingerprint.unwrap.getBytes),
      )
      .getCryptographicEvidence

    val mapping = TemplateBoundPartyMapping(
      partyId = partyId,
      hostingParticipantIds = hostingParticipantIds,
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

      // Step 2: If permissionless hosting is enabled, auto-allocate on this participant.
      // Then verify all OTHER participants are already hosting.
      _ <- if (permissionlessTbpHosting && hostingParticipantIds.contains(localParticipantId)) {
        ensurePartyHosted(partyId, localParticipantId, signingKeyFingerprint)
      } else {
        EitherT.rightT[FutureUnlessShutdown, String](())
      }
      remoteParticipants = hostingParticipantIds.filterNot(_ == localParticipantId)
      _ <- if (remoteParticipants.nonEmpty || !permissionlessTbpHosting) {
        val toVerify = if (permissionlessTbpHosting) remoteParticipants else hostingParticipantIds
        verifyAllParticipantsHosting(partyId, toVerify)
      } else {
        EitherT.rightT[FutureUnlessShutdown, String](())
      }

      // Step 3: Submit the TemplateBoundPartyMapping topology transaction
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
        s"Template-bound party topology transaction accepted for $partyId " +
          s"on ${hostingParticipantIds.size} participant(s). " +
          s"Proceeding to destroy signing key $signingKeyFingerprint."
      )

      // Step 4: DESTROY THE KEY — point of no return.
      _ <- privateStore
        .removePrivateKey(signingKeyFingerprint)
        .leftMap(e => s"Failed to destroy signing key: $e")

      _ = logger.info(
        s"Destroyed signing key $signingKeyFingerprint for template-bound party $partyId. " +
          s"Hosting participants: ${hostingParticipantIds.mkString(", ")}. " +
          s"Allowed templates: ${allowedTemplateIds.mkString(", ")}."
      )
    } yield mapping
  }

  /** Verify that every listed participant has a live PartyToParticipant mapping
    * for this party. If any participant is not hosting, reject before we
    * destroy the key.
    */
  private def verifyAllParticipantsHosting(
      partyId: PartyId,
      requiredParticipants: Seq[ParticipantId],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    import com.daml.nonempty.NonEmpty
    val ptpKey = PartyToParticipant.uniqueKey(partyId)
    for {
      existingTxs <- EitherT.right[String](
        topologyManager.store
          .findTransactionsForMapping(
            com.digitalasset.canton.topology.processing.EffectiveTime.MaxValue,
            NonEmpty(Set, ptpKey),
          )
      )

      latestPtp = existingTxs
        .flatMap(_.select[TopologyChangeOp.Replace, PartyToParticipant].map(_.mapping))
        .maxByOption(_.participants.size)

      hostedParticipantIds = latestPtp.toList.flatMap(_.participants.map(_.participantId)).toSet

      missingParticipants = requiredParticipants.filterNot(hostedParticipantIds.contains)

      _ <- EitherT.cond[FutureUnlessShutdown](
        missingParticipants.isEmpty,
        (),
        s"The following participants are not yet hosting party $partyId: " +
          s"${missingParticipants.mkString(", ")}. " +
          s"Each participant must have a live PartyToParticipant mapping before " +
          s"the key can be destroyed. Set up hosting on each participant first.",
      )
    } yield ()
  }

  /** Ensure the party is hosted on this participant via a PartyToParticipant mapping.
    * Used in permissionless single-participant mode.
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
        serial = None,
        signingKeys = Seq(signingKeyFingerprint),
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
