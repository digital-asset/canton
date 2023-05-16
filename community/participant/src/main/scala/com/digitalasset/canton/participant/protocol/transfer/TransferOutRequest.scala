// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.CanSubmit
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.protocol.{LfContractId, LfTemplateId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, TransferCounter}

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Request to transfer a contract away from a domain.
  *
  * @param adminParties admin parties of participants that (a) host a stakeholder of the contract and
  *                     (b) are connected to both source and target domain
  * @param targetTimeProof a sequenced event that the submitter has recently observed on the target domain.
  *                        Determines the timestamp of the topology at the target domain.
  */
final case class TransferOutRequest(
    submitterMetadata: TransferSubmitterMetadata,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    contractId: LfContractId,
    templateId: LfTemplateId,
    sourceDomain: SourceDomainId,
    sourceProtocolVersion: SourceProtocolVersion,
    sourceMediator: MediatorId,
    targetDomain: TargetDomainId,
    targetProtocolVersion: TargetProtocolVersion,
    targetTimeProof: TimeProof,
    transferCounter: TransferCounter,
) {

  def toFullTransferOutTree(
      hashOps: HashOps,
      hmacOps: HmacOps,
      seed: SaltSeed,
      uuid: UUID,
  ): FullTransferOutTree = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, hmacOps)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, hmacOps)
    val commonData =
      TransferOutCommonData.create(hashOps)(
        commonDataSalt,
        sourceDomain,
        sourceMediator,
        stakeholders,
        adminParties,
        uuid,
        transferCounter,
        sourceProtocolVersion,
      )
    val view = TransferOutView.create(hashOps)(
      viewSalt,
      submitterMetadata,
      contractId,
      templateId,
      targetDomain,
      targetTimeProof,
      sourceProtocolVersion,
      targetProtocolVersion,
    )
    val tree = TransferOutViewTree(commonData, view, sourceProtocolVersion.v, hashOps)
    FullTransferOutTree(tree)
  }
}

object TransferOutRequest {

  def validated(
      participantId: ParticipantId,
      timeProof: TimeProof,
      contractId: LfContractId,
      templateId: LfTemplateId,
      submitterMetadata: TransferSubmitterMetadata,
      stakeholders: Set[LfPartyId],
      sourceDomain: SourceDomainId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceMediator: MediatorId,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
      sourceIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      transferCounter: TransferCounter,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    TransferOutRequestValidated,
  ] =
    for {
      _ <- CanSubmit(
        contractId,
        sourceIps,
        submitterMetadata.submitter,
        participantId,
      )
      adminPartiesAndRecipients <- AdminPartiesAndParticipants(
        contractId,
        submitterMetadata.submitter,
        stakeholders,
        sourceIps,
        targetIps,
        logger,
      )
    } yield {
      val transferOutRequest = TransferOutRequest(
        submitterMetadata,
        stakeholders,
        adminPartiesAndRecipients.adminParties,
        contractId,
        templateId,
        sourceDomain,
        sourceProtocolVersion,
        sourceMediator,
        targetDomain,
        targetProtocolVersion,
        timeProof,
        transferCounter,
      )

      TransferOutRequestValidated(transferOutRequest, adminPartiesAndRecipients.participants)
    }

}
