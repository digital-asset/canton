// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, LfWorkflowId}

import java.util.UUID

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
    workflowId: Option[LfWorkflowId],
    contractId: LfContractId,
    sourceDomain: DomainId,
    sourceProtocolVersion: SourceProtocolVersion,
    sourceMediator: MediatorId,
    targetDomain: DomainId,
    targetProtocolVersion: TargetProtocolVersion,
    targetTimeProof: TimeProof,
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
        sourceProtocolVersion,
      )
    val view = TransferOutView.create(hashOps)(
      viewSalt,
      submitterMetadata,
      workflowId,
      contractId,
      targetDomain,
      targetTimeProof,
      sourceProtocolVersion,
      targetProtocolVersion,
    )
    val tree = TransferOutViewTree(commonData, view, sourceProtocolVersion.v, hashOps)
    FullTransferOutTree(tree)
  }
}
