// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data._
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.version.SourceProtocolVersion

import java.util.UUID

/** Request to transfer a contract away from a domain.
  *
  * @param adminParties admin parties of participants that (a) host a stakeholder of the contract and
  *                     (b) are connected to both source and target domain
  * @param targetTimeProof a sequenced event that the submitter has recently observed on the target domain.
  *                        Determines the timestamp of the topology at the target domain.
  */
case class TransferOutRequest(
    submitter: LfPartyId,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    contractId: LfContractId,
    originDomain: DomainId,
    sourceProtocolVersion: SourceProtocolVersion,
    originMediator: MediatorId,
    targetDomain: DomainId,
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
        originDomain,
        originMediator,
        stakeholders,
        adminParties,
        uuid,
        sourceProtocolVersion,
      )
    val view = TransferOutView.create(hashOps)(
      viewSalt,
      submitter,
      contractId,
      targetDomain,
      targetTimeProof,
      sourceProtocolVersion.v,
    )
    val tree = TransferOutViewTree(commonData, view)(sourceProtocolVersion.v, hashOps)
    FullTransferOutTree(tree)
  }
}
