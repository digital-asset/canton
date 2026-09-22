// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.implicits.toTraverseOps
import com.digitalasset.canton.data.ContractReassignment
import com.digitalasset.canton.participant.admin.data.{ActiveContract, RepairContract}
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.nonempty.NonEmpty

private[party] object ReceivedContractValidation {
  def validateContracts(
      contracts: NonEmpty[Seq[ActiveContract]],
      synchronizerId: SynchronizerId,
  ): Either[String, NonEmpty[Seq[ContractReassignment]]] =
    contracts.toNEF
      .traverse(activeContract =>
        for {
          repairContract <- RepairContract.fromLapiActiveContract(activeContract.contract)
          _ <- Either.cond(
            repairContract.synchronizerId == synchronizerId,
            (),
            s"Received contract ${repairContract.contractId} has unexpected synchronizer ${repairContract.synchronizerId}",
          )
          contractInstance <- ContractInstance.create(repairContract.contract)

        } yield {
          // TODO(#26468): Use representative package
          ContractReassignment(
            contractInstance,
            ReassignmentTag.Source(contractInstance.templateId.packageId),
            ReassignmentTag.Target(contractInstance.templateId.packageId),
            repairContract.reassignmentCounter,
          )
        }
      )
}
