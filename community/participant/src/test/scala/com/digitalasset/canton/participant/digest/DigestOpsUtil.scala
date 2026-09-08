// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.digest

import com.digitalasset.canton.participant.commitment.TracedLtHash16Blake3
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

object DigestOpsUtil {
  def makeExpectedDigest(
      contractId: LfContractId,
      partyPairs: Seq[(LfPartyId, LfPartyId)],
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis,
      isActivation: Boolean = true,
      enableTracing: Boolean,
  ): TracedLtHash16Blake3 =
    DigestOps.combineDigests(partyPairs.map { case (partyId1, partyId2) =>
      DigestOps.singleDigest(
        contractId = contractId,
        reassignmentCounter = reassignmentCounter,
        partyId1 = partyId1,
        partyId2 = partyId2,
        isActivation = isActivation,
        traceChanges = enableTracing,
      )
    })
}
