// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.{LfContractId, LfHash, ReassignmentId}
import com.digitalasset.canton.resource.ToDbPrimitive
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.{LedgerCommandId, LedgerUserId, LfPartyId}
import com.digitalasset.daml.lf.data.Bytes
import slick.jdbc.GetResult

final case class ChangeIdHash(hash: LfHash) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[ChangeIdHash] = ChangeIdHash
}

object ChangeIdHash extends PrettyPrintingCompanion[ChangeIdHash] {
  def apply(changeId: ChangeId): ChangeIdHash = ChangeIdHash(changeId.hash)

  def routedUnassignment(
      userId: LedgerUserId,
      commandId: LedgerCommandId,
      submitter: LfPartyId,
      contractIds: Set[LfContractId],
  ): ChangeIdHash =
    ChangeIdHash(LfHash.hashReassignmentChangeId(userId, commandId, submitter, contractIds))

  def ledgerApiUnassignment(
      userId: LedgerUserId,
      commandId: LedgerCommandId,
      submitter: LfPartyId,
  ): ChangeIdHash =
    ChangeIdHash(LfHash.hashLedgerApiReassignmentChangeId(userId, commandId, submitter))

  def assignment(reassignmentId: ReassignmentId): ChangeIdHash =
    ChangeIdHash(LfHash.hashAssignmentReassignmentId(Bytes.fromByteString(reassignmentId.toBytes)))

  implicit val getResultChangeIdHash: GetResult[ChangeIdHash] = { r =>
    val hex = r.nextString()

    ChangeIdHash(
      LfHash
        .fromString(hex)
        .getOrElse(throw new DbDeserializationException(s"Cannot parse change ID hash $hex"))
    )
  }

  implicit val changeIdToDbPrimitive: ToDbPrimitive[ChangeIdHash, String] =
    ToDbPrimitive(_.hash.toHexString)

  override protected val pretty: Pretty[ChangeIdHash] = prettyOfClass(
    unnamedParam(_.hash)
  )
}
