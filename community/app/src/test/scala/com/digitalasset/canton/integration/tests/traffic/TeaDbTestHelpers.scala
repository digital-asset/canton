// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.tea.projection.{ActAsCategory, UnchargedReason}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.daml.lf.data.Ref

trait TeaDbTestHelpers { this: BaseTest =>

  protected def dbStorage(participant: LocalParticipantReference): DbStorage =
    participant.underlying.value.storage match {
      case dbStorage: DbStorage => dbStorage
      case other => fail(s"expected DbStorage, got $other")
    }

  protected def changeIdHash(commandId: String, actAs: Set[PartyId]): String =
    ChangeId(
      Ref.UserId.assertFromString(LedgerApiCommands.defaultUserId),
      Ref.CommandId.assertFromString(commandId),
      actAs.map(_.toLf),
    ).hash.toHexString

  /** `(submission_id, account_id)` of the correlations stored for a change id. */
  protected def correlationsFromDb(
      participant: LocalParticipantReference,
      changeIdHash: String,
  ): Seq[(String, Option[String])] = {
    val storage = dbStorage(participant)
    import storage.api.*
    implicit val closeContext: CloseContext = CloseContext(storage)
    storage
      .query(
        sql"""select submission_id, account_id
              from par_traffic_enforcement_correlations
              where change_id_hash = $changeIdHash"""
          .as[(String, Option[String])],
        "read tea correlations",
      )
      .futureValueUS
  }

  protected def insertCorrelationIntoDb(
      participant: LocalParticipantReference,
      changeIdHash: String,
      submissionId: String,
      accountId: Option[String],
  ): Unit = {
    val storage = dbStorage(participant)
    import storage.api.*
    implicit val closeContext: CloseContext = CloseContext(storage)
    storage
      .update_(
        sqlu"""insert into par_traffic_enforcement_correlations
               (change_id_hash, submission_id, account_id, timestamp)
               values ($changeIdHash, $submissionId, $accountId, ${CantonTimestamp.Epoch})""",
        "insert tea correlation",
      )
      .futureValueUS
  }

  /** `(submission_id, paid_traffic_cost, reason, act_as_category)` of the uncharged completions
    * stored for a change id, ordered by offset.
    */
  protected def unchargedFromDb(
      participant: LocalParticipantReference,
      changeIdHash: String,
  ): Seq[(String, Long, UnchargedReason, ActAsCategory)] = {
    val storage = dbStorage(participant)
    import storage.api.*
    implicit val closeContext: CloseContext = CloseContext(storage)
    storage
      .query(
        sql"""select submission_id, paid_traffic_cost, reason, act_as_category
              from par_traffic_enforcement_uncharged_completions
              where change_id_hash = $changeIdHash
              order by completion_offset"""
          .as[(String, Long, UnchargedReason, ActAsCategory)],
        "read tea uncharged completions",
      )
      .futureValueUS
  }
}
