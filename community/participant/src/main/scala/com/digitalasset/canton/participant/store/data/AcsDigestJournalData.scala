// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

object AcsDigestJournalData {
  sealed trait JournalTable extends Product with Serializable {
    def tableName: String
    def keyColumnName: String
    def digestColumnNames: List[String]
  }
  object JournalTable {
    case object PartyJournalTable extends JournalTable {
      override val tableName = "par_acs_party_running_digest"
      override val keyColumnName = "party_and_order_id"
      override val digestColumnNames: List[String] = List("digest")
    }
    case object ParticipantJournalTable extends JournalTable {
      override val tableName = "par_acs_participant_running_digest"
      override val keyColumnName = "participant_id"
      override val digestColumnNames: List[String] = List("digest", "hashed_digest")
    }
  }
}
