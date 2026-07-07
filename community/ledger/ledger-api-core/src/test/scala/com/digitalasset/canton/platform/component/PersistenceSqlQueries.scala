// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import anorm.SqlParser.long
import anorm.{SqlStringInterpolation, ~}

trait PersistenceSqlQueries {
  self: IndexComponentTest =>

  protected def getAchsEventSeqIds: List[Long] =
    withConnection { implicit connection =>
      SQL"""SELECT event_sequential_id
          FROM lapi_filter_achs_stakeholder
          ORDER BY event_sequential_id"""
        .as(long("event_sequential_id").*)
    }

  protected def getActivateEventSeqIds: List[Long] =
    withConnection { implicit connection =>
      SQL"""SELECT event_sequential_id
          FROM lapi_filter_activate_stakeholder
          ORDER BY event_sequential_id"""
        .as(long("event_sequential_id").*)
    }

  protected def offsetForActivateContractEventSeqId(seqId: Long): Long =
    withConnection { implicit connection =>
      SQL"""SELECT MAX(event_offset) AS max_offset
              FROM lapi_events_activate_contract
              WHERE event_sequential_id <= $seqId"""
        .as(long("max_offset").?.single)
        .getOrElse(0L)
    }

  protected def eventSeqIdForActivateContractOffset(offset: Long): Long =
    withConnection { implicit connection =>
      SQL"""SELECT MAX(event_sequential_id) AS max_seq_id
            FROM lapi_events_activate_contract
            WHERE event_offset <= $offset"""
        .as(long("max_seq_id").?.single)
        .getOrElse(0L)
    }

  protected def getLastEventSeqId: Long =
    withConnection { implicit connection =>
      SQL"SELECT ledger_end_sequential_id FROM lapi_parameters"
        .as(long("ledger_end_sequential_id").?.single)
        .getOrElse(0L)
    }

  protected def getAchsState: (Long, Long, Long) =
    withConnection { implicit connection =>
      SQL"SELECT valid_at, last_populated, last_removed FROM lapi_achs_state"
        .as((long("valid_at") ~ long("last_populated") ~ long("last_removed")).single)(
          connection
        ) match {
        case v ~ lp ~ lr => (v, lp, lr)
      }
    }

  protected def getAchsValidAt: Long = getAchsState._1

  protected def getAchsSize: Long =
    withConnection { implicit connection =>
      SQL"SELECT COUNT(DISTINCT event_sequential_id) AS count FROM lapi_filter_achs_stakeholder"
        .as(long("count").single)
    }

  protected def getAchsStateRowCount: Long =
    withConnection { implicit connection =>
      SQL"SELECT COUNT(*) AS count FROM lapi_achs_state"
        .as(long("count").single)
    }
}
