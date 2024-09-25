// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Oracle, Postgres}
import com.digitalasset.canton.topology.Member
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext

/** This file contains the legacy implementations of checkpointing in the DbSequencerStore to reduce the clutter there.
  * These are called from the new DbSequencerStore to provide backwards compatibility.
  */
object DbSequencerStoreLegacy {

  import com.digitalasset.canton.topology.Member.DbStorageImplicits.*

  private[store] def oldMemberCheckpointsQuery(
      storage: DbStorage,
      beforeInclusive: CantonTimestamp,
      safeWatermark: CantonTimestamp,
  )(implicit
      executionContext: ExecutionContext
  ): DBIOAction[Map[Member, CounterCheckpoint], NoStream, Effect.Read] = {
    import storage.api.*
    val (memberContainsBefore, memberContainsAfter): (String, String) = storage.profile match {
      case _: Postgres =>
        ("", " = any(events.recipients)")
      case _: H2 =>
        ("array_contains(events.recipients, ", ")")
      case _: Oracle => sys.error("Oracle no longer supported")
    }
    // this query returns checkpoints for all registered enabled members at the given timestamp
    // it will produce checkpoints at exactly the `beforeInclusive` timestamp by assuming that the checkpoint's
    // `timestamp` doesn't need to be exact as long as it's a valid lower bound for a given (member, counter).
    // it does this by taking existing events and checkpoints before or at the given timestamp in order to compute
    // the equivalent latest checkpoint for each member at or before this timestamp.
    val query = storage.profile match {
      case _: Postgres =>
        sql"""
          -- the max counter for each member will be either the number of events -1 (because the index is 0 based)
          -- or the checkpoint counter + number of events after that checkpoint
          -- the timestamp for a member will be the maximum between the highest event timestamp and the checkpoint timestamp (if it exists)
          select sequencer_members.member, checkpoints.counter + count(events.ts), $beforeInclusive, checkpoints.latest_sequencer_event_ts
          from sequencer_members
          left join (
              -- if the member has checkpoints, let's find the latest one that's still before or at the given timestamp.
              -- using checkpoints is essential for cases where the db has been pruned and for performance (to avoid scanning complete events table)
              -- the very negative number is CantonTimestamp.MinValue, it helps postgres to use the index on ts field (instead of IS NOT NULL check)
            select m.id member, coalesce(cc.counter, -1) as counter, coalesce(cc.ts, -62135596800000000) as ts, cc.latest_sequencer_event_ts
              from sequencer_members m
                left join lateral (
                        select *
                        from sequencer_counter_checkpoints
                        where member = m.id and ts <= $beforeInclusive
                        order by member, ts desc
                        limit 1
                    ) cc
                    on true
          ) as checkpoints on checkpoints.member = sequencer_members.id
          left join sequencer_events as events
            on ((sequencer_members.id = any(events.recipients))
                    -- we just want the events between the checkpoint and the requested timestamp
                    -- and within the safe watermark
                    and events.ts <= $beforeInclusive and events.ts <= $safeWatermark
                    -- start from closest checkpoint the checkpoint is defined, we only want events past it
                    and events.ts > checkpoints.ts
                    -- start from member's registration date
                    and events.ts >= sequencer_members.registered_ts)
          left join sequencer_watermarks watermarks
            on (events.node_index is not null) and events.node_index = watermarks.node_index
          where (
              -- no need to consider disabled members since they can't be served events anymore
              sequencer_members.enabled = true
              -- consider the given timestamp
              and sequencer_members.registered_ts <= $beforeInclusive
              and ((events.ts is null) or (
                  -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                   watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                  ))
            )
          group by (sequencer_members.member, checkpoints.counter, checkpoints.ts, checkpoints.latest_sequencer_event_ts)
           """
      case _ =>
        sql"""
          -- the max counter for each member will be either the number of events -1 (because the index is 0 based)
          -- or the checkpoint counter + number of events after that checkpoint
          -- the timestamp for a member will be the maximum between the highest event timestamp and the checkpoint timestamp (if it exists)
          select sequencer_members.member, coalesce(checkpoints.counter, - 1) + count(events.ts), $beforeInclusive, checkpoints.latest_sequencer_event_ts
          from sequencer_members
          left join (
              -- if the member has checkpoints, let's find the one latest one that's still before or at the given timestamp.
              -- using checkpoints is essential for cases where the db has been pruned
              select member, max(counter) as counter, max(ts) as ts, max(latest_sequencer_event_ts) as latest_sequencer_event_ts
              from sequencer_counter_checkpoints
              where ts <= $beforeInclusive
              group by member
          ) as checkpoints on checkpoints.member = sequencer_members.id
          left join sequencer_events as events
            on ((#$memberContainsBefore sequencer_members.id #$memberContainsAfter)
                    -- we just want the events between the checkpoint and the requested timestamp
                    -- and within the safe watermark
                    and events.ts <= $beforeInclusive and events.ts <= $safeWatermark
                    -- if the checkpoint is defined, we only want events past it
                    and ((checkpoints.ts is null) or (checkpoints.ts < events.ts))
                    -- start from member's registration date
                    and events.ts >= sequencer_members.registered_ts)
          left join sequencer_watermarks watermarks
            on (events.node_index is not null) and events.node_index = watermarks.node_index
          where (
              -- no need to consider disabled members since they can't be served events anymore
              sequencer_members.enabled = true
              -- consider the given timestamp
              and sequencer_members.registered_ts <= $beforeInclusive
              and ((events.ts is null) or (
                  -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                   watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                  ))
            )
          group by (sequencer_members.member, checkpoints.counter, checkpoints.ts, checkpoints.latest_sequencer_event_ts)
          """
    }
    query.as[(Member, CounterCheckpoint)].map(_.toMap)
  }

  private[store] def oldMemberLatestSequencerTimestampQuery(
      storage: DbStorage,
      timestamp: CantonTimestamp,
      safeWatermark: CantonTimestamp,
      sequencerId: SequencerMemberId,
  )(implicit
      executionContext: ExecutionContext
  ): DBIOAction[Map[Member, Option[CantonTimestamp]], NoStream, Effect.Read] = {
    import storage.api.*
    val (memberContainsBefore, memberContainsAfter): (String, String) = storage.profile match {
      case _: Postgres =>
        ("", " = any(events.recipients)")
      case _: H2 =>
        ("array_contains(events.recipients, ", ")")
      case _: Oracle => sys.error("Oracle no longer supported")
    }

    // in order to compute the latest sequencer event for each member at a timestamp, we find the latest event ts
    // for an event addressed both to the sequencer and that member
    val query = storage.profile match {
      case _: Postgres =>
        sql"""
          -- for each member we scan the sequencer_events table
          -- bounded above by the requested `timestamp`, watermark, registration date, etc...
          -- bounded below by an existing sequencer counter (or by beginning of time)
          -- this is crucial to avoid scanning the whole table and using the index on `ts` field
          select sequencer_members.member, coalesce(max(events.ts), checkpoints.latest_sequencer_event_ts)
          from sequencer_members
          left join (
              -- if the member has checkpoints, let's find the latest one that's still before or at the given timestamp.
              -- using checkpoints is essential for cases where the db has been pruned
              -- the very negative number is CantonTimestamp.MinValue
            select
                m.id member,
                coalesce(cc.ts, -62135596800000000) as ts,
                cc.latest_sequencer_event_ts,
                0 as predicate_pushdown_barrier
              from sequencer_members m
                left join lateral (
                        select *
                        from sequencer_counter_checkpoints
                        where member = m.id and ts <= $timestamp
                        order by member, ts desc
                        limit 1
                    ) cc
                    on true
          ) as checkpoints on checkpoints.member = sequencer_members.id
          left join sequencer_events as events
            on ((sequencer_members.id = any(events.recipients)) -- member is in recipients
                  -- this sequencer itself is in recipients
                  -- NOTE: we add predicate_pushdown_barrier (="0") here to prevent the query planner quirk,
                  -- so that Postgres doesn't push down the predicate (and revert to Seq scan on events table)
                  and ((checkpoints.predicate_pushdown_barrier + $sequencerId) = any(events.recipients))
                  -- we just want the events between the checkpoint and the requested timestamp
                  -- and within the safe watermark
                  and events.ts <= $timestamp and events.ts <= $safeWatermark
                  -- start from closest checkpoint, we only want events past it
                  and events.ts > checkpoints.ts
                  -- start from member's registration date
                  and events.ts >= sequencer_members.registered_ts)
          left join sequencer_watermarks watermarks
            on (events.node_index is not null) and events.node_index = watermarks.node_index
          where (
              -- no need to consider disabled members since they can't be served events anymore
              sequencer_members.enabled = true
              -- consider the given timestamp
              and sequencer_members.registered_ts <= $timestamp
              and ((events.ts is null) or (
                  -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                   watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                  ))
            )
          group by (sequencer_members.member, checkpoints.latest_sequencer_event_ts)
           """
      case _ =>
        sql"""
          select sequencer_members.member, max(events.ts)
          from sequencer_members
          left join sequencer_events as events
            on ((#$memberContainsBefore sequencer_members.id #$memberContainsAfter)
                    and (#$memberContainsBefore $sequencerId #$memberContainsAfter)
                    and events.ts <= $timestamp and events.ts <= $safeWatermark
                    -- start from member's registration date
                    and events.ts >= sequencer_members.registered_ts)
          left join sequencer_watermarks watermarks
            on (events.node_index is not null) and events.node_index = watermarks.node_index
          where (
              -- no need to consider disabled members since they can't be served events anymore
              sequencer_members.enabled = true
              -- consider the given timestamp
              and sequencer_members.registered_ts <= $timestamp
              and events.ts is not null
              -- if the sequencer that produced the event is offline, only consider up until its offline watermark
              and  (watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts))
            )
          group by (sequencer_members.member, events.ts)
          """
    }
    query.as[(Member, Option[CantonTimestamp])].map(_.toMap)
  }
}
