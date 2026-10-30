-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table par_traffic_enforcement_uncharged_completions
(
    -- Ledger API offset of the completion
    completion_offset bigint              not null primary key,
    -- Hash of the Ledger API change id
    change_id_hash    varchar collate "C" not null,
    submission_id     varchar collate "C" not null,
    paid_traffic_cost bigint              not null,
    -- Why the completion wasn't charged
    reason            smallint            not null,
    -- Shape of the completion's act-as parties
    act_as_category   smallint            not null,
    -- Record time of the completion
    timestamp         bigint              not null
);

create index idx_traffic_uncharged_completion_timestamp on par_traffic_enforcement_uncharged_completions (timestamp);

create or replace function debug.tea_uncharged_reason(smallint) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 0 then 'missing_correlation'
    when $1 = 1 then 'no_account_selected'
    when $1 = 2 then 'rejected_completion_charging_disabled'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

create or replace function debug.tea_act_as_category(smallint) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 0 then 'multiple_acting_parties'
    when $1 = 1 then 'admin_acting_party'
    when $1 = 2 then 'single_non_admin_acting_party'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

create or replace view debug.par_traffic_enforcement_uncharged_completions as
select
    completion_offset,
    change_id_hash,
    submission_id,
    paid_traffic_cost,
    debug.tea_uncharged_reason(reason) as reason,
    debug.tea_act_as_category(act_as_category) as act_as_category,
    debug.canton_timestamp(timestamp) as timestamp
from par_traffic_enforcement_uncharged_completions;

create table par_traffic_enforcement_state
(
    -- For example "last_completion_offset"
    name        varchar collate "C" not null primary key,
    state_value varchar collate "C" not null
);

create or replace view debug.par_traffic_enforcement_state as
select
    name,
    state_value
from par_traffic_enforcement_state;
