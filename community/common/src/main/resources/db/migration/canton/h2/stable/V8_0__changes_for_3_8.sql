-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- H2 does not support partial indexes.
create index sequencer_members_onboarding_idx
    on sequencer_members (status, registered_ts);

-- participant event category to activation/deactivation tables
alter table lapi_events_activate_contract add column participant_event_category int;
alter table lapi_events_deactivate_contract add column participant_event_category int;

-- traffic enforcement uncharged completions and projection state
create table par_traffic_enforcement_uncharged_completions
(
    -- Ledger API offset of the completion
    completion_offset bigint   not null primary key,
    -- Hash of the Ledger API change id
    change_id_hash    varchar  not null,
    submission_id     varchar  not null,
    paid_traffic_cost bigint   not null,
    -- Why the completion wasn't charged
    reason            smallint not null,
    -- Shape of the completion's act-as parties
    act_as_category   smallint not null,
    -- Record time of the completion
    timestamp         bigint   not null
);

create index idx_traffic_uncharged_completion_timestamp on par_traffic_enforcement_uncharged_completions (timestamp);

create table par_traffic_enforcement_state
(
    -- For example "last_completion_offset"
    name        varchar not null primary key,
    state_value varchar not null
);

-- traffic debit rights on user rights
-- The unique constraint on lapi_user_rights has no name, so we recreate the table to extend it by account_id.
create table lapi_user_rights_new (
    user_internal_id integer not null references lapi_users (internal_id) on delete cascade,
    user_right integer not null,
    for_party varchar,
    for_party2 varchar generated always as (case
                                                     when for_party is not null
                                                     then for_party
                                                     else ''
                                                     end),
    -- Set only for CanDebitTrafficFrom rights
    account_id varchar,
    account_id2 varchar generated always as (case
                                                      when account_id is not null
                                                      then account_id
                                                      else ''
                                                      end),
    granted_at bigint not null,
    unique (user_internal_id, user_right, for_party2, account_id2),
    constraint lapi_user_rights_party_or_account_check check (for_party is null or account_id is null)
);

insert into lapi_user_rights_new (user_internal_id, user_right, for_party, granted_at)
    select user_internal_id, user_right, for_party, granted_at
    from lapi_user_rights;

drop table lapi_user_rights;

alter table lapi_user_rights_new rename to lapi_user_rights;

create index lapi_string_interning_es_idx on lapi_string_interning using btree (external_string);
