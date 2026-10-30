-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Set only for CanDebitTrafficFrom rights
alter table lapi_user_rights add column account_id varchar collate "C";

alter table lapi_user_rights add constraint lapi_user_rights_party_or_account_check
    check (for_party is null or account_id is null);

drop index lapi_user_rights_user_internal_id_user_right_idx;

create unique index lapi_user_rights_user_internal_id_user_right_idx
    on lapi_user_rights using btree (user_internal_id, user_right)
    where (for_party is null and account_id is null);

create unique index lapi_user_rights_user_internal_id_user_right_account_idx
    on lapi_user_rights using btree (user_internal_id, user_right, account_id)
    where (account_id is not null);

create or replace function debug.lapi_user_right(integer) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 1 then 'ParticipantAdmin'
    when $1 = 2 then 'CanActAs'
    when $1 = 3 then 'CanReadAs'
    when $1 = 4 then 'IdentityProviderAdmin'
    when $1 = 5 then 'CanReadAsAnyParty'
    when $1 = 6 then 'CanExecuteAs'
    when $1 = 7 then 'CanExecuteAsAnyParty'
    when $1 = 8 then 'CanActAsAnyParty'
    when $1 = 9 then 'CanDebitTrafficFrom'
    when $1 = 10 then 'CanDebitTrafficFromAnyAccount'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

create or replace view debug.lapi_user_rights as
  select
    debug.resolve_lapi_user(user_internal_id) as user_internal_id,
    debug.lapi_user_right(user_right) as user_right,
    for_party,
    debug.canton_timestamp(granted_at) as granted_at,
    account_id
  from lapi_user_rights;
