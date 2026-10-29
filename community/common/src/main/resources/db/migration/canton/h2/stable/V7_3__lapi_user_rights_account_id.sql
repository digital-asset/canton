-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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
