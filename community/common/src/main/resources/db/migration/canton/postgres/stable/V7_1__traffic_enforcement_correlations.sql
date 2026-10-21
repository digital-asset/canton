-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table par_traffic_enforcement_correlations
(
    -- Hash of the Ledger API change id
    change_id_hash varchar collate "C" not null,
    -- Submission id distinguishes attempts for the same change id
    submission_id  varchar collate "C" not null,
    -- NULL means no account was selected
    account_id     varchar collate "C",
    -- Participant wall clock at insert
    timestamp      bigint              not null,
    primary key (change_id_hash, submission_id)
);

create index idx_traffic_correlation_timestamp on par_traffic_enforcement_correlations (timestamp);

create or replace view debug.par_traffic_enforcement_correlations as
select
    change_id_hash,
    submission_id,
    account_id,
    debug.canton_timestamp(timestamp) as timestamp
from par_traffic_enforcement_correlations;
