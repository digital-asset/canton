-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table accepted_agreements (domain_id varchar(300) not null, agreement_id varchar(300) not null);
create table service_agreements (domain_id varchar(300) not null, agreement_id varchar(300) not null, agreement_text varchar not null);

create unique index idx_service_agreements on service_agreements(domain_id, agreement_id);
create unique index idx_accepted_agreements on accepted_agreements(domain_id, agreement_id);
