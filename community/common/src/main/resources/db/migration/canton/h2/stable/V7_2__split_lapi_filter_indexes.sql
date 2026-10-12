-- Drop the trailing `first_per_sequential_id` column from the `_s_idx` indexes on the LAPI filter
-- tables. See the Postgres counterpart of this migration for the rationale. Postgres additionally
-- gains a `_sf_idx` partial index on `event_sequential_id where first_per_sequential_id`; H2 does
-- not support partial indexes, so no equivalent is created here.

drop index lapi_filter_activate_stakeholder_s_idx;
create index lapi_filter_activate_stakeholder_s_idx on lapi_filter_activate_stakeholder using btree (event_sequential_id);

drop index lapi_filter_activate_witness_s_idx;
create index lapi_filter_activate_witness_s_idx on lapi_filter_activate_witness using btree (event_sequential_id);

drop index lapi_filter_achs_stakeholder_s_idx;
create index lapi_filter_achs_stakeholder_s_idx on lapi_filter_achs_stakeholder using btree (event_sequential_id);

drop index lapi_filter_deactivate_stakeholder_s_idx;
create index lapi_filter_deactivate_stakeholder_s_idx on lapi_filter_deactivate_stakeholder using btree (event_sequential_id);

drop index lapi_filter_deactivate_witness_s_idx;
create index lapi_filter_deactivate_witness_s_idx on lapi_filter_deactivate_witness using btree (event_sequential_id);

drop index lapi_filter_various_witness_s_idx;
create index lapi_filter_various_witness_s_idx on lapi_filter_various_witness using btree (event_sequential_id);

