-- Split the `(event_sequential_id, first_per_sequential_id)` index on the LAPI filter tables into
-- a plain index on `event_sequential_id` and a partial index on `event_sequential_id` restricted to
-- `first_per_sequential_id`.

drop index lapi_filter_activate_stakeholder_s_idx;
create index lapi_filter_activate_stakeholder_s_idx  on lapi_filter_activate_stakeholder using btree (event_sequential_id);
create index lapi_filter_activate_stakeholder_sf_idx on lapi_filter_activate_stakeholder using btree (event_sequential_id) where first_per_sequential_id;

drop index lapi_filter_activate_witness_s_idx;
create index lapi_filter_activate_witness_s_idx  on lapi_filter_activate_witness using btree (event_sequential_id);
create index lapi_filter_activate_witness_sf_idx on lapi_filter_activate_witness using btree (event_sequential_id) where first_per_sequential_id;

drop index lapi_filter_achs_stakeholder_s_idx;
create index lapi_filter_achs_stakeholder_s_idx  on lapi_filter_achs_stakeholder using btree (event_sequential_id);
create index lapi_filter_achs_stakeholder_sf_idx on lapi_filter_achs_stakeholder using btree (event_sequential_id) where first_per_sequential_id;

drop index lapi_filter_deactivate_stakeholder_s_idx;
create index lapi_filter_deactivate_stakeholder_s_idx  on lapi_filter_deactivate_stakeholder using btree (event_sequential_id);
create index lapi_filter_deactivate_stakeholder_sf_idx on lapi_filter_deactivate_stakeholder using btree (event_sequential_id) where first_per_sequential_id;

drop index lapi_filter_deactivate_witness_s_idx;
create index lapi_filter_deactivate_witness_s_idx  on lapi_filter_deactivate_witness using btree (event_sequential_id);
create index lapi_filter_deactivate_witness_sf_idx on lapi_filter_deactivate_witness using btree (event_sequential_id) where first_per_sequential_id;

drop index lapi_filter_various_witness_s_idx;
create index lapi_filter_various_witness_s_idx  on lapi_filter_various_witness using btree (event_sequential_id);
create index lapi_filter_various_witness_sf_idx on lapi_filter_various_witness using btree (event_sequential_id) where first_per_sequential_id;

