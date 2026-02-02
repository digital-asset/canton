// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local pts = panels.timeSeries;

local selectors = 'namespace="$namespace"';
local labels = '[{{node}}] {{cache}}';

db.new('Caching')
+ db.withUid('digital-asset-caching')
+ db.withTags(['cache', 'performance'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_cache_size'),
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    pts.short('Cache Sizes', [
      queries.simple('daml_cache_size{%s}' % selectors, labels),
    ]),
    pts.throughput('Cache Throughput (hits)', [
      queries.simple('rate(daml_cache_hits{%s}[$__rate_interval])' % selectors, labels),
    ]),
    pts.percentiles(
      'Contract State Cache Update Latency',
      queries.commonPercentiles('daml_participant_api_execution_cache_contract_state_register_update_duration_seconds', selectors)
    ),
  ], panelWidth=12),
)
