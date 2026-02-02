// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local pts = panels.timeSeries;

local selectors = 'namespace="$namespace"';

db.new('Database')
+ db.withUid('digital-asset-database-ops')
+ db.withTags(['db', 'performance', 'postgres'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_db_storage_general_executor_running'),
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    pts.percentiles(
      'Commit Latency',
      queries.commonPercentiles('daml_db_commit_duration_seconds', selectors)
    ),
    pts.percentiles(
      'SQL Query Run Latency',
      queries.commonPercentiles('daml_db_query_duration_seconds', selectors)
    ),
    pts.percentiles(
      'SQL Query and Result Read Latency',
      queries.commonPercentiles('daml_db_exec_duration_seconds', selectors)
    ),
    pts.percentiles(
      'Connection Acquire Latency',
      queries.commonPercentiles('daml_db_wait_duration_seconds', selectors)
    ),
  ], panelWidth=12),
)
