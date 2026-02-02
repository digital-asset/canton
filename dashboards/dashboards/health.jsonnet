// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local psh = panels.statusHistory;

db.new('Health')
+ db.withUid('digital-asset-health')
+ db.withTags(['availability'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_health_status'),
  sharedVars.pod('daml_health_status'),
  sharedVars.container('daml_health_status'),
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    psh.health("Components' Status", [queries.daml.health(sharedVars.selectors)]),
  ], panelWidth=24),
)
