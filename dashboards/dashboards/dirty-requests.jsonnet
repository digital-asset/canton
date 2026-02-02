// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local pts = panels.timeSeries;

local selectors = 'namespace="$namespace", pod=~"$pod"';
local labels = '[{{node}} / {{domain}}]';
local legend = pts.withLegend(displayMode='table', placement='right', calcs=['lastNotNull']);

db.new('Dirty Requests')
+ db.withUid('digital-asset-dirty-requests')
+ db.withTags([])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_participant_inflight_validation_requests'),
  sharedVars.pod('daml_participant_inflight_validation_requests'),
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    pts.short('In-flight Dirty Requests', [
      queries.simple('daml_participant_inflight_validation_requests{%s}' % selectors, '%s in-flight' % labels),
      queries.simple('daml_participant_max_inflight_validation_requests{%s}' % selectors, '%s max' % labels),
    ]) + legend,
  ], panelWidth=24),
)
