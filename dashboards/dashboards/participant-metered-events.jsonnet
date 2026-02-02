// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local pts = panels.timeSeries;

local vars = {
  local q = g.dashboard.variable.query,
  participant:
    q.new('participant')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(sharedVars.datasource)
    + q.queryTypes.withLabelValues('participant_id', 'daml_participant_api_indexer_metered_events_total{namespace="$namespace"}')
    + q.selectionOptions.withMulti()
    + q.selectionOptions.withIncludeAll(true, '.*'),
  application:
    q.new('application')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(sharedVars.datasource)
    + q.queryTypes.withLabelValues('application_id', 'daml_participant_api_indexer_metered_events_total{namespace="$namespace", participant_id=~"$participant"}')
    + q.selectionOptions.withMulti()
    + q.selectionOptions.withIncludeAll(true, '.*'),
  groupBy:
    g.dashboard.variable.custom.new('group_by', values=['participant_id', 'application_id'])
    + g.dashboard.variable.custom.generalOptions.withLabel('group by')
    + q.selectionOptions.withMulti(),
};

local selectors = 'namespace="$namespace", participant_id=~"$participant", application_id=~"$application"';

db.new('Participant Metered Events')
+ db.withUid('digital-asset-metered-events')
+ db.withTags([])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_participant_api_indexer_metered_events_total'),
  vars.participant,
  vars.application,
  vars.groupBy,
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    panels.gauge('Metered Events - Increase', [
      queries.simple('sum by($group_by) (increase(daml_participant_api_indexer_metered_events_total{%s}[$__range]))' % selectors),
    ]),
    pts.throughput('Metered Events - Throughput', [
      queries.simple('sum by(${group_by:csv}) (rate(daml_participant_api_indexer_metered_events_total{%s}[$__rate_interval])) > 0' % selectors),
    ], unit='eps'),
  ], panelWidth=24),
)
