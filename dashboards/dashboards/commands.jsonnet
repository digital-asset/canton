// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local pts = panels.timeSeries;

local selectors = 'namespace="$namespace"';

db.new('Commands')
+ db.withUid('digital-asset-commands')
+ db.withTags([])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_participant_api_commands_valid_submissions_total'),
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    pts.throughput('Valid Submissions Throughput', [
      queries.simple('rate(daml_participant_api_commands_valid_submissions_total{%s}[$__rate_interval])' % selectors, '{{node}}'),
    ]),
    pts.short('Commands In-flight', [
      queries.simple('daml_participant_api_commands_max_in_flight_length{%s}' % selectors, 'length'),
      queries.simple('daml_participant_api_commands_max_in_flight_capacity{%s}' % selectors, 'capacity'),
    ]),
    pts.percentiles(
      'Total Command Processing Latency',
      queries.commonPercentiles('daml_participant_api_commands_submissions_duration_seconds', selectors)
    ),
    pts.percentiles(
      'Command Validation Latency',
      queries.commonPercentiles('daml_participant_api_commands_validation_duration_seconds', selectors)
    ),
    pts.short('Failed Command Interpretations', [
      queries.simple('daml_participant_api_commands_failed_command_interpretations_total{%s}' % selectors, '{{node}}'),
    ]),
  ], panelWidth=12),
)
