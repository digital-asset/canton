// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local row = g.panel.row;
local pts = panels.timeSeries;
local psh = panels.statusHistory;
local pbc = panels.barChart;

local selectors = 'namespace="$namespace"';
local noLegend = pts.withLegend(showLegend=false);

db.new('Network Operation Center Health')
+ db.withUid('digital-asset-noc-health')
+ db.withTags(['availability'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_health_status'),
])

+ db.withPanels([
  row.new('Participant Received Events')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    pts.throughput('Transactions', [
      queries.simple('sum by(status) (rate(daml_participant_api_indexer_events_total{%s, event_type="transaction"}[$__rate_interval]))' % selectors),
    ], unit='cps'),
    pbc.throughput('Processed Events', [
      queries.simple('rate(daml_participant_api_indexer_events_total{%s}[$__rate_interval])' % selectors, '{{application_id}}:{{event_type}}'),
    ], unit='cps'),
    pbc.throughput('Metered Events', [
      queries.simple('rate(daml_participant_api_indexer_metered_events_total{%s}[$__rate_interval])' % selectors, '{{application_id}}'),
    ], unit='cps'),
  ], panelWidth=8)),

  row.new('gRPC Endpoints')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    pts.throughput('Rate | All gRPC Requests', [
      queries.simple('sum(rate(daml_grpc_server_handled_total{%s}[$__rate_interval]))' % selectors, 'all gRPC requests'),
    ]) + noLegend,
    pbc.returnCodes('Return Code | All gRPC Requests', [
      queries.simple('sum by(grpc_code) (rate(daml_grpc_server_handled_total{%s}[$__rate_interval]))' % selectors),
    ]),
    pts.percentiles(
      'Latency | All gRPC Requests',
      queries.commonPercentiles('daml_grpc_server_duration_seconds', selectors)
    ),
  ], panelWidth=8)),

  row.new('Top 5 Failures')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    pts.throughput('Rejected Transactions | Top 5 Application IDs', [
      queries.simple(
        'topk(5, sum by(application_id) (rate(daml_participant_api_indexer_events_total{%s, status="rejected"}[$__rate_interval])))' % selectors,
        '{{application_id}}'
      ),
    ], unit='cps') + noLegend,
    pts.throughput('gRPC Errors | Top 5 Endpoints', [
      queries.simple(
        'topk(5, sum by(grpc_service_name, grpc_method_name) (rate(daml_grpc_server_handled_total{%s, grpc_code!="OK"}[$__rate_interval])))' % selectors,
        '{{grpc_service_name}}:{{grpc_method_name}}'
      ),
    ], unit='cps') + noLegend,
  ], panelWidth=8)),

  row.new('Misc.')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    panels.text(null, "# Detailed Dashboards\n\n* Events: [Received](/d/digital-asset-received-events) / [Metered](/d/digital-asset-metered-events)\n* [Components' Status](/d/digital-asset-health)\n* [gRPC Endpoints](/d/digital-asset-grpc-endpoints)\n* [JVM Metrics](/d/digital-asset-jvm-metrics)\n* [JVM Executor Services](/d/digital-asset-jvm-executor-services)"),
    psh.health("Components' Status", [queries.daml.health(selectors)]),
    pts.garbageCollection('JVM Garbage Collection', queries.jvm.gc(selectors).all),
  ], panelWidth=8)),
])
