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
  quantile:
    g.dashboard.variable.custom.new('quantile', values=['0.5', '0.9', '0.95', '0.99']),
  component:
    q.new('component')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(sharedVars.datasource)
    + q.queryTypes.withLabelValues('service', 'daml_grpc_server_started_total{namespace="$namespace"}'),
  service:
    q.new('service')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(sharedVars.datasource)
    + q.queryTypes.withLabelValues('grpc_service_name', 'daml_grpc_server_started_total{namespace="$namespace", service="$component"}'),
  method:
    q.new('method')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(sharedVars.datasource)
    + q.queryTypes.withLabelValues('grpc_method_name', 'daml_grpc_server_started_total{namespace="$namespace", service="$component", grpc_service_name="$service"}')
    + q.selectionOptions.withIncludeAll(),
};

local selectors = '%s, service="$component", grpc_service_name=~"$service", grpc_method_name=~"$method"' % sharedVars.selectors;

db.new('gRPC Endpoints')
+ db.withUid('digital-asset-grpc-endpoints')
+ db.withTags(['grpc', 'network', 'bandwidth'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_grpc_server_started_total'),
  sharedVars.pod('daml_grpc_server_started_total'),
  sharedVars.container('daml_grpc_server_started_total'),
  vars.component,
  vars.service,
  vars.method,
  vars.quantile,
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    pts.latency('Latency Quantile', [
      queries.simple(
        'histogram_quantile($quantile, sum by(grpc_service_name, grpc_client_type, grpc_method_name, grpc_server_type, grpc_code) (rate(daml_grpc_server_duration_seconds{%s}[$__rate_interval])))' % selectors,
        '{{grpc_method_name}} - {{grpc_code}} ({{grpc_client_type}} - {{grpc_server_type}})'
      ),
    ], width=24),
    pts.throughput('Requests Started', [
      queries.simple(
        'sum by(grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name) (rate(daml_grpc_server_started_total{%s}[$__rate_interval]))' % selectors,
        '{{grpc_method_name}} ({{grpc_client_type}} - {{grpc_server_type}})'
      ),
    ], unit='reqps'),
    pts.throughput('Requests Finished', [
      queries.simple(
        'sum by(grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name, grpc_code) (rate(daml_grpc_server_handled_total{%s}[$__rate_interval]))' % selectors,
        '{{grpc_method_name}} - {{grpc_code}} - ({{grpc_client_type}} - {{grpc_server_type}})'
      ),
    ], unit='reqps'),
    pts.throughput('Errors', [
      queries.simple(
        'sum by(grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name, grpc_code) (rate(daml_grpc_server_handled_total{%s, grpc_code!="OK"}[$__rate_interval]))' % selectors,
        '{{grpc_method_name}} - {{grpc_code}}'
      ),
    ]),
    pts.histogram('Error Distribution', [
      queries.simple(
        'sum by(service, grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name, grpc_code) (rate(daml_grpc_server_handled_total{%s, grpc_code!="OK"}[1m])) / on(service, grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name) group_left() sum by(service, grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name) (rate(daml_grpc_server_handled_total{%s}[1m]))' % [selectors, selectors],
        '{{grpc_method_name}} - {{grpc_code}}'
      ),
    ]),
    pts.bytesLog2('Average Request Payload Size', [
      queries.simple(
        'histogram_sum(sum by(grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name) (rate(daml_grpc_server_messages_received_bytes{%s}[$__rate_interval]))) / histogram_count(sum by(grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name) (rate(daml_grpc_server_messages_received_bytes{%s}[$__rate_interval])))' % [selectors, selectors],
        '{{grpc_method_name}} - ({{grpc_client_type}} - {{grpc_server_type}})'
      ),
    ]),
    pts.bytesLog2Throughput('Request Payload Throughput', [
      queries.simple(
        'histogram_sum(sum by(grpc_server_type, grpc_service_name, grpc_client_type, grpc_method_name) (rate(daml_grpc_server_messages_received_bytes{%s}[$__rate_interval])))' % selectors,
        '{{grpc_method_name}} - ({{grpc_client_type}} - {{grpc_server_type}})'
      ),
    ]),
  ], panelWidth=12),
)
