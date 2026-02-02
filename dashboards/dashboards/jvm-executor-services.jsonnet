// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local row = g.panel.row;
local pts = panels.timeSeries;

local vars = {
  local var = g.dashboard.variable,
  local q = var.query,
  local so = var.custom.selectionOptions,
  name:
    q.new('name')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(sharedVars.datasource)
    + q.queryTypes.withLabelValues('name', 'daml_executor_pool_size{%s}' % sharedVars.selectors)
    + so.withMulti()
    + so.withIncludeAll(),
  groupWithout:
    var.constant.new('group_without', 'endpoint, instance,daml_version,job,namespace,container,canton_version')
    + var.constant.generalOptions.showOnDashboard.withNothing(),
};

local selectors = '%s, name="$name"' % sharedVars.selectors;
local labels = '{{pod}} - {{container}} - {{job}} - {{name}}';
local legend = pts.withLegend(displayMode='table', placement='right', calcs=['lastNotNull']);
local commonDescription = g.panel.timeSeries.panelOptions.withDescription('Common between thread and fork-join pools');

db.new('JVM Executor Services')
+ db.withUid('digital-asset-jvm-executor-services')
+ db.withTags(['jvm', 'executors', 'pools'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('daml_executor_pool_size'),
  sharedVars.pod('daml_executor_pool_size'),
  sharedVars.container('daml_executor_pool_size'),
  vars.name,
  vars.groupWithout,
])

+ db.withPanels([
  row.new('Runtime Metrics')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    pts.latency(
      'Task Run Latency (p95)',
      queries.percentile(
        'daml_executor_runtime_duration_seconds',
        selectors,
        'namespace, pod, container, name, component, participant',
        0.95,
        '%s / {{component}} {{participant}}' % labels
      )
    ) + legend,
    pts.latency('Task Run Latency (average)', [
      queries.simple(
        'histogram_sum(sum without() (daml_executor_runtime_duration_seconds{%s})) / histogram_count(sum without() (daml_executor_runtime_duration_seconds{%s}))' % [selectors, selectors],
        '%s / {{component}} {{participant}}' % labels
      ),
    ]) + legend,
    pts.latency(
      'Task Idle Duration (p95)',
      queries.percentile(
        'daml_executor_runtime_idle_duration_seconds',
        selectors,
        'namespace, pod, container, name, component, participant',
        0.95,
        '%s / {{component}} {{participant}}' % labels
      )
    ) + legend,
    pts.latency('Task Idle Duration (average)', [
      queries.simple(
        'histogram_sum(sum without() (daml_executor_runtime_idle_duration_seconds{%s})) / histogram_count(sum without() (daml_executor_runtime_idle_duration_seconds{%s}))' % [selectors, selectors],
        labels
      ),
    ]) + legend,
    pts.throughput('Submitted Tasks', [
      queries.simple('sum without($group_without) (rate(daml_executor_runtime_submitted_total{%s}[$__rate_interval]))' % selectors, labels),
    ]),
    pts.throughput('Completed Tasks', [
      queries.simple('sum without($group_without) (rate(daml_executor_runtime_completed_total{%s}[$__rate_interval]))' % selectors, labels),
    ]),
    pts.short('Running Tasks', [
      queries.simple('sum without($group_without) (daml_executor_runtime_running{%s})' % selectors, labels),
    ], width=24),
  ], panelWidth=12)),

  row.new('Common Metrics')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    pts.short('Pool Size', [
      queries.simple('daml_executor_pool_size{%s}' % selectors, '%s / {{type}}' % labels),
    ]) + legend + commonDescription,
    pts.short('Active Threads', [
      queries.simple('daml_executor_threads_active{%s}' % selectors, '%s / {{type}}' % labels),
    ]) + legend + commonDescription,
    pts.short('Queued Tasks', [
      queries.simple('daml_executor_tasks_queued{%s}' % selectors, '%s / {{type}}' % labels),
    ]) + legend + commonDescription,
  ], panelWidth=24)),

  row.new('Fork-Join Metrics')
  + row.withCollapsed(true)
  + row.withPanels(g.util.grid.wrapPanels([
    pts.short('Pool Size', [
      queries.simple('daml_executor_threads_running{%s}' % selectors, labels),
    ]) + legend,
    pts.throughput('Stolen Tasks', [
      queries.simple('sum without($group_without) (rate(daml_executor_tasks_stolen{%s}[$__rate_interval]))' % selectors, labels),
    ]) + legend,
    pts.throughput('Task Queuing Rate', [
      queries.simple('sum without($group_without) (rate(daml_executor_tasks_executing_queued{%s}[$__rate_interval]))' % selectors, labels),
    ]) + legend,
    pts.short('Queued Tasks', [
      queries.simple('daml_executor_tasks_executing_queued{%s}' % selectors, labels),
    ]) + legend,
  ], panelWidth=24)),
])
