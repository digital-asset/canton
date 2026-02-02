// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

/// TEMPLATED MANAGED FILE, DO NOT EDIT
// If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

local g = import 'g.libsonnet';
local var = g.dashboard.variable;
local ds = var.datasource;
local q = var.query;
local so = var.custom.selectionOptions;

{
  selectors: 'namespace="$namespace", pod=~"$pod", container=~"$container"',

  datasource:
    ds.new('datasource', 'prometheus')
    + ds.generalOptions.withLabel('Source')
    + ds.generalOptions.withDescription('Metrics source'),

  namespace(ts):
    q.new('namespace')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(self.datasource)
    + q.generalOptions.withLabel('Namespace')
    + q.generalOptions.withDescription('Kubernetes namespace')
    + q.queryTypes.withLabelValues('namespace', ts),

  pod(ts):
    q.new('pod')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(self.datasource)
    + q.generalOptions.withLabel('Pod')
    + q.generalOptions.withDescription('Namespace pods')
    + q.queryTypes.withLabelValues('pod', '%s{namespace="$namespace"}' % ts)
    + so.withMulti()
    + so.withIncludeAll(),

  container(ts):
    q.new('container')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(self.datasource)
    + q.generalOptions.withLabel('Container')
    + q.generalOptions.withDescription('Pod containers')
    + q.queryTypes.withLabelValues('container', '%s{namespace="$namespace", pod=~"$pod"}' % ts)
    + so.withMulti()
    + so.withIncludeAll(),

  jvm:
    q.new('jvm')
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(self.datasource)
    + q.generalOptions.withLabel('Job')
    + q.generalOptions.withDescription('JVM jobs')
    + q.queryTypes.withLabelValues(
      'job',
      'jvm_memory_used_bytes{%s}' % self.selectors
    ),

  jvm_mempool:
    q.new(
      'jvm_mempool',
      '{__name__="jvm_memory_used_bytes", job="$%s"}' % self.jvm.name
    )
    + q.refresh.onTime()
    + q.withDatasourceFromVariable(self.datasource)
    + q.withRegex('/.*((id|jvm_memory_pool_name)="(?<text>.*?)").*/')
    + q.withSort()
    + q.generalOptions.withLabel('MemPool')
    + q.generalOptions.withDescription('JVM memory pools')
    + q.generalOptions.showOnDashboard.withNothing()
    + q.selectionOptions.withIncludeAll(),
}
