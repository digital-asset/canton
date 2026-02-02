// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

/// TEMPLATED MANAGED FILE, DO NOT EDIT
// If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

local g = import 'g.libsonnet';
local variables = import 'variables.libsonnet';
local promq = g.query.prometheus;
local datasource = '${%s}' % variables.datasource.name;

{
  simple(query, label='__auto'):
    promq.new(datasource, query)
    + promq.withLegendFormat(label),

  percentile(ts, selectors, sumBy='namespace', quantile=0.95, label='__auto'):
    self.simple(
      'histogram_quantile(%.2f, sum by(%s) (rate(%s{%s}[$__rate_interval])))' % [quantile, sumBy, ts, selectors],
      label
    ),

  commonPercentiles(ts, selectors, sumBy='namespace'): [
    self.percentile(ts, selectors, sumBy, 0.5, 'p50'),
    self.percentile(ts, selectors, sumBy, 0.9, 'p90'),
    self.percentile(ts, selectors, sumBy, 0.95, 'p95'),
    self.percentile(ts, selectors, sumBy, 0.99, 'p99'),
  ],

  jvm: {
    selectors: '%s, job="$jvm"' % variables.selectors,

    diagnostics: {
      micrometer: {
        cpus: {
          available: $.simple('system_cpu_count{%s}' % $.jvm.selectors, 'available'),
          used: $.simple('system_cpu_usage{%s} * system_cpu_count{%s}' % [$.jvm.selectors, $.jvm.selectors], 'in-use'),
          system_load_1m: $.simple('system_load_average_1m{%s}' % $.jvm.selectors, 'system load (1m)'),
        },
        threads:
          $.simple('jvm_threads_states_threads{%s}' % $.jvm.selectors, '{{state}}'),
        gc: {
          base(source, label=source, scale='1'):
            $.simple('sum(idelta(jvm_gc_pause_seconds_%s{%s}[$__rate_interval])) by(gc) * %s'
                     % [source, $.jvm.selectors, scale],
                     '{{gc}} (%s)' % label),
          count: self.base('count'),
          time: self.base('sum', 'time', '1e-9'),
          all: [self.count, self.time],
        },
        memory: {
          heap: {
            base(memory_type, label=memory_type):
              $.simple('sum(jvm_memory_%s_bytes{%s, area="heap"}) by(area)' % [memory_type, $.jvm.selectors], label),
            used: self.base('used'),
            committed: self.base('committed'),
            limit: self.base('max'),
            all: [self.used, self.committed, self.limit],
          },
          pools: {
            base(memory_type, label=memory_type):
              $.simple('jvm_memory_%s_bytes{%s, id="$jvm_mempool"}' % [memory_type, $.jvm.selectors], label),
            allocated:
              $.simple('irate(jvm_memory_used_bytes{%s, id="$jvm_mempool"}[$__rate_interval])' % $.jvm.selectors, 'allocated'),
            used: self.base('used'),
            committed: self.base('committed'),
            limit: self.base('max'),
            all: [self.used, self.committed, self.limit, self.allocated],
          },
        },
      },
    },

    cpus: {
      available: $.simple('jvm_cpu_count{%s}' % $.jvm.selectors, 'available'),
      used: $.simple('jvm_cpu_recent_utilization_ratio{%s} * jvm_cpu_count{%s}' % [$.jvm.selectors, $.jvm.selectors], 'in-use'),
      system_load_1m: $.simple('jvm_system_cpu_load_1m{%s}' % $.jvm.selectors, 'system load (1m)'),
    },

    gc(selectors=null): {
      base(source, label=source):
        $.simple('sum(idelta(jvm_gc_duration_seconds_%s{%s}[$__rate_interval])) by(jvm_gc_name, job)'
                 % [source, if selectors != null then selectors else $.jvm.selectors],
                 '[{{job}}] {{jvm_gc_name}} (%s)' % label),
      base_expo(source, label=source):
        $.simple('histogram_%s(sum(rate(jvm_gc_duration_seconds{%s}[$__rate_interval])) by(jvm_gc_name, job))'
                 % [source, if selectors != null then selectors else $.jvm.selectors],
                 '[{{job}}] {{jvm_gc_name}} (%s)' % label),
      count: self.base('count'),
      time: self.base('sum', 'time'),
      count_expo: self.base_expo('count'),
      time_expo: self.base_expo('sum', 'time'),
      all: [self.count, self.time, self.count_expo, self.time_expo],
    },

    threads:
      $.simple('jvm_thread_count{%s}' % $.jvm.selectors, '{{jvm_thread_state}}'),

    memory: {
      base(area, memory_type, label=memory_type):
        $.simple('sum(jvm_memory_%s_bytes{%s, jvm_memory_type="%s"}) by(jvm_memory_type)' % [memory_type, $.jvm.selectors, area], label),
      heap: {
        used: $.jvm.memory.base('heap', 'used'),
        committed: $.jvm.memory.base('heap', 'committed'),
        limit: $.jvm.memory.base('heap', 'limit', 'max'),
        all: [self.used, self.committed, self.limit],
      },
      non_heap: {
        used: $.jvm.memory.base('non_heap', 'used'),
        committed: $.jvm.memory.base('non_heap', 'committed'),
        limit: $.jvm.memory.base('non_heap', 'limit', 'max'),
        all: [self.used, self.committed, self.limit],
      },

      pools: {
        base(memory_type, label=memory_type):
          $.simple('jvm_memory_%s_bytes{%s, jvm_memory_pool_name="$jvm_mempool"}' % [memory_type, $.jvm.selectors], label),
        allocated:
          $.simple('irate(jvm_memory_used_bytes{%s, jvm_memory_pool_name="$jvm_mempool"}[$__rate_interval])' % $.jvm.selectors, 'allocated'),
        used: self.base('used'),
        committed: self.base('committed'),
        limit: self.base('limit', 'max'),
        all: [self.used, self.committed, self.limit, self.allocated],
      },
    },
  },

  daml: {
    health(selectors=null): $.simple('daml_health_status{%s}' % selectors, '{{component}}'),
  },
}
