// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

/// TEMPLATED MANAGED FILE, DO NOT EDIT
// If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

local g = import 'g.libsonnet';

{
  timeSeries: {
    local this = self,
    local ts = g.panel.timeSeries,
    local custom = ts.fieldConfig.defaults.custom,
    local options = ts.options,

    base(title, targets, width=null):
      ts.new(title)
      + ts.queryOptions.withTargets(targets)
      + ts.queryOptions.withInterval('10s')
      + ts.standardOptions.withDecimals(null)
      + ts.panelOptions.withGridPos(w=width)
      + ts.options.withTooltip({ mode: 'multi' })
      + custom.withFillOpacity(0)
      + custom.withSpanNulls(true)
      + custom.withAxisSoftMin(0)
      + custom.withShowPoints('never'),

    short(title, targets, width=null):
      self.base(title, targets, width)
      + ts.standardOptions.withUnit('short'),

    shortStacked(title, targets, width=null):
      self.short(title, targets, width)
      + custom.withFillOpacity(10)
      + custom.withStacking({ mode: 'normal' }),

    throughput(title, targets, width=null, unit='ops'):
      self.base(title, targets, width)
      + ts.standardOptions.withUnit(unit),

    throughputStacked(title, targets, width=null, unit='ops'):
      self.throughput(title, targets, width, unit)
      + custom.withStacking({ mode: 'normal' })
      + custom.withFillOpacity(10),

    bytes(title, targets, width=null):
      self.base(title, targets, width)
      + ts.standardOptions.withUnit('decbytes'),

    bytesLog2(title, targets, width=null):
      self.bytes(title, targets, width)
      + custom.scaleDistribution.withType('log')
      + custom.scaleDistribution.withLog(2),

    bytesLog2Throughput(title, targets, width=null):
      self.bytesLog2(title, targets, width)
      + ts.standardOptions.withUnit('Bps'),

    histogram(title, targets, width=null):
      self.base(title, targets, width)
      + ts.standardOptions.withUnit('percentunit')
      + custom.withFillOpacity(100)
      + custom.withDrawStyle('bars')
      + custom.withStacking({ mode: 'normal' }),

    garbageCollection(title, targets, width=null):
      self.short(title, targets, width)
      + ts.standardOptions.withOverrides([
        ts.fieldOverride.byRegexp.new('/.*\\(time\\)/')
        + ts.fieldOverride.byRegexp.withProperty('unit', 's')
        + ts.fieldOverride.byRegexp.withProperty('decimals', null)
        + ts.fieldOverride.byRegexp.withProperty('custom.fillOpacity', 10),
      ]),

    memoryUsage(title, targets, width=null):
      self.bytes(title, targets, width)
      + custom.withFillOpacity(10)
      + $.utils.hideExceptNames(ts, ['used', 'committed']),

    memoryPoolsUsage(title, targets, width=null, maxPerRow=3, repeat_label='jvm_mempool'):
      self.bytes(title, targets, width)
      + custom.withFillOpacity(10)
      + $.utils.hideExceptNames(ts, ['used', 'committed', 'allocated'])
      + ts.panelOptions.withRepeat(repeat_label)
      + ts.panelOptions.withRepeatDirection('h')
      + (
        if $.utils.isPreV11() then {}
        else ts.panelOptions.withMaxPerRow(maxPerRow)
      )
      + ts.standardOptions.withOverridesMixin([
        ts.fieldOverride.byName.new('allocated')
        + ts.fieldOverride.byName.withProperty('unit', 'Bps')
        + ts.fieldOverride.byName.withProperty('custom.fillOpacity', 0),
      ]),

    latency(title, targets, width=null, unit='s'):
      self.base(title, targets, width)
      + ts.standardOptions.withMin(0)
      + ts.standardOptions.withUnit(unit)
      + custom.withSpanNulls(false),

    percentiles(title, targets, width=null):
      self.base(title, targets, width)
      + $.utils.hideExceptNames(ts, ['p50', 'p90', 'p95', 'p99'])
      + ts.standardOptions.withMin(0)
      + ts.standardOptions.withUnit('s')
      + ts.standardOptions.withDecimals(null)
      + custom.withFillOpacity(0)
      + custom.withSpanNulls(false)
      + custom.withShowPoints('auto')
      + ts.standardOptions.withOverridesMixin([
        ts.fieldOverride.byName.new('p99')
        + ts.fieldOverride.byName.withProperty('color', { fixedColor: 'red', mode: 'fixed' }),
        ts.fieldOverride.byName.new('p95')
        + ts.fieldOverride.byName.withProperty('color', { fixedColor: 'orange', mode: 'fixed' }),
        ts.fieldOverride.byName.new('p90')
        + ts.fieldOverride.byName.withProperty('color', { fixedColor: 'yellow', mode: 'fixed' }),
        ts.fieldOverride.byName.new('p50')
        + ts.fieldOverride.byName.withProperty('color', { fixedColor: 'green', mode: 'fixed' }),
        ts.fieldOverride.byName.new('throughput')
        + ts.fieldOverride.byName.withProperty('unit', 'ops')
        + ts.fieldOverride.byName.withProperty('custom.lineWidth', 2)
        + ts.fieldOverride.byName.withProperty('color', { fixedColor: 'text', mode: 'fixed' }),
      ]),

    withLegend(showLegend=true, displayMode='list', placement='bottom', calcs=[]):
      ts.options.legend.withShowLegend(showLegend)
      + ts.options.legend.withCalcs(calcs)
      + ts.options.legend.withDisplayMode(displayMode)
      + ts.options.legend.withPlacement(placement),

    cpu(title, targets, width=null):
      this.base(title, targets, width)
      + ts.standardOptions.withUnit('percent')
      + ts.standardOptions.withDecimals(null)
      + options.legend.withDisplayMode('table')
      + options.legend.withPlacement('right')
      + options.legend.withCalcs(['max', 'mean', 'lastNotNull'])
      + custom.withSpanNulls(false),

    cpuStacked(title, targets, width=null):
      self.cpu(title, targets, width)
      + custom.withStacking({ mode: 'normal' })
      + custom.withFillOpacity(50),

    cpuThrottling(title, targets, width=null):
      this.base(title, targets, width)
      + ts.standardOptions.withUnit('cores')
      + ts.standardOptions.withMin(null)
      + ts.standardOptions.withDecimals(null),

    io(title, targets, unit='Bps', width=null):
      this.base(title, targets, width)
      + ts.standardOptions.withUnit(unit)
      + ts.standardOptions.withMin(null),
  },

  statusHistory: {
    local sh = g.panel.statusHistory,
    local custom = sh.fieldConfig.defaults.custom,
    local options = sh.options,

    base(title, targets, width=null):
      sh.new(title)
      + sh.queryOptions.withTargets(targets)
      + sh.queryOptions.withInterval('10s')
      + sh.panelOptions.withGridPos(w=width),

    health(title, targets, width=null):
      self.base(title, targets, width)
      + sh.standardOptions.withMax(1)
      + sh.standardOptions.withMin(0)
      + sh.standardOptions.withNoValue('0')
      + sh.standardOptions.withMappings([
        (
          if $.utils.isPreV11() then sh.standardOptions.mapping.ValueMap.withType('value')
          else sh.standardOptions.mapping.ValueMap.withType()
        )
        + sh.standardOptions.mapping.ValueMap.withOptions({
          '0': { text: 'inactive', index: 1 },
          '1': { text: 'active', index: 0 },
        }),
      ])
      + sh.standardOptions.thresholds.withSteps([
        sh.standardOptions.threshold.step.withColor('dark-red')
        + sh.standardOptions.threshold.step.withValue(null),
        sh.standardOptions.threshold.step.withColor('green')
        + sh.standardOptions.threshold.step.withValue(1),
      ])
      + options.withShowValue('never')
      + options.withRowHeight(0.8)
      + options.withColWidth(1)
      + options.legend.withShowLegend(false)
      + custom.withFillOpacity(71)
      + custom.withLineWidth(0),
  },

  barChart: {
    local bc = g.panel.barChart,
    local options = bc.options,

    base(title, targets, width=null):
      bc.new(title)
      + bc.queryOptions.withTargets(targets)
      + bc.queryOptions.withInterval('10s')
      + bc.panelOptions.withGridPos(w=width),

    throughput(title, targets, width=null, unit='reqps'):
      self.base(title, targets, width)
      + bc.standardOptions.withUnit(unit)
      + bc.fieldConfig.defaults.custom.withAxisSoftMin(0)
      + options.withOrientation('vertical')
      + options.withXTickLabelSpacing(100)
      + options.withStacking('normal')
      + options.legend.withShowLegend(false),

    returnCodes(title, targets, width=null):
      self.base(title, targets, width)
      + bc.standardOptions.withDecimals(0)
      + bc.standardOptions.withMax(1)
      + bc.standardOptions.withMin(0)
      + bc.fieldConfig.defaults.custom.withAxisSoftMin(0)
      + options.withOrientation('vertical')
      + options.withXTickLabelSpacing(-100)
      + options.withStacking('percent')
      + options.withXField('Time'),
  },

  text(title, content, mode='markdown', width=null):
    g.panel.text.new(title)
    + g.panel.text.options.withMode(mode)
    + g.panel.text.options.withContent(content)
    + g.panel.text.panelOptions.withGridPos(w=width),

  gauge(title, targets, width=null, height=null):
    g.panel.gauge.new(title)
    + g.panel.gauge.queryOptions.withTargets(targets)
    + g.panel.gauge.standardOptions.withDecimals(0)
    + g.panel.gauge.panelOptions.withGridPos(w=width, h=height),

  stat(title, targets, width=null, height=null):
    g.panel.stat.new(title)
    + g.panel.stat.queryOptions.withTargets(targets)
    + g.panel.stat.standardOptions.withDecimals(0)
    + g.panel.stat.standardOptions.color.withMode('fixed')
    + g.panel.stat.standardOptions.color.withFixedColor('green')
    + g.panel.stat.options.withGraphMode('none')
    + g.panel.stat.panelOptions.withGridPos(w=width, h=height),

  table: {
    local tb = g.panel.table,
    local tf = tb.queryOptions.transformation,

    settings(title, targets, width=null):
      tb.new(title)
      + tb.queryOptions.withTargets(targets)
      + tb.panelOptions.withGridPos(w=width)
      + tb.options.withShowHeader(false)
      + tb.queryOptions.withTransformations([
        tf.withId('organize')
        + tf.withOptions({
          excludeByName: { Time: true, __name__: true, Value: true },
        }),
        tf.withId('reduce')
        + tf.withOptions({
          reducers: ['last'],
        }),
      ]),

    launchArgs(title, targets, width=null):
      tb.new(title)
      + tb.queryOptions.withTargets(targets)
      + tb.panelOptions.withGridPos(w=width)
      + tb.options.withShowHeader(false)
      + tb.queryOptions.withTransformations([
        tf.withId('organize')
        + tf.withOptions({
          includeByName: { process_command_args: true },
        }),
        tf.withId('extractFields')
        + tf.withOptions({
          source: 'process_command_args',
          format: 'kvp',
          replace: true,
          keepTime: false,
        }),
        tf.withId('reduce')
        + tf.withOptions({
          reducers: ['last'],
          labelsToFields: false,
        }),
        tf.withId('filterByValue')
        + tf.withOptions({
          filters: [{
            fieldName: 'Field',
            config: {
              id: 'regex',
              options: { value: '--.*' },
            },
          }],
          type: 'include',
          match: 'all',
        }),
        tf.withId('sortBy')
        + tf.withOptions({
          sort: [{
            field: 'Field',
          }],
        }),
      ]),
  },

  heatmap: {
    local hm = g.panel.heatmap,
    local custom = hm.fieldConfig.defaults.custom,
    local options = hm.options,

    base(title, targets, width=null):
      hm.new(title)
      + hm.queryOptions.withTargets(targets)
      + hm.queryOptions.withInterval('10s')
      + hm.panelOptions.withGridPos(w=width)
      + custom.withScaleDistribution('linear')
      + (
        if $.utils.isPreV11() then
          options.color.HeatmapColorOptions.withMode('opacity')
          + options.color.HeatmapColorOptions.withFill('dark-green')
        else
          options.color.withMode('opacity')
          + options.color.withFill('dark-green')
      )
      + options.withLegend({ show: false }),

    latency(title, targets, width=null):
      self.base(title, targets, width)
      + (
        if $.utils.isPreV11() then
          options.color.HeatmapColorOptions.withScheme('Greens')
          + options.color.HeatmapColorOptions.withSteps(128)
        else
          options.color.withScheme('Greens')
          + options.color.withSteps(128)
      )
      + options.yAxis.withAxisPlacement('right')
      + options.yAxis.withUnit('s')
      + options.yAxis.withAxisWidth(60)
      + options.yAxis.withDecimals(0)
      + options.withCellValues({ decimals: 0, unit: 'short' }),
  },

  utils: {
    hideExceptNames(from, excluded=[]):
      from.standardOptions.withOverrides([
        {
          __systemRef: 'hideSeriesFrom',
          matcher: { id: 'byNames', options: { mode: 'exclude', names: excluded, prefix: 'All except:', readOnly: true } },
          properties: [{ id: 'custom.hideFrom', value: { viz: true, legend: false, tooltip: false } }],
        },
      ]),

    isPreV11():
      std.member(
        ['v9.4.0', 'v9.5.0', 'v10.0.0', 'v10.1.0', 'v10.2.0', 'v10.3.0', 'v10.4.0'],
        std.extVar('grafonnet_version')
      ),
  },
}
