// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics
import com.codahale.metrics.{Metric, MetricFilter}
import com.daml.metrics.{JvmMetricSet, MetricName}
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.domain.metrics.{
  DomainMetrics,
  EnvMetrics,
  MediatorNodeMetrics,
  SequencerMetrics,
}
import com.digitalasset.canton.metrics.MetricsConfig.MetricsFilterConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.dropwizard.DropwizardExports

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap

case class MetricsConfig(
    reporters: Seq[MetricsReporterConfig] = Seq.empty,
    reportJvmMetrics: Boolean = false,
)

sealed trait MetricsReporterConfig {
  def filters: Seq[MetricsFilterConfig]

  def metricFilter: MetricFilter =
    (name: String, _: Metric) => filters.isEmpty || filters.exists(_.matches(name))
}

sealed trait MetricsPrefix
object MetricsPrefix {

  /** Do not use a prefix */
  object NoPrefix extends MetricsPrefix

  /** Use a static text string as prefix */
  case class Static(prefix: String) extends MetricsPrefix

  /** Uses the hostname as the prefix */
  object Hostname extends MetricsPrefix

  def prefixFromConfig(prefix: MetricsPrefix): Option[String] = prefix match {
    case Hostname => Some(java.net.InetAddress.getLocalHost.getHostName)
    case NoPrefix => None
    case Static(prefix) => Some(prefix)
  }

}

object MetricsConfig {

  case class JMX(filters: Seq[MetricsFilterConfig] = Seq.empty) extends MetricsReporterConfig

  case class Csv(
      directory: File,
      interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
  ) extends MetricsReporterConfig

  case class Graphite(
      address: String = "localhost",
      port: Int = 2003,
      prefix: MetricsPrefix = MetricsPrefix.Hostname,
      interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
  ) extends MetricsReporterConfig

  case class Prometheus(address: String = "localhost", port: Int = 9100)
      extends MetricsReporterConfig {
    override def filters: Seq[MetricsFilterConfig] = Seq.empty
  }

  case class MetricsFilterConfig(
      startsWith: String = "",
      contains: String = "",
      endsWith: String = "",
  ) {
    def matches(name: String): Boolean =
      name.startsWith(startsWith) && name.contains(contains) && name.endsWith(endsWith)
  }
}

case class MetricsFactory(
    reporters: Seq[metrics.Reporter],
    registry: metrics.MetricRegistry,
    reportJVMMetrics: Boolean,
) extends AutoCloseable {

  private val envMetrics = new EnvMetrics(registry)
  private val participants = TrieMap[String, ParticipantMetrics]()
  private val domains = TrieMap[String, DomainMetrics]()
  private val sequencers = TrieMap[String, SequencerMetrics]()
  private val mediators = TrieMap[String, MediatorNodeMetrics]()
  private val allNodeMetrics: Seq[TrieMap[String, _]] =
    Seq(participants, domains, sequencers, mediators)
  private def nodeMetricsExcept(toExclude: TrieMap[String, _]): Seq[TrieMap[String, _]] =
    allNodeMetrics filterNot (_ eq toExclude)

  object benchmark extends MetricsGroup(MetricName(MetricsFactory.prefix :+ "benchmark"), registry)

  object health extends HealthMetrics(MetricName(MetricsFactory.prefix :+ "health"), registry)

  // add default, system wide metrics to the metrics reporter
  if (reportJVMMetrics) {
    registry.registerAll(new JvmMetricSet) // register Daml repo JvmMetricSet
  }

  private def newRegistry(prefix: String): metrics.MetricRegistry = {
    val nested = new metrics.MetricRegistry()
    registry.register(prefix, nested)
    nested
  }

  def forParticipant(name: String): ParticipantMetrics = {
    participants.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "participant", participants)
        new ParticipantMetrics(MetricsFactory.prefix, newRegistry(metricName))
      },
    )
  }

  def forJvm: EnvMetrics = envMetrics

  def forDomain(name: String): DomainMetrics = {
    domains.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "domain", domains)
        new DomainMetrics(MetricsFactory.prefix, newRegistry(metricName))
      },
    )
  }

  def forSequencer(name: String): SequencerMetrics = {
    sequencers.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "sequencer", sequencers)
        new SequencerMetrics(MetricsFactory.prefix, newRegistry(metricName))
      },
    )
  }

  def forMediator(name: String): MediatorNodeMetrics = {
    mediators.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "mediator", mediators)
        new MediatorNodeMetrics(MetricsFactory.prefix, newRegistry(metricName))
      },
    )
  }

  /** de-duplicate name if there is someone using the same name for another type of node (not sure that will ever happen)
    */
  private def deduplicateName(
      name: String,
      nodeType: String,
      nodesToExclude: TrieMap[String, _],
  ): String =
    if (nodeMetricsExcept(nodesToExclude).exists(_.keySet.contains(name)))
      s"$nodeType-$name"
    else name

  /** returns the documented metrics by possibly creating fake participants / domains */
  def metricsDoc(): (Seq[MetricDoc.Item], Seq[MetricDoc.Item]) = {
    def sorted(lst: Seq[MetricDoc.Item]): Seq[MetricDoc.Item] =
      lst
        .groupBy(_.name)
        .flatMap(_._2.headOption.toList)
        .toSeq
        .sortBy(_.name)

    val participantMetrics: ParticipantMetrics =
      participants.headOption.map(_._2).getOrElse(forParticipant("dummyParticipant"))
    val participantItems = MetricDoc.getItems(participantMetrics)
    val clientMetrics =
      MetricDoc.getItems(participantMetrics.domainMetrics(DomainAlias.tryCreate("<domain>")))
    val domainMetrics = MetricDoc.getItems(
      domains.headOption
        .map { case (_, domainMetrics) => domainMetrics }
        .getOrElse(forDomain("dummyDomain"))
    )

    // the fake instances are fine here as we do this anyway only when we build and export the docs
    (sorted(participantItems ++ clientMetrics), sorted(domainMetrics))
  }

  override def close(): Unit = reporters.foreach(_.close())

}

object MetricsFactory extends LazyLogging {

  import MetricsConfig.*

  val prefix: MetricName = MetricName("canton")

  def forConfig(config: MetricsConfig): MetricsFactory = {
    val registry = new metrics.MetricRegistry()
    val reporter = registerReporter(config, registry)
    new MetricsFactory(reporter, registry, config.reportJvmMetrics)
  }

  private def registerReporter(
      config: MetricsConfig,
      registry: metrics.MetricRegistry,
  ): Seq[metrics.Reporter] = {
    config.reporters.map {

      case reporterConfig @ JMX(_filters) =>
        val reporter =
          metrics.jmx.JmxReporter.forRegistry(registry).filter(reporterConfig.metricFilter).build()
        logger.debug("Starting metrics reporting using JMX")
        reporter.start()
        reporter

      case reporterConfig @ Csv(directory, interval, _filters) =>
        directory.mkdirs()
        logger.debug(s"Starting metrics reporting to csv-file ${directory.toString}")
        val reporter = metrics.CsvReporter
          .forRegistry(registry)
          .filter(reporterConfig.metricFilter)
          .formatFor(Locale.ENGLISH) // Format decimal numbers like "12345.12345".
          .build(directory)
        reporter.start(interval.unwrap.toMillis, TimeUnit.MILLISECONDS)
        reporter

      case reporterConfig @ Graphite(address, port, prefix, interval, _filters) =>
        logger.debug(s"Starting metrics reporting for Graphite to $address:$port")
        val builder = metrics.graphite.GraphiteReporter
          .forRegistry(registry)
          .filter(reporterConfig.metricFilter)
        val reporter = MetricsPrefix
          .prefixFromConfig(prefix)
          .fold(builder)(str => builder.prefixedWith(str))
          .build(new metrics.graphite.Graphite(address, port))
        reporter.start(interval.unwrap.toMillis, TimeUnit.MILLISECONDS)
        reporter

      case Prometheus(hostname, port) =>
        logger.debug(s"Exposing metrics for Prometheus on port $hostname:$port")
        new DropwizardExports(registry).register[DropwizardExports]()
        val reporter = new Reporters.Prometheus(hostname, port)
        reporter
    }
  }
}

class HealthMetrics(prefix: MetricName, registry: metrics.MetricRegistry)
    extends MetricsGroup(prefix, registry) {

  val pingLatency: metrics.Timer = timer("ping-latency")

}

abstract class MetricsGroup(prefix: MetricName, registry: metrics.MetricRegistry) {

  def timer(name: String): metrics.Timer = registry.timer(MetricName(prefix :+ name))

}
