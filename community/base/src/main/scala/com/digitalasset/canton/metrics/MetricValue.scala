// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.metrics.data.*

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

sealed trait MetricValue extends PrettyPrintingFromCompanion {

  def attributes: Map[String, String]

  def toCsvHeader(data: MetricData): String
  def toCsvRow(ts: CantonTimestamp, data: MetricData, unknownKeys: Seq[String]): String

  final def select[TargetType <: MetricValue](implicit
      M: ClassTag[TargetType]
  ): Option[TargetType] = M.unapply(this)

  protected def renderUnknownKeys(unknownKeys: Seq[String]): String =
    unknownKeys
      .flatMap(k => attributes.get(k).map(v => s"$k=$v"))
      .mkString(";")
}

object MetricValue {

  def allFromMetricData(items: Iterable[MetricData]): Seq[(MetricValue, MetricData)] =
    items.flatMap { data =>
      MetricValue.fromMetricData(data).map(value => (value, data))
    }.toSeq
  def fromMetricData(item: MetricData): Seq[MetricValue] =
    item.getType match {
      case MetricDataType.LONG_GAUGE =>
        item.getLongGaugeData.getPoints.asScala.map(fromLongPoint).toSeq
      case MetricDataType.DOUBLE_GAUGE =>
        item.getDoubleGaugeData.getPoints.asScala.map(fromDoublePoint).toSeq
      case MetricDataType.LONG_SUM => item.getLongSumData.getPoints.asScala.map(fromLongPoint).toSeq
      case MetricDataType.DOUBLE_SUM =>
        item.getDoubleSumData.getPoints.asScala.map(fromDoublePoint).toSeq
      case MetricDataType.SUMMARY =>
        item.getSummaryData.getPoints.asScala.map(fromSummaryValue).toSeq
      case MetricDataType.HISTOGRAM =>
        item.getHistogramData.getPoints.asScala.map(fromHistogramData).toSeq
      case MetricDataType.EXPONENTIAL_HISTOGRAM =>
        item.getExponentialHistogramData.getPoints.asScala
          .map(c => fromExponentialHistogramData(c))
          .toSeq
    }

  import Pretty.*

  import scala.jdk.CollectionConverters.*

  implicit val prettyValueAtPercentile: Pretty[ValueAtQuantile] = prettyOfClass(
    param("percentile", _.getQuantile.toString.unquoted),
    param("value", _.getValue.toString.unquoted),
  )

  implicit val prettyOfAttributes: Pretty[Map[String, String]] = prettyOfClass(
    unnamedParam(_.map { case (k, v) => s"$k=$v".singleQuoted }.toSeq)
  )

  trait Point[T] {
    this: MetricValue =>

    def value: T

    override def toCsvHeader(data: MetricData): String =
      "timestamp,count,attributes"

    override def toCsvRow(ts: CantonTimestamp, data: MetricData, unknownKeys: Seq[String]): String =
      (Seq(ts.getEpochSecond.toString, value.toString) :+ renderUnknownKeys(
        unknownKeys
      ))
        .mkString(",")
  }

  final case class LongPoint(value: Long, attributes: Map[String, String])
      extends MetricValue
      with Point[Long] {
    override def prettyCompanion: PrettyPrintingCompanion[LongPoint] = LongPoint

  }

  object LongPoint extends PrettyPrintingCompanion[LongPoint] {
    override protected val pretty: Pretty[LongPoint] = prettyOfClass(
      param("value", _.value),
      param("attributes", _.attributes),
    )
  }

  final case class DoublePoint(value: Double, attributes: Map[String, String])
      extends MetricValue
      with Point[Double] {
    override def prettyCompanion: PrettyPrintingCompanion[DoublePoint] = DoublePoint

  }

  object DoublePoint extends PrettyPrintingCompanion[DoublePoint] {
    override protected val pretty: Pretty[DoublePoint] = prettyOfClass(
      param("value", _.value.toString.unquoted),
      param("attributes", _.attributes),
    )
  }

  final case class Summary(
      sum: Double,
      count: Long,
      quantiles: Seq[ValueAtQuantile],
      attributes: Map[String, String],
  ) extends MetricValue {
    override def prettyCompanion: PrettyPrintingCompanion[Summary] = Summary

    override def toCsvHeader(data: MetricData): String =
      (Seq("timestamp", "sum", "count") ++ quantiles
        .map(_.getQuantile)
        .map(x => s"p$x%2.0f") :+ "attributes")
        .mkString(",")

    override def toCsvRow(ts: CantonTimestamp, data: MetricData, unknownKeys: Seq[String]): String =
      (Seq(ts.getEpochSecond.toString, sum.toString, count.toString) ++ quantiles.map(
        _.getValue.toString
      ) :+ renderUnknownKeys(unknownKeys))
        .mkString(",")

  }

  object Summary extends PrettyPrintingCompanion[Summary] {
    override protected val pretty: Pretty[Summary] = prettyOfClass(
      param("sum", _.sum.toString.unquoted),
      param("count", _.count),
      param("quantiles", _.quantiles),
      param("attributes", _.attributes),
    )
  }

  final case class Histogram(
      sum: Double,
      count: Long,
      counts: List[Long],
      boundaries: List[Double],
      attributes: Map[String, String],
  ) extends MetricValue {
    def maxBoundary: Double =
      counts.zipAll(boundaries, 0L, Double.MaxValue).foldLeft(0.0) {
        case (acc, (count, boundary)) =>
          if (count > 0L) boundary else acc
      }

    def average: Double = sum / Math.max(count, 1)

    def percentileBoundary(percentile: Double): Double = {
      require(percentile >= 0 && percentile <= 1, "percentile must be between 0 and 1")
      @tailrec
      def go(remaining: List[(Long, Double)], aggregatedCount: Long): Double =
        remaining match {
          case Nil => Double.MaxValue
          case head :: tail =>
            val (count, boundary) = head
            val newCount = aggregatedCount + count
            if (newCount >= percentile * count) boundary else go(tail, newCount)
        }
      go(counts.zipAll(boundaries, 0, Double.MaxValue), 0)
    }

    override def prettyCompanion: PrettyPrintingCompanion[Histogram] = Histogram

    override def toCsvHeader(data: MetricData): String =
      "timestamp,sum,count,attributes"

    override def toCsvRow(ts: CantonTimestamp, data: MetricData, unknownKeys: Seq[String]): String =
      (Seq(ts.getEpochSecond.toString, sum.toString, count.toString) :+ renderUnknownKeys(
        unknownKeys
      ))
        .mkString(",")

  }

  object Histogram extends PrettyPrintingCompanion[Histogram] {
    override protected val pretty: Pretty[Histogram] = prettyOfClass(
      param("sum", _.sum.toString.unquoted),
      param("count", _.count),
      param("counts", _.counts),
      param("boundaries", _.boundaries.map(_.toString.unquoted)),
      param("attributes", _.attributes),
    )
  }

  final case class ExponentialHistogram(
      count: Long,
      sum: Double,
      min: Double,
      max: Double,
      attributes: Map[String, String],
  ) extends MetricValue {

    override def toCsvHeader(data: MetricData): String = "timestamp,sum,count,min,max,attributes"

    override def toCsvRow(ts: CantonTimestamp, data: MetricData, unknownKeys: Seq[String]): String =
      (Seq(
        ts.getEpochSecond.toString,
        sum.toString,
        count.toString,
        min.toString,
        max.toString,
      ) :+ renderUnknownKeys(
        unknownKeys
      ))
        .mkString(",")

    override def prettyCompanion: PrettyPrintingCompanion[ExponentialHistogram] =
      ExponentialHistogram
  }
  private object ExponentialHistogram extends PrettyPrintingCompanion[ExponentialHistogram] {

    override protected val pretty: Pretty[ExponentialHistogram] =
      prettyOfClass(
        param("attributes", _.attributes)
      )
  }

  private def fromLongPoint(data: LongPointData): LongPoint =
    LongPoint(data.getValue, mapAttributes(data.getAttributes))
  private def fromDoublePoint(data: DoublePointData): DoublePoint =
    DoublePoint(data.getValue, mapAttributes(data.getAttributes))

  private def fromSummaryValue(data: SummaryPointData): Summary =
    Summary(
      data.getSum,
      data.getCount,
      data.getValues.asScala.toSeq,
      mapAttributes(data.getAttributes),
    )

  private def fromHistogramData(data: HistogramPointData): Histogram =
    Histogram(
      data.getSum,
      data.getCount,
      data.getCounts.asScala.map(_.longValue()).toList,
      data.getBoundaries.asScala.map(_.doubleValue()).toList,
      mapAttributes(data.getAttributes),
    )

  private def fromExponentialHistogramData(
      data: ExponentialHistogramPointData
  ): ExponentialHistogram =
    ExponentialHistogram(
      data.getCount,
      data.getSum,
      data.getMin,
      data.getMax,
      mapAttributes(data.getAttributes),
    )

  private def mapAttributes(attributes: Attributes): Map[String, String] =
    attributes.asMap().asScala.map { case (k, v) => (k.getKey, v.toString) }.toMap

  implicit val prettyMetricData: Pretty[MetricData] =
    prettyOfClass(
      param("name", _.getName.singleQuoted),
      param("unit", _.getUnit.singleQuoted),
      param("type", _.getType.name().singleQuoted),
      param("values", x => fromMetricData(x)),
    )
}
