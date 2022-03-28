// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.data.EitherT
import com.codahale.metrics
import com.codahale.metrics.{Gauge, Timer}
import com.daml.metrics.{MetricName, Timed, VarGauge}

import scala.annotation.StaticAnnotation
import scala.concurrent.{Future, blocking}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

sealed trait MetricHandle[T <: metrics.Metric] {
  def name: String
  def metric: T
  def metricType: String // type string used for documentation purposes
}

object MetricHandle {

  trait Factory {

    def prefix: MetricName

    def registry: metrics.MetricRegistry

    def timer(name: MetricName): TimerM = TimerM(name, registry.timer(name))

    def varGauge[T](name: MetricName, initial: T): GaugeM[VarGauge[T], T] =
      addGauge(name, VarGauge[T](initial), _.updateValue(initial))

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    private def addGauge[T <: Gauge[M], M](
        name: MetricName,
        newGauge: => T,
        resetExisting: (T => Unit),
    ): GaugeM[T, M] = blocking {
      synchronized {
        val res: GaugeM[T, M] = Option(registry.getGauges.get(name: String)) match {
          case Some(existingGauge) => GaugeM(name, existingGauge.asInstanceOf[T])
          case None =>
            val gauge = newGauge
            // This is not idempotent, therefore we need to query first.
            registry.register(name, gauge)
            GaugeM(name, gauge)
        }
        resetExisting(res.metric)
        res
      }
    }

    def loadGauge(
        name: MetricName,
        interval: FiniteDuration,
        timer: Timer,
    ): GaugeM[TimedLoadGauge, Double] =
      addGauge(name, new TimedLoadGauge(interval, timer), _ => ())

    def refGauge[T](name: MetricName, empty: T): GaugeM[RefGauge[T], T] =
      addGauge(name, new RefGauge[T](empty), x => x.setReference(None))

    def intGauge(name: MetricName, initial: Integer): GaugeM[IntGauge, Integer] =
      addGauge(name, new IntGauge(initial), x => x.setValue(initial))

    def meter(name: MetricName): MeterM = {
      // This is idempotent
      MeterM(name, registry.meter(name))
    }

    def counter(name: MetricName): CounterM = {
      // This is idempotent
      CounterM(name, registry.counter(name))
    }

    def histogram(name: MetricName): HistogramM = {
      HistogramM(name, registry.histogram(name))
    }

  }

  trait NodeMetrics extends MetricHandle.Factory {
    def dbStorage: DbStorageMetrics
  }

  case class TimerM(name: String, metric: metrics.Timer) extends MetricHandle[metrics.Timer] {
    def metricType: String = "Timer"

    def timeEitherT[E, A](ev: EitherT[Future, E, A]): EitherT[Future, E, A] = {
      EitherT(Timed.future(metric, ev.value))
    }

  }

  case class GaugeM[U <: metrics.Gauge[T], T](name: String, metric: U)
      extends MetricHandle[metrics.Gauge[T]] {
    def metricType: String = "Gauge"
  }

  case class MeterM(name: String, metric: metrics.Meter) extends MetricHandle[metrics.Meter] {
    def metricType: String = "Meter"
  }

  case class CounterM(name: String, metric: metrics.Counter) extends MetricHandle[metrics.Counter] {
    def metricType: String = "Counter"
  }

  case class HistogramM(name: String, metric: metrics.Histogram)
      extends MetricHandle[metrics.Histogram] {
    def metricType: String = "Histogram"
  }

  type VarGaugeM[T] = GaugeM[VarGauge[T], T]

}

object MetricDoc {

  case class Tag(summary: String, description: String) extends StaticAnnotation

  case class Item(tag: Tag, name: String, metricType: String)

  // ignore scala / java packages
  private val ignorePackages = Seq("scala.", "java.")
  private def includeSymbol(symbol: ru.Symbol): Boolean =
    !ignorePackages.exists(symbol.fullName.startsWith)

  /** Get MetricDoc.Tag annotated metrics from any instance in a nested way
    *
    * NOTE: does not support lazy val metrics
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def getItems[T: ClassTag](instance: T): Seq[Item] = {

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val mirroredType = mirror.reflect(instance)

    // baseClasses includes the entire dependency path, therefore we need to filter that out as otherwise, we get infinite loops
    val symbols = mirroredType.symbol.baseClasses.filter(includeSymbol).toSet
    symbols.toSeq.flatMap(_.typeSignature.members.flatMap { m =>
      // do not pick methods
      if (m.isMethod) {
        Seq()
      } else if (m.isModule) {
        // descend into objects nested in classes (which appear as modules)
        val ts = m.asInstanceOf[ru.ModuleSymbol]
        val fm = mirroredType.reflectModule(ts)
        getItems(fm.instance)
      } else if (m.isTerm && !m.isJava) {
        val ts = m.asInstanceOf[ru.TermSymbol]
        // let's just skip java collections
        if (includeSymbol(ts)) {
          try {
            val rf = mirroredType.reflectField(ts)
            // ignore java symbols
            if (rf.symbol.isJava) {
              Seq()
            } else {
              rf.get match {
                // if it is a metric handle, try to grab the annotation and the name
                case x: MetricHandle[_] =>
                  extractTag(rf.symbol.annotations)
                    .map(tag => Item(tag = tag, name = x.name, metricType = x.metricType))
                    .toList
                // otherwise, continue scanning for metrics
                case _ =>
                  val fm = rf.get
                  getItems(fm)
              }
            }
          } catch {
            // this is dirty, but we'll get quite a few reflection and class loader errors
            // just by scanning our objects, and i haven't figured out a way to prevent these to
            // happen ...
            case _: Throwable => Seq()
          }
        } else Seq()
      } else Seq()
    }.toSeq)
  }

  private def extractTag[T: ClassTag](annotations: Seq[ru.Annotation]): Option[Tag] = {
    annotations.map(fromAnnotation(_, tagParser)).collect({ case Some(s) => s }) match {
      case Nil =>
        None
      case tag :: Nil =>
        Some(tag)
      case x =>
        throw new IllegalArgumentException(s"Multiple tags observed! $x")
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def tagParser(tree: ru.Tree): Tag = {
    try {
      Seq(1, 2).map(
        tree.children(_).asInstanceOf[ru.Literal].value.value.asInstanceOf[String]
      ) match {
        case s :: d :: Nil => Tag(summary = s, description = d.stripMargin)
        case _ => throw new IllegalStateException("Unreachable code.")
      }
    } catch {
      case x: RuntimeException =>
        println(
          "Failed to process description (description needs to be a constant-string. i.e. don't apply stripmargin here ...): " + tree.toString
        )
        throw x
    }
  }

  private def fromAnnotation[T: ru.TypeTag](
      annotation: ru.Annotation,
      parser: ru.Tree => T,
  ): Option[T] = {
    if (annotation.tree.tpe.typeSymbol == ru.typeOf[T].typeSymbol) {
      Some(parser(annotation.tree))
    } else None
  }

}
