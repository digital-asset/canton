// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics
import com.codahale.metrics.Timer
import com.daml.metrics.MetricHandle.Gauge
import com.daml.metrics.{MetricHandle as DamlMetricHandle, MetricName}

import scala.annotation.StaticAnnotation
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe as ru}

sealed trait MetricHandle[T <: metrics.Metric] {
  def name: String
  def metric: T
  def metricType: String // type string used for documentation purposes
}

object MetricHandle {

  trait Factory extends DamlMetricHandle.Factory {
    def loadGauge(
        name: MetricName,
        interval: FiniteDuration,
        timer: Timer,
    ): Gauge[TimedLoadGauge, Double] =
      this.addGauge(name, new TimedLoadGauge(interval, timer), _ => ())

    def refGauge[T](name: MetricName, empty: T): Gauge[RefGauge[T], T] =
      this.addGauge(name, new RefGauge[T](empty), x => x.setReference(None))

    def intGauge(name: MetricName, initial: Integer): Gauge[IntGauge, Integer] =
      this.addGauge(name, new IntGauge(initial), x => x.setValue(initial))
  }

  trait NodeMetrics extends MetricHandle.Factory {
    def dbStorage: DbStorageMetrics
  }
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
    symbols.toSeq.flatMap(
      _.typeSignature.members
        .flatMap { m =>
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
                    case x: DamlMetricHandle[_] =>
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
        }
        .toSeq
    )
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
