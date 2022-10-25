// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.MetricHandle.Timer
import com.daml.metrics.{MetricHandle as DamlMetricHandle, MetricName}

import scala.annotation.StaticAnnotation
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe as ru

object MetricHandle {

  trait Factory extends DamlMetricHandle.DropwizardFactory {
    def loadGauge(
        name: MetricName,
        interval: FiniteDuration,
        timer: Timer,
    ): TimedLoadGauge =
      reRegisterGauge[Double, TimedLoadGauge](name, new TimedLoadGauge(interval, timer))

    def refGauge[T](name: MetricName, empty: T): RefGauge[T] =
      reRegisterGauge[T, RefGauge[T]](name, new RefGauge[T](empty))

  }

  trait NodeMetrics extends Factory {
    def dbStorage: DbStorageMetrics
  }
}

object MetricDoc {

  case class Tag(summary: String, description: String) extends StaticAnnotation

  // The GroupTag can be defined for metrics that share similar names and should be grouped using a
  // wildcard (the representative).
  case class GroupTag(representative: String) extends StaticAnnotation

  def toItem(tags: Seq[Tag], groupTags: Seq[GroupTag], x: DamlMetricHandle): Option[Item] =
    (tags, groupTags) match {
      case (List(tag), List()) => Some(Item(tag = tag, name = x.name, metricType = x.metricType))
      case (List(tag), _) => Some(groupTagToItem(tag, groupTags, x))
      case _ => None
    }

  // Converts a Tag accompanied with a GroupTag to a MetricDoc.Item. If the metric's name matches
  // one of the representatives then the item's name is set to be the corresponding representative.
  // The instance is set to equal the part of the metric's name replaced by the wildcard
  // (representative). Otherwise, the item is constructed as if the GroupTag was missing.
  def groupTagToItem(tag: Tag, groupTags: Seq[GroupTag], x: DamlMetricHandle): Item = {
    val wildcard = "<.*>".r // wildcard must be inside angle brackets (<,>)
    val matchingRepresentative = groupTags
      .map(_.representative)
      .find(representative => {
        val escaped = representative.replace(".", "\\.")
        val pattern = wildcard.replaceFirstIn(escaped, ".*")
        pattern.r.matches(x.name)
      })
    matchingRepresentative match {
      case None => Item(tag = tag, name = x.name, metricType = x.metricType, groupingInfo = None)
      case Some(representative) => {
        val startWildcard = representative.indexOf('<')
        val endWildcard = representative.indexOf('>')
        val commonSuffixLength = representative.length - endWildcard - 1
        Item(
          tag = tag,
          name = representative,
          metricType = x.metricType,
          groupingInfo = Some(
            GroupInfo(
              Seq(
                x.name.drop(startWildcard).take(x.name.length - commonSuffixLength - startWildcard)
              )
            )
          ),
        )
      }
    }
  }

  case class GroupInfo(
      instances: Seq[String]
  )

  case class Item(
      tag: Tag,
      name: String,
      metricType: String,
      groupingInfo: Option[GroupInfo] = None,
  )

  // ignore scala / java packages
  private val ignorePackages = Seq("scala.", "java.")
  private def includeSymbol(symbol: ru.Symbol): Boolean =
    !ignorePackages.exists(symbol.fullName.startsWith)

  // Deduplicate Items that have the same name and collect the instances of those into one item
  def deduplicateGroupped(grouped: Seq[Item]): Seq[Item] =
    grouped
      .groupBy(_.name)
      .values
      .map((s: Seq[Item]) =>
        s.headOption
          .map(
            _.copy(groupingInfo =
              Some(
                GroupInfo(s.map(_.groupingInfo).flatMap(_.toList).flatMap(_.instances).distinct)
              )
            )
          )
      )
      .flatMap(_.toList)
      .toSeq

  /** Get MetricDoc.Tag annotated metrics from any instance in a nested way
    *
    * NOTE: does not support lazy val metrics
    */
  def getItems[T: ClassTag](instance: T): Seq[Item] = {
    val (unique, grouped) = getItemsAll[T](instance, Seq()).partition(_.groupingInfo.isEmpty)
    unique ++ deduplicateGroupped(grouped)
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def getItemsAll[T: ClassTag](
      instance: T,
      inheritedGroupTags: Seq[GroupTag],
  ): Seq[Item] = {

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val mirroredType = mirror.reflect(instance)

    // baseClasses includes the entire dependency path, therefore we need to filter that out as otherwise, we get infinite loops
    val symbols = mirroredType.symbol.baseClasses.filter(includeSymbol).toSet
    symbols.toSeq.flatMap(symbol => {
      val classGroupTags =
        if (symbol.isClass) extractTag(symbol.annotations, groupTagParser) else Seq()
      symbol.typeSignature.members.flatMap { m =>
        // do not pick methods
        if (m.isMethod) {
          Seq()
        } else if (m.isModule) {
          // descend into objects nested in classes (which appear as modules)
          val ts = m.asInstanceOf[ru.ModuleSymbol]
          val fm = mirroredType.reflectModule(ts)
          // pass the group tags of the object deeper in the hierarchy
          getItemsAll(fm.instance, inheritedGroupTags ++ classGroupTags)
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
                  case x: DamlMetricHandle =>
                    val tag = extractTag(rf.symbol.annotations, tagParser)
                    val groupTags = extractTag(rf.symbol.annotations, groupTagParser)
                    toItem(tag, inheritedGroupTags ++ classGroupTags ++ groupTags, x).toList
                  // otherwise, continue scanning for metrics
                  case _ =>
                    val fm = rf.get
                    getItemsAll(fm, inheritedGroupTags ++ classGroupTags)
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
      }.toSeq
    })
  }

  private def extractTag[T: ClassTag, S: ru.TypeTag](
      annotations: Seq[ru.Annotation],
      tagParser: ru.Tree => S,
  ): Seq[S] = {
    val filtered = annotations.map(fromAnnotation[S](_, tagParser)).collect({ case Some(s) => s })
    filtered match {
      case a :: b :: rest =>
        a match {
          case a: Tag => throw new IllegalArgumentException(s"Multiple tags observed! $filtered")
          case _ => filtered
        }
      case _ => filtered
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

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def groupTagParser(tree: ru.Tree): GroupTag = {
    try {
      Seq(1).map(
        tree.children(_).asInstanceOf[ru.Literal].value.value.asInstanceOf[String]
      ) match {
        case r :: Nil =>
          GroupTag(representative = r)
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
