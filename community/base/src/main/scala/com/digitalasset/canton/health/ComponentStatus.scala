// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.implicits.catsSyntaxEitherId
import com.digitalasset.canton.*
import com.digitalasset.canton.admin.health.v30 as proto
import com.digitalasset.canton.health.ComponentHealthState.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

/** Simple representation of the health state of a component, easily (de)serializable (from)to
  * protobuf or JSON
  *
  * @param name
  *   name of the component, as registered with the health service
  * @param state
  *   current health state of the component
  * @param labels
  *   optional metadata labels, e.g. [[ComponentStatus.SynchronizerLabelKey]] -> physical
  *   synchronizer id for components that exist once per connected synchronizer
  */
final case class ComponentStatus(
    name: String,
    state: ComponentHealthState,
    labels: Map[String, String],
) extends PrettyPrinting {
  def toProtoV30: proto.ComponentStatus =
    proto.ComponentStatus(
      name = name,
      status = state.toComponentStatusV0,
      labels = labels,
    )

  override protected val pretty: Pretty[ComponentStatus] = ComponentStatus.componentStatusPretty
}

object ComponentStatus {

  /** Label key holding the full physical synchronizer id for components that exist once per
    * connected synchronizer. Used to group per-synchronizer components in status reports.
    */
  val SynchronizerLabelKey: String = "synchronizer"

  /** Renders a synchronizer label for display: full physical synchronizer ids are truncated to
    * their pretty form, anything unparseable is rendered as-is.
    */
  private def displaySynchronizerId(label: String): String =
    topology.PhysicalSynchronizerId
      .fromProtoPrimitive(label, "synchronizer label")
      .fold(_ => label, _.toString)

  /** Display representation of a component status, decoupled from the two `ComponentStatus` classes
    * (this internal one and the console-side one in the admin api client data).
    *
    * @param name
    *   name of the component
    * @param synchronizerLabel
    *   the synchronizer label, if any; labeled entries are rendered in the "Synchronizers:"
    *   section, unlabeled ones as individual top-level lines
    * @param stateText
    *   rendered state, used for entries in the synchronizer section
    * @param fullLine
    *   rendered "name : state" line, used for top-level (unlabeled) entries
    */
  final case class RenderEntry(
      name: String,
      synchronizerLabel: Option[String],
      stateText: String,
      fullLine: String,
  )

  /** Renders component statuses for display, separating node-level components from per-synchronizer
    * components:
    *
    *   - unlabeled entries are rendered individually as "name : state", in input order
    *   - entries labeled with a synchronizer are rendered in a trailing "Synchronizers:" section,
    *     one sub-section per synchronizer (sorted by id), listing each component as "name : state"
    *     in input order.
    */
  def renderGrouped(components: Seq[ComponentStatus]): Seq[String] =
    renderEntries(components.map { component =>
      RenderEntry(
        name = component.name,
        synchronizerLabel = component.labels.get(SynchronizerLabelKey),
        stateText = component.state.toString,
        fullLine = component.toString,
      )
    })

  /** Renders the entries as described in [[renderGrouped]]. */
  def renderEntries(entries: Seq[RenderEntry]): Seq[String] = {
    val sep = System.lineSeparator()

    val (labeled, unlabeled) = entries.partition(_.synchronizerLabel.isDefined)

    val nodeLines = unlabeled.map(_.fullLine)

    val synchronizerSection =
      if (labeled.isEmpty) Seq.empty
      else {
        val bySynchronizer = labeled
          .groupBy(_.synchronizerLabel.getOrElse(""))
          .toSeq
          .sortBy { case (sync, _) => sync }

        val body = bySynchronizer.map { case (sync, comps) =>
          val componentLines =
            comps.map(c => s"$sep\t\t\t${c.name} : ${c.stateText}").mkString
          s"$sep\t\t${displaySynchronizerId(sync)}$componentLines"
        }.mkString

        Seq(s"Synchronizers:$body")
      }

    nodeLines ++ synchronizerSection
  }

  def fromProtoV30(
      dependency: proto.ComponentStatus
  ): ParsingResult[ComponentStatus] =
    dependency.status match {
      case proto.ComponentStatus.Status.Ok(value) =>
        ComponentStatus(
          dependency.name,
          ComponentHealthState.Ok(value.description),
          dependency.labels,
        ).asRight
      case proto.ComponentStatus.Status
            .Degraded(value: proto.ComponentStatus.StatusData) =>
        ComponentStatus(
          dependency.name,
          Degraded(UnhealthyState(value.description)),
          dependency.labels,
        ).asRight
      case proto.ComponentStatus.Status.Failed(value) =>
        ComponentStatus(
          dependency.name,
          Failed(UnhealthyState(value.description)),
          dependency.labels,
        ).asRight
      case _ =>
        ProtoDeserializationError.UnrecognizedField("Unknown state").asLeft
    }

  implicit val componentStatusEncoder: Encoder[ComponentStatus] = deriveEncoder[ComponentStatus]

  implicit val componentStatusPretty: Pretty[ComponentStatus] = {
    import Pretty.*
    prettyInfix[ComponentStatus](_.name.unquoted, ":", _.state)
  }
}
