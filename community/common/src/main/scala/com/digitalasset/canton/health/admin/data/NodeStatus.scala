// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.data

import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.admin.data.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.util.HasProtoV0
import com.digitalasset.canton.DomainId
import com.google.protobuf.ByteString

import java.time.Duration

sealed trait NodeStatus[+S <: NodeStatus.Status]
    extends PrettyPrinting
    with Product
    with Serializable {
  def trySuccess: S
  def successOption: Option[S]
}

object NodeStatus {

  /** A failure to query the node's status */
  case class Failure(msg: String) extends NodeStatus[Nothing] {
    override def pretty: Pretty[Failure] = prettyOfString(_.msg)
    override def trySuccess: Nothing =
      sys.error(s"Status did not complete successfully. Error: $msg")
    override def successOption: Option[Nothing] = None
  }

  /** A node is running but not yet initialized. */
  case class NotInitialized(active: Boolean) extends NodeStatus[Nothing] {
    override def pretty: Pretty[NotInitialized] = prettyOfClass(param("active", _.active))
    override def trySuccess: Nothing = sys.error(s"Node is not yet initialized.")
    override def successOption: Option[Nothing] = None
  }

  case class Success[S <: Status](status: S) extends NodeStatus[S] {
    override def trySuccess: S = status
    override def pretty: Pretty[Success.this.type] = prettyOfParam(_.status)
    override def successOption: Option[S] = status.some
  }

  trait Status
      extends PrettyPrinting
      with HasProtoV0[v0.NodeStatus.Status]
      with Product
      with Serializable {
    def uid: UniqueIdentifier
    def uptime: Duration
    def ports: Map[String, Port]
    def active: Boolean
    def toProtoV0: v0.NodeStatus.Status // explicitly making it public
  }

  private[data] def portsString(ports: Map[String, Port]): String =
    multiline(ports.map { case (portDescription, port) =>
      s"$portDescription: ${port.unwrap}"
    }.toSeq)
  private[data] def multiline(elements: Seq[String]): String =
    if (elements.isEmpty) "None" else elements.map(el => s"\n\t$el").mkString
}

case class SimpleStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
) extends NodeStatus.Status {
  override def pretty: Pretty[SimpleStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
      ).mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status =
    v0.NodeStatus.Status(
      uid.toProtoPrimitive,
      Some(DurationConverter.toProtoPrimitive(uptime)),
      ports.fmap(_.unwrap),
      ByteString.EMPTY,
      active,
      topologyQueues = Some(topologyQueue.toProtoV0),
    )
}

object SimpleStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[SimpleStatus] =
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(proto.id, "Status.id")
      uptime <- ProtoConverter
        .required("Status.uptime", proto.uptime)
        .flatMap(DurationConverter.fromProtoPrimitive)
      ports <- proto.ports.toList
        .traverse { case (s, i) =>
          Port.create(i).map(p => (s, p))
        }
        .map(_.toMap)
      topology <- ProtoConverter.parseRequired(
        TopologyQueueStatus.fromProto,
        "topologyQueues",
        proto.topologyQueues,
      )
    } yield SimpleStatus(uid, uptime, ports, proto.active, topology)
}

/** Health status of the sequencer component itself.
  * @param isActive implementation specific flag indicating whether the sequencer is active
  */
case class SequencerHealthStatus(isActive: Boolean, details: Option[String] = None)
    extends HasProtoV0[v0.SequencerHealthStatus]
    with PrettyPrinting {
  override def toProtoV0: v0.SequencerHealthStatus = v0.SequencerHealthStatus(isActive, details)
  override def pretty: Pretty[SequencerHealthStatus] = prettyOfClass(
    param("isActive", _.isActive),
    paramIfDefined("details", _.details.map(_.unquoted)),
  )
}

object SequencerHealthStatus {
  def fromProto(
      statusP: v0.SequencerHealthStatus
  ): ParsingResult[SequencerHealthStatus] =
    Right(SequencerHealthStatus(statusP.active, statusP.details))
}

/** Topology manager queue status
  *
  * Status around topology management queues
  * @param manager number of queued commands in the topology manager
  * @param dispatcher number of queued transactions in the dispatcher
  * @param clients number of observed transactions that are not yet effective
  */
case class TopologyQueueStatus(manager: Int, dispatcher: Int, clients: Int)
    extends HasProtoV0[v0.TopologyQueueStatus]
    with PrettyPrinting {
  override def toProtoV0: v0.TopologyQueueStatus =
    v0.TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients)

  def isIdle: Boolean = Seq(manager, dispatcher, clients).forall(_ == 0)

  override def pretty: Pretty[TopologyQueueStatus.this.type] = prettyOfClass(
    param("manager", _.manager),
    param("dispatcher", _.dispatcher),
    param("clients", _.clients),
  )
}

object TopologyQueueStatus {
  def fromProto(
      statusP: v0.TopologyQueueStatus
  ): ParsingResult[TopologyQueueStatus] = {
    val v0.TopologyQueueStatus(manager, dispatcher, clients) = statusP
    Right(TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients))
  }
}

case class DomainStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
) extends NodeStatus.Status {
  val id: DomainId = DomainId(uid)

  // A domain node is not replicated and always active
  override def active: Boolean = true

  override def pretty: Pretty[DomainStatus] =
    prettyOfString(_ =>
      Seq(
        s"Domain id: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
        show"Sequencer: $sequencer",
      ).mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)
    SimpleStatus(uid, uptime, ports, active, topologyQueue).toProtoV0
      .copy(
        extra = v0.DomainStatusInfo(participants, Some(sequencer.toProtoV0)).toByteString
      )
  }
}

object DomainStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[DomainStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      domainStatus <- ProtoConverter
        .parse[DomainStatus, v0.DomainStatusInfo](
          v0.DomainStatusInfo.parseFrom,
          domainStatusInfoP => {
            for {
              participants <- domainStatusInfoP.connectedParticipants.traverse(pId =>
                ParticipantId.fromProtoPrimitive(pId, s"DomainStatus.connectedParticipants")
              )
              sequencer <- ProtoConverter.parseRequired(
                SequencerHealthStatus.fromProto,
                "sequencer",
                domainStatusInfoP.sequencer,
              )

            } yield DomainStatus(
              status.uid,
              status.uptime,
              status.ports,
              participants,
              sequencer,
              status.topologyQueue,
            )
          },
          proto.extra,
        )
    } yield domainStatus
}

case class ParticipantStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedDomains: Map[DomainId, Boolean],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
) extends NodeStatus.Status {
  val id: ParticipantId = ParticipantId(uid)
  override def pretty: Pretty[ParticipantStatus] =
    prettyOfString(_ =>
      Seq(
        s"Participant id: ${id.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected domains: ${multiline(connectedDomains.filter(_._2).map(_._1.toString).toSeq)}",
        s"Unhealthy domains: ${multiline(connectedDomains.filterNot(_._2).map(_._1.toString).toSeq)}",
        s"Active: $active",
      ).mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status = {
    val domains = connectedDomains.map { case (domainId, healthy) =>
      v0.ParticipantStatusInfo.ConnectedDomain(
        domain = domainId.toProtoPrimitive,
        healthy = healthy,
      )
    }.toList
    SimpleStatus(uid, uptime, ports, active, topologyQueue).toProtoV0
      .copy(extra = v0.ParticipantStatusInfo(domains, active).toByteString)
  }
}

object ParticipantStatus {

  private def connectedDomainFromProtoV0(
      proto: v0.ParticipantStatusInfo.ConnectedDomain
  ): ParsingResult[(DomainId, Boolean)] = {
    DomainId.fromProtoPrimitive(proto.domain, s"ParticipantStatus.connectedDomains").map {
      domainId =>
        (domainId, proto.healthy)
    }
  }

  def fromProtoV0(
      proto: v0.NodeStatus.Status
  ): ParsingResult[ParticipantStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      participantStatus <- ProtoConverter
        .parse[ParticipantStatus, v0.ParticipantStatusInfo](
          v0.ParticipantStatusInfo.parseFrom,
          participantStatusInfoP =>
            for {
              connectedDomains <- participantStatusInfoP.connectedDomains.traverse(
                connectedDomainFromProtoV0
              )
            } yield ParticipantStatus(
              status.uid,
              status.uptime,
              status.ports,
              connectedDomains.toMap: Map[DomainId, Boolean],
              participantStatusInfoP.active,
              status.topologyQueue,
            ),
          proto.extra,
        )
    } yield participantStatus
}

case class SequencerNodeStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
) extends NodeStatus.Status {
  override def active: Boolean = sequencer.isActive
  override def toProtoV0: v0.NodeStatus.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)
    SimpleStatus(uid, uptime, ports, active, topologyQueue).toProtoV0.copy(
      extra = v0
        .SequencerNodeStatus(participants, sequencer.toProtoV0.some, domainId.toProtoPrimitive)
        .toByteString
    )
  }

  override def pretty: Pretty[SequencerNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Sequencer id: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"details-extra: ${sequencer.details}",
      ).mkString(System.lineSeparator())
    )
}

object SequencerNodeStatus {
  def fromProtoV0(
      sequencerP: v0.NodeStatus.Status
  ): ParsingResult[SequencerNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(sequencerP)
      sequencerNodeStatus <- ProtoConverter.parse[SequencerNodeStatus, v0.SequencerNodeStatus](
        v0.SequencerNodeStatus.parseFrom,
        sequencerNodeStatusP =>
          for {
            participants <- sequencerNodeStatusP.connectedParticipants.traverse(pId =>
              ParticipantId.fromProtoPrimitive(pId, s"SequencerNodeStatus.connectedParticipants")
            )
            sequencer <- ProtoConverter.parseRequired(
              SequencerHealthStatus.fromProto,
              "sequencer",
              sequencerNodeStatusP.sequencer,
            )
            domainId <- DomainId.fromProtoPrimitive(
              sequencerNodeStatusP.domainId,
              s"SequencerNodeStatus.domainId",
            )
          } yield SequencerNodeStatus(
            status.uid,
            domainId,
            status.uptime,
            status.ports,
            participants,
            sequencer,
            status.topologyQueue,
          ),
        sequencerP.extra,
      )
    } yield sequencerNodeStatus

}

case class MediatorNodeStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
) extends NodeStatus.Status {
  override def pretty: Pretty[MediatorNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
      ).mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status =
    SimpleStatus(uid, uptime, ports, active, topologyQueue).toProtoV0.copy(
      extra = v0
        .MediatorNodeStatus(domainId.toProtoPrimitive)
        .toByteString
    )
}

object MediatorNodeStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[MediatorNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      mediatorNodeStatus <- ProtoConverter.parse[MediatorNodeStatus, v0.MediatorNodeStatus](
        v0.MediatorNodeStatus.parseFrom,
        mediatorNodeStatusP =>
          for {
            domainId <- DomainId.fromProtoPrimitive(
              mediatorNodeStatusP.domainId,
              s"MediatorNodeStatus.domainId",
            )
          } yield MediatorNodeStatus(
            status.uid,
            domainId,
            status.uptime,
            status.ports,
            status.active,
            status.topologyQueue,
          ),
        proto.extra,
      )
    } yield mediatorNodeStatus
}
