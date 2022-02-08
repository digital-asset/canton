// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.console.{DomainReference, InstanceReference, ParticipantReference}
import com.digitalasset.canton.health.admin.data.{DomainStatus, NodeStatus, ParticipantStatus}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

trait CantonStatus extends PrettyPrinting {

  protected def descriptions[Ref <: InstanceReference](
      statusMap: Map[Ref, Ref#Status],
      failureMap: Map[Ref, NodeStatus.Failure],
  ): Seq[String] = {

    val success = sort(statusMap)
      .map { case (d, status) =>
        show"Status for $d:\n$status"
      }

    val failure = sort(failureMap)
      .map { case (d, msg) =>
        show"$d cannot be reached: $msg"
      }

    success ++ failure
  }

  private def sort[K <: InstanceReference, V](status: Map[K, V]): Seq[(K, V)] =
    status.toSeq.sortBy(_._1.name)
}

case class CommunityCantonStatus(
    domainStatus: Map[DomainReference, DomainStatus],
    unreachableDomains: Map[DomainReference, NodeStatus.Failure],
    participantStatus: Map[ParticipantReference, ParticipantStatus],
    unreachableParticipants: Map[ParticipantReference, NodeStatus.Failure],
) extends CantonStatus {
  def tupled: (Map[DomainReference, DomainStatus], Map[ParticipantReference, ParticipantStatus]) =
    (domainStatus, participantStatus)

  override def pretty: Pretty[CommunityCantonStatus] = prettyOfString { _ =>
    val domains = descriptions[DomainReference](domainStatus, unreachableDomains)
    val participants =
      descriptions[ParticipantReference](participantStatus, unreachableParticipants)
    (domains ++ participants).mkString(System.lineSeparator() * 2)
  }
}
