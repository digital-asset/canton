// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.store.{Sequenced, SequencerMemberId}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Who gets notified that a event has been written */
sealed trait WriteNotification {
  def union(notification: WriteNotification): WriteNotification
  def memberIds: Set[SequencerMemberId]
  def isBroadcast: Boolean
  final def isBroadcastOrIncludes(member: SequencerMemberId): Boolean =
    isBroadcast || memberIds.contains(member)
}

object WriteNotification {

  case object None extends WriteNotification {
    override def union(notification: WriteNotification): WriteNotification = notification
    override def memberIds: Set[SequencerMemberId] = Set.empty
    override def isBroadcast: Boolean = false
  }
  final case class Members(memberIds: Set[SequencerMemberId]) extends WriteNotification {
    override def union(notification: WriteNotification): WriteNotification =
      notification match {
        case Members(newMemberIds) => Members(memberIds ++ newMemberIds)
        case None => this
      }

    override def isBroadcast: Boolean = memberIds.contains(SequencerMemberId.Broadcast)

    override def toString: String = s"Members(${memberIds.map(_.unwrap).mkString(",")})"
  }

  def apply(events: NonEmpty[Seq[Sequenced[?]]]): WriteNotification =
    events
      .map(_.event.notifies)
      .reduceLeft(_ union _)
}

/** Signal that a reader should attempt to read the latest events as some may have been written */
sealed trait ReadSignal
case object ReadSignal extends ReadSignal

/** Component to signal to a [[SequencerReader]] that more events may be available to read so should
  * attempt fetching events from its store.
  */
trait EventSignaller extends AutoCloseable {
  def isLegacySignaller: Boolean
  def notifyOfLocalWrite(notification: WriteNotification): Future[Unit]
  def readSignalsForMember(member: Member, memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): Source[ReadSignal, NotUsed]
}
