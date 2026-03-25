// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.WriteNotification.{All, Members}
import com.digitalasset.canton.synchronizer.sequencer.store.{Sequenced, SequencerMemberId}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.annotation.tailrec

/** Who gets notified that a event has been written */
sealed trait WriteNotification extends Product with Serializable {
  final def union(notification: WriteNotification): WriteNotification = (this, notification) match {
    case (Members(thisMembers), Members(thatMembers)) => Members(thisMembers ++ thatMembers)
    case (All, _) | (_, All) => All
    case (WriteNotification.NoTarget, those) => those
    case (these, WriteNotification.NoTarget) => these
  }
  def isBroadcast: Boolean
}

object WriteNotification {

  case object NoTarget extends WriteNotification {
    override def isBroadcast: Boolean = false
  }

  final case class Members private[WriteNotification] (memberIds: NonEmpty[Set[SequencerMemberId]])
      extends WriteNotification {
    override def isBroadcast: Boolean = false
    override def toString: String = s"Members(${memberIds.map(_.unwrap).mkString(",")})"
  }

  case object All extends WriteNotification {
    override def isBroadcast: Boolean = true
  }

  @VisibleForTesting
  def forMemberIds(members: NonEmpty[Set[SequencerMemberId]]): WriteNotification =
    if (members.contains(SequencerMemberId.Broadcast)) WriteNotification.All
    else WriteNotification.Members(members)

  def forEvents(events: NonEmpty[Seq[Sequenced[?]]]): WriteNotification = {
    val iter = events.iterator
    @tailrec
    def go(notification: WriteNotification): WriteNotification =
      if (iter.hasNext) {
        val sequenced = iter.next()
        val newNotification = forMemberIds(sequenced.event.notifies)
        if (newNotification.isBroadcast) newNotification
        else go(newNotification.union(notification))
      } else notification

    go(NoTarget)
  }
}

/** Signal that a reader should attempt to read the latest events as some may have been written */
sealed trait ReadSignal
case object ReadSignal extends ReadSignal

/** Component to signal to a [[SequencerReader]] that more events may be available to read so should
  * attempt fetching events from its store.
  */
trait EventSignaller extends AutoCloseable {
  def notifyOfLocalWrite(notification: WriteNotification): Unit
  def readSignalsForMember(member: Member, memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): Source[ReadSignal, NotUsed]
}
