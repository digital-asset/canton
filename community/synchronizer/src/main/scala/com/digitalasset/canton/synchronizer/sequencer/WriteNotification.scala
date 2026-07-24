// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.synchronizer.sequencer.store.{Sequenced, SequencerMemberId}
import com.digitalasset.canton.util.signalling.Notification
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting

import scala.annotation.tailrec

object WriteNotification {

  @VisibleForTesting
  def forMemberIds(members: NonEmpty[Set[SequencerMemberId]]): WriteNotification =
    if (members.contains(SequencerMemberId.Broadcast)) Notification.all
    else Notification.Keys(members)

  def forEvents(events: NonEmpty[Seq[Sequenced[?]]]): WriteNotification = {
    val iter = events.iterator
    @tailrec
    def go(notification: WriteNotification): WriteNotification =
      if (iter.hasNext) {
        val sequenced = iter.next()
        val newNotification = forMemberIds(sequenced.event.notifies)
        if (newNotification.isBroadcast) newNotification
        else go(Notification.union(newNotification, notification))
      } else notification

    go(Notification.noTarget)
  }
}
