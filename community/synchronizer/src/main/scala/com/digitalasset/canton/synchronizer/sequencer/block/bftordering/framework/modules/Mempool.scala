// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.WriteReadiness
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.Traced

object Mempool {

  sealed trait Message extends Product

  final case object Start extends Message

  // From clients
  final case class OrderRequest(
      tx: Traced[OrderingRequest],
      from: Option[ModuleRef[SequencerNode.Message]] = None,
      // Only used for metrics, not populated by unit and simulation tests
      sender: Option[Member] = None,
      // The maximum sequencing time of the underlying request, if known, so that the mempool can
      //  discard expired requests without having to deserialize the payload. Not set for requests
      //  without a well-defined max sequencing time, i.e. acknowledgement requests.
      maxSequencingTime: Option[CantonTimestamp] = None,
  ) extends Message

  // From local availability
  final case class CreateLocalBatches(atMost: Short) extends Message

  // From local output module, allows the mempool to discard queued requests whose
  // max sequencing time has passed
  final case class LatestKnownSequencingTimeUpdate(latestKnownSequencingTime: CantonTimestamp)
      extends Message

  final case object MempoolBatchCreationClockTick extends Message

  // From local P2P output module
  final case class P2PConnectivityUpdate(
      membership: Membership,
      authenticatedCountIncludingSelf: Int,
  ) extends Message
      with PrettyPrinting {

    override protected def pretty: Pretty[P2PConnectivityUpdate] =
      prettyOfClass(
        param("membership", _.membership),
        param("authenticatedCountIncludingSelf", _.authenticatedCountIncludingSelf),
      )
  }

  sealed trait Admin extends Message
  object Admin {
    final case class GetWriteReadiness(reply: WriteReadiness => Unit) extends Admin
  }
}

trait Mempool[E <: Env[E]] extends Module[E, Mempool.Message] {
  def availability: ModuleRef[Availability.Message[E]]
}
