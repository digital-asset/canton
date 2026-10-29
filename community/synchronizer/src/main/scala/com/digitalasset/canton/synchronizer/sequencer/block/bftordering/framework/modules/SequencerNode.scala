// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest

object SequencerNode {

  sealed trait Message extends Product

  final case object RequestAccepted extends Message

  final case class RequestRejected(reason: RequestRejected.Reason) extends Message

  object RequestRejected {
    sealed trait Reason extends Product {
      def rejectionMessage: String
    }

    final case class P2PNotReady(authenticated: Int, disseminationQuorum: Int) extends Reason {
      override def rejectionMessage =
        s"P2P connectivity is not ready (authenticated = $authenticated < dissemination quorum = $disseminationQuorum), rejecting"
    }

    case object Blacklisted extends Reason {
      override def rejectionMessage =
        s"Mempool received client request but this node is currently blacklisted, rejecting"
    }

    final case class QueueFull(maxQueueSize: Int) extends Reason {
      override def rejectionMessage =
        s"Mempool received client request but the queue is full ($maxQueueSize), rejecting"
    }

    final case class InvalidTag(tag: String) extends Reason {
      override def rejectionMessage =
        s"Mempool received a client request with an invalid tag '$tag', " +
          s"valid tags are: (${OrderingRequest.ValidTags.mkString(", ")}), rejecting"
    }

    final case class MaxRequestSizeExceeded(size: Int, maxSize: Int) extends Reason {
      override def rejectionMessage =
        s"Mempool received client request of size $size " +
          s"but it exceeds the maximum ($maxSize), rejecting"
    }
  }

  sealed trait SnapshotMessage extends Message
  object SnapshotMessage {
    final case class AdditionalInfo(info: v30.BftSequencerSnapshotAdditionalInfo)
        extends SnapshotMessage

    final case class AdditionalInfoRetrievalError(
        errorMessage: String
    ) extends SnapshotMessage
  }
}
