// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering

sealed trait OrderingMode extends Product with Serializable {

  /** If `true`, dissemination will use the current topology for the output pull protocol. */
  def isStateTransfer: Boolean = this match {
    case OrderingMode.StateTransfer => true
    case OrderingMode.Consensus => false
  }
}

object OrderingMode {

  case object Consensus extends OrderingMode

  case object StateTransfer extends OrderingMode
}
