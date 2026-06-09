// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.platform.*

sealed trait ContractStateEvent extends Product with Serializable

object ContractStateEvent {
  final case class Activated(
      contractId: ContractId,
      globalKey: Option[Key],
      eventSequentialId: Long,
      isInitial: Boolean, // distinguish between initial activation (created) and re-activation (assigned)
  ) extends ContractStateEvent
  final case class Deactivated(
      contractId: ContractId,
      globalKey: Option[Key],
      deactivatedEventSequentialId: Long, // event sequential ID of the activated event
      isFinal: Boolean, // distinguish between final deactivation (archived) and non-final deactivation (unassigned)
  ) extends ContractStateEvent
}
