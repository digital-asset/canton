// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey, TransferId}
import com.digitalasset.canton.util.SetsUtil.requireDisjoint

/** Defines the contracts and transfers for conflict detection.
  * Transfers are not locked because the transferred contracts are already being locked.
  */
final case class ActivenessSet(
    contracts: ActivenessCheck[LfContractId],
    transferIds: Set[TransferId],
    keys: ActivenessCheck[LfGlobalKey],
) extends PrettyPrinting {

  override def pretty: Pretty[ActivenessSet] = prettyOfClass(
    param("contracts", _.contracts),
    paramIfNonEmpty("transferIds", _.transferIds),
    param("keys", _.keys),
  )
}

object ActivenessSet {
  val empty: ActivenessSet =
    ActivenessSet(
      ActivenessCheck.empty[LfContractId],
      Set.empty,
      ActivenessCheck.empty[LfGlobalKey],
    )
}

/** Defines the activeness checks and locking for one kind of states (contracts, keys, ...)
  *
  * [[ActivenessCheck.checkFresh]], [[ActivenessCheck.checkFree]], and [[ActivenessCheck.checkActive]]
  * must be pairwise disjoint.
  *
  * @throws java.lang.IllegalArgumentException if [[ActivenessCheck.checkFresh]], [[ActivenessCheck.checkFree]],
  *                                            and [[ActivenessCheck.checkActive]] are not pairwise disjoint.
  */
private[participant] final case class ActivenessCheck[Key](
    checkFresh: Set[Key],
    checkFree: Set[Key],
    checkActive: Set[Key],
    lock: Set[Key],
)(implicit val prettyK: Pretty[Key])
    extends PrettyPrinting {

  requireDisjoint(checkFresh -> "fresh", checkFree -> "free")
  requireDisjoint(checkFresh -> "fresh", checkActive -> "active")
  requireDisjoint(checkFree -> "free", checkActive -> "active")

  val lockOnly: Set[Key] = lock -- checkFresh -- checkFree -- checkActive

  override def pretty: Pretty[ActivenessCheck.this.type] = prettyOfClass(
    paramIfNonEmpty("fresh", _.checkFresh),
    paramIfNonEmpty("free", _.checkFree),
    paramIfNonEmpty("active", _.checkActive),
    paramIfNonEmpty("lock", _.lock),
  )
}

private[participant] object ActivenessCheck {
  def empty[Key: Pretty]: ActivenessCheck[Key] =
    ActivenessCheck(Set.empty, Set.empty, Set.empty, Set.empty)
}
