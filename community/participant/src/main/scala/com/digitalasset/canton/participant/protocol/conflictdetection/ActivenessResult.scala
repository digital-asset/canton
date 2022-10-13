// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractKeyJournal}
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey, TransferId}
import pprint.Tree

/** The result of the activeness check for an [[ActivenessSet]].
  * If all sets are empty, the activeness check was successful.
  *
  * @param contracts The contracts whose activeness check has failed
  * @param inactiveTransfers The transfers that shall be completed, but that are not active.
  */
final case class ActivenessResult(
    contracts: ActivenessCheckResult[LfContractId, ActiveContractStore.Status],
    inactiveTransfers: Set[TransferId],
    keys: ActivenessCheckResult[LfGlobalKey, ContractKeyJournal.Status],
) extends PrettyPrinting {

  def isSuccessful: Boolean =
    contracts.isSuccessful && inactiveTransfers.isEmpty && keys.isSuccessful

  override def pretty: Pretty[ActivenessResult] = {
    import ActivenessResult.paramIfNotSuccessful
    prettyOfClass(
      paramIfNotSuccessful("contracts", _.contracts),
      paramIfNonEmpty("inactiveTransfers", _.inactiveTransfers),
      paramIfNotSuccessful("keys", _.keys),
    )
  }

}

object ActivenessResult {
  val success: ActivenessResult =
    ActivenessResult(ActivenessCheckResult.success, Set.empty, ActivenessCheckResult.success)

  private def paramIfNotSuccessful[K, A <: PrettyPrinting](
      name: String,
      getValue: ActivenessResult => ActivenessCheckResult[K, A],
  ): ActivenessResult => Option[Tree] = {
    PrettyUtil.param(name, getValue, !getValue(_).isSuccessful)
  }
}

/** The result of the activeness check for an [[ActivenessCheck]].
  * If all sets are empty, the activeness check was successful.
  *
  * @param alreadyLocked The items that have already been locked at the activeness check.
  * @param notFresh The items that are supposed to not exist, but do.
  * @param notFree The items that shall be free, but are not.
  * @param notActive The contracts that shall be active, but are not.
  */
private[conflictdetection] final case class ActivenessCheckResult[Key, Status <: PrettyPrinting](
    alreadyLocked: Set[Key],
    notFresh: Set[Key],
    unknown: Set[Key],
    notFree: Map[Key, Status],
    notActive: Map[Key, Status],
)(implicit val prettyK: Pretty[Key])
    extends PrettyPrinting {

  def isSuccessful: Boolean =
    alreadyLocked.isEmpty && notFresh.isEmpty && unknown.isEmpty && notFree.isEmpty && notActive.isEmpty

  override def pretty: Pretty[ActivenessCheckResult.this.type] = prettyOfClass(
    paramIfNonEmpty("alreadyLocked", _.alreadyLocked),
    paramIfNonEmpty("notFresh", _.notFresh),
    paramIfNonEmpty("unknown", _.unknown),
    paramIfNonEmpty("notFree", _.notFree),
    paramIfNonEmpty("notActive", _.notActive),
  )

}

private[conflictdetection] object ActivenessCheckResult {
  def success[Key: Pretty, Status <: PrettyPrinting] =
    ActivenessCheckResult[Key, Status](Set.empty, Set.empty, Set.empty, Map.empty, Map.empty)
}
