// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  LfContractId,
  LfGlobalKey,
  TransferId,
  WithContractHash,
  WithContractMetadata,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.SetsUtil.requireDisjoint

/** Describes the effect of a confirmation request on the active contracts, contract keys, and transfers.
  * Transient contracts appear the following two sets:
  * <ol>
  *   <li>The union of [[creations]] and [[transferIns]]</li>
  *   <li>The union of [[archivals]] or [[transferOuts]]</li>
  * </ol>
  *
  * @param archivals The contracts to be archived, along with their stakeholders. Must not contain contracts in [[transferOuts]].
  * @param creations The contracts to be created.
  * @param transferOuts The contracts to be transferred out, along with their target domains and stakeholders.
  *                     Must not contain contracts in [[archivals]].
  * @param transferIns The contracts to be transferred in, along with their transfer IDs.
  * @param keyUpdates The contract keys with their new state.
  * @throws java.lang.IllegalArgumentException if `transferOuts` overlap with `archivals`
  *                                            or `creations` overlaps with `transferIns`.
  */
final case class CommitSet(
    archivals: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    creations: Map[LfContractId, WithContractHash[ContractMetadata]],
    transferOuts: Map[LfContractId, WithContractHash[(DomainId, Set[LfPartyId])]],
    transferIns: Map[LfContractId, WithContractHash[WithContractMetadata[TransferId]]],
    keyUpdates: Map[LfGlobalKey, ContractKeyJournal.Status],
) extends PrettyPrinting {
  // In a request by a malicious submitter,
  // creations may overlap with transferIns because transfer-in requests can be batched with a confirmation request.
  // A transfer-out request cannot be batched with a confirmation request, though.
  // TODO(M40): Once transfer-ins can be batched with a confirmation request,
  //  check that we emit a sensible warning about malicious behaviour.
  requireDisjoint(transferOuts.keySet -> "Transfer-outs", archivals.keySet -> "archivals")
  requireDisjoint(transferIns.keySet -> "Transfer-ins", creations.keySet -> "creations")

  override def pretty: Pretty[CommitSet] = prettyOfClass(
    paramIfNonEmpty("archivals", _.archivals),
    paramIfNonEmpty("creations", _.creations),
    paramIfNonEmpty("transfer outs", _.transferOuts),
    paramIfNonEmpty("transfer ins", _.transferIns.fmap(_.unwrap.unwrap)),
    paramIfNonEmpty("key updates", _.keyUpdates),
  )
}

object CommitSet {
  val empty = CommitSet(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}
