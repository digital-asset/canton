// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, WithContractHash}
import com.digitalasset.canton.tracing.TraceContext

/** Components that need to keep a running snapshot of ACS.
  */
trait AcsChangeListener {

  /** ACS change notification. Any response logic needs to happen in the background. The ACS change set may be empty,
    * (e.g., in case of time proofs).
    *
    * @param toc time of the change
    * @param acsChange active contract set change descriptor
    */
  def publish(toc: RecordTime, acsChange: AcsChange)(implicit traceContext: TraceContext): Unit

}

/** Represents a change to the ACS. The deactivated contracts are accompanied by their stakeholders.
  *
  * Note that we include both the LfContractId (for uniqueness) and the LfHash (reflecting contract content).
  */
final case class AcsChange(
    activations: Map[LfContractId, WithContractHash[ContractMetadata]],
    deactivations: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
)

object AcsChange {
  val empty: AcsChange = AcsChange(Map.empty, Map.empty)

  // TODO(i12904) The ACS commitments processor expects the caller to ensure that the activations/deactivations passed to
  //  it really describe a set of contracts. Double activations or double deactivations for a contract (due to a bug
  //  or maliciousness) will violate this expectation.
  //  Examples of malicious cases we need to handle:
  //    1. archival without a prior create
  //    2. archival followed by a create
  //    3. if we have a double archive as in "create -> archive -> archive",
  //  We should define a sensible semantics for non-repudation in all such cases.
  def fromCommitSet(commitSet: CommitSet): AcsChange = {
    val activations =
      commitSet.creations.fmap(_.map(_.contractMetadata)) ++ commitSet.transferIns.fmap(
        _.map(_.contractMetadata)
      )
    val deactivations = commitSet.archivals ++ commitSet.transferOuts.fmap(_.map(_.stakeholders))
    val transient = activations.keySet.intersect(deactivations.keySet)
    AcsChange(
      activations = activations -- transient,
      deactivations = deactivations -- transient,
    )
  }
}
