// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction.checks

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyTransactionRejection
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{TemplateBoundPartyMapping, TopologyMapping}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Enforces immutability of template-bound party configurations.
  *
  * Once a TemplateBoundPartyMapping is created (initial registration), it cannot
  * be modified. The allowed template set is fixed at creation time. If the party
  * needs different templates, a new party must be created.
  *
  * This is the strongest possible governance: no update mechanism to subvert.
  * The party's signing key is destroyed and its template whitelist is immutable.
  * The template IS the authority, permanently.
  *
  * Rationale: any update mechanism (even governance-gated) introduces a trust
  * assumption. The hosting participant could lobby for a governance vote to add
  * a drain template. Immutability eliminates this attack vector entirely.
  */
class TemplateBoundPartyChecks(implicit ec: ExecutionContext) extends TopologyMappingChecks {

  override def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    toValidate.mapping match {
      case _: TemplateBoundPartyMapping =>
        inStore match {
          case Some(_existingMapping) =>
            // A TemplateBoundPartyMapping already exists for this party.
            // Reject the update — template-bound parties are immutable.
            EitherT.leftT[FutureUnlessShutdown, Unit](
              TopologyTransactionRejection.Other(
                "Template-bound party configurations are immutable. " +
                  "The allowed template set cannot be modified after creation. " +
                  "Create a new template-bound party if different templates are needed."
              )
            )
          case None =>
            // First registration — allow it.
            EitherT.rightT[FutureUnlessShutdown, TopologyTransactionRejection](())
        }
      case _ =>
        // Not a TemplateBoundPartyMapping — pass through.
        EitherT.rightT[FutureUnlessShutdown, TopologyTransactionRejection](())
    }
  }
}
