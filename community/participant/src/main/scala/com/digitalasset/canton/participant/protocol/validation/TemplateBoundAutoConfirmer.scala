// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Determines whether a transaction can be auto-confirmed on behalf of
  * template-bound parties, and performs the structural template validation.
  *
  * Integration point: called from ProtocolProcessor during confirmation
  * response construction. If all template-bound parties in the transaction
  * pass validation, the participant auto-confirms on their behalf without
  * requiring their (destroyed) signing key.
  *
  * The auto-confirmation replaces the cryptographic check (valid signature
  * from party) with a structural check (all actions on allowed templates).
  */
class TemplateBoundAutoConfirmer(
    override val loggerFactory: NamedLoggerFactory
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Check all parties in the transaction and auto-confirm for template-bound ones.
    *
    * @param involvedParties parties that are signatories/controllers in this transaction
    * @param actionTemplateIdsByParty for each party, the template IDs of actions where
    *                                  that party is a signatory/controller
    * @param topologySnapshot the topology at the transaction's timestamp
    * @return Left with error if any template-bound party violates its template constraints.
    *         Right with the set of parties that were auto-confirmed.
    */
  def checkAndAutoConfirm(
      involvedParties: Set[LfPartyId],
      actionTemplateIdsByParty: Map[LfPartyId, Set[String]],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[TemplateBoundPartyValidator.Invalid, Set[LfPartyId]]] = {
    import cats.syntax.parallel.*

    // Look up which parties are template-bound
    involvedParties.toList
      .parTraverse { party =>
        topologySnapshot.templateBoundPartyConfig(party).map(party -> _)
      }
      .map { partyConfigs =>
        val templateBound = partyConfigs.collect { case (party, Some(config)) =>
          party -> config
        }

        // Validate each template-bound party
        val validationResults = templateBound.map { case (party, config) =>
          val templateIds = actionTemplateIdsByParty.getOrElse(party, Set.empty)
          TemplateBoundPartyValidator.validate(party, config, templateIds)
        }

        // Check for any failures
        validationResults.collectFirst { case invalid: TemplateBoundPartyValidator.Invalid =>
          invalid
        } match {
          case Some(invalid) => Left(invalid)
          case None =>
            val autoConfirmedParties = templateBound.map(_._1).toSet
            if (autoConfirmedParties.nonEmpty) {
              logger.info(
                s"Auto-confirmed ${autoConfirmedParties.size} template-bound parties: " +
                  s"${autoConfirmedParties.mkString(", ")}"
              )
            }
            Right(autoConfirmedParties)
        }
      }
  }
}
