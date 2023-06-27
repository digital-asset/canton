// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import com.daml.lf.data.Ref.Identifier
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.LfTemplateId

/** @param partyTemplate Each pair `(partyId, templatesO)` will be used to filter the contracts as follows:
  *                - `partyId` must be a stakeholder of the contract
  *                - `templatesO` if defined, the contract instance is one of the `templatesO`
  */
final case class RequestInclusiveFilter(
    partyTemplate: Map[LfPartyId, Option[NonEmpty[Seq[LfTemplateId]]]]
) {
  val parties: Set[LfPartyId] = partyTemplate.keySet

  /** Check whether a contract (here, stakeholders and templateId) pass the filter
    *
    * @param stakeholders Stakeholders of the contract
    * @param templateId   Template of the contract
    * @return true if the contract satisfies the filter
    */
  def satisfyFilter(stakeholders: Set[LfPartyId], templateId: Identifier): Boolean = {
    // At least one stakeholder should pass the per-party filter
    stakeholders.exists { stakeholder =>
      partyTemplate.get(stakeholder) match {
        case Some(templatesFilterO) =>
          templatesFilterO match {
            case Some(templates) =>
              templates.contains(templateId)

            case None => true // There is no restriction on the template
          }

        // stakeholder is not in the request
        case None => false
      }
    }
  }
}
