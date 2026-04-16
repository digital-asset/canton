// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.topology.transaction.TemplateBoundPartyMapping

/** Validates that all actions involving a template-bound party are on allowed templates.
  *
  * This is the core structural check that replaces cryptographic signing for
  * template-bound parties. If all actions pass, the hosting participant auto-confirms.
  * If any action is on a disallowed template, the transaction is rejected.
  */
object TemplateBoundPartyValidator {

  /** Result of validating a template-bound party's actions in a transaction. */
  sealed trait ValidationResult
  case object Valid extends ValidationResult
  final case class Invalid(
      partyId: LfPartyId,
      disallowedTemplateIds: Set[String],
  ) extends ValidationResult {
    def message: String =
      s"Template-bound party $partyId acted on disallowed templates: ${disallowedTemplateIds.mkString(", ")}"
  }

  /** Validate that all action nodes where the given party is a signatory or actor
    * use only allowed templates.
    *
    * @param partyId the template-bound party
    * @param config the party's template-bound configuration (allowed templates)
    * @param actionTemplateIds the template IDs of all action nodes where this party
    *                          is a signatory, controller, or stakeholder
    * @return Valid if all templates are allowed, Invalid with the violating template IDs otherwise
    */
  def validate(
      partyId: LfPartyId,
      config: TemplateBoundPartyMapping,
      actionTemplateIds: Set[String],
  ): ValidationResult = {
    val disallowed = actionTemplateIds.filterNot(config.isTemplateAllowed)
    if (disallowed.isEmpty) Valid
    else Invalid(partyId, disallowed)
  }
}
