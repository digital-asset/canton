// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.platform.{
  ContractIdEventFilter,
  EventFilter,
  Party,
  TemplatePartiesFilter,
}
import com.digitalasset.daml.lf.data.Ref.NameTypeConRef

sealed trait DecomposedFilter

final case class DecomposedTemplatePartyFilter(
    party: Option[Party],
    templateId: Option[NameTypeConRef],
) extends DecomposedFilter

final case class DecomposedContractIdFilter(
    internalContractId: Long
) extends DecomposedFilter

object FilterUtils {
  def decomposeFilters(filter: EventFilter): Seq[DecomposedFilter] =
    filter match {
      case TemplatePartiesFilter(relation, templateWildcardParties) => {
        val wildcardFilters =
          templateWildcardParties match {
            case Some(parties) =>
              parties.map(party => DecomposedTemplatePartyFilter(Some(party), None))
            case None => Seq(DecomposedTemplatePartyFilter(None, None))
          }
        val filters = relation.iterator.flatMap {
          case (templateId, Some(parties)) =>
            parties.iterator.map(party =>
              DecomposedTemplatePartyFilter(Some(party), Some(templateId))
            )
          case (templateId, None) =>
            Iterator(DecomposedTemplatePartyFilter(None, Some(templateId)))
        }.toVector ++ wildcardFilters
        filters
      }
      case ContractIdEventFilter(contractIds, _) =>
        contractIds.values.map(DecomposedContractIdFilter(_)).toSeq
    }
}
