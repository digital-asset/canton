// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.daml.lf.data.Ref.NameTypeConRef

sealed trait EventFilter extends ((NameTypeConRef, Set[String], ContractId) => Boolean) {
  def allFilterParties: Option[Set[Party]]
}

/** This class represents the filters used in transactions and contracts fetching based on the
  * templates or interfaces they implement and the parties included.
  *
  * @param relation
  *   holds the per template filters, if the value of a specific key (identifier) is defined then
  *   the filter corresponds only to the specific set of parties, if None then all the parties known
  *   to the participant are included
  * @param templateWildcardParties
  *   represents all the templates (template-wildcard) for the set of parties specified if defined
  *   or all the parties known to the participant if None.
  */
final case class TemplatePartiesFilter(
    relation: Map[NameTypeConRef, Option[Set[Party]]],
    templateWildcardParties: Option[Set[Party]],
) extends EventFilter {
  def apply(templateId: NameTypeConRef, parties: Set[String], contractId: ContractId) = {
    val matchesByWildcard: Boolean = EventFilter.checkPartyFilter(templateWildcardParties, parties)

    def matchesByTemplateId(templateId: NameTypeConRef): Boolean =
      relation.get(templateId) match {
        case Some(filterParties) => EventFilter.checkPartyFilter(filterParties, parties)
        case None => false // templateId is not in the filter
      }

    matchesByWildcard || matchesByTemplateId(templateId)
  }

  val allFilterParties: Option[Set[Party]] = {
    val partiesO = Seq(templateWildcardParties) ++ relation.values
    if (partiesO.exists(_.isEmpty)) {
      None
    } else {
      Some(partiesO.flatten.flatten.toSet)
    }
  }
}

/** Filters events by the contracts they belong to.
  *
  * @param contractIds
  *   the contract ids to match, mapped to their internal contract ids
  * @param readAsParty
  *   if defined, only events witnessed by at least one of these parties match
  */
final case class ContractIdEventFilter(
    contractIds: Map[ContractId, Long],
    readAsParty: Option[Set[Party]],
) extends EventFilter {
  def apply(templateId: NameTypeConRef, parties: Set[String], contractId: ContractId): Boolean =
    contractIds.contains(contractId) && EventFilter.checkPartyFilter(readAsParty, parties)
  override def allFilterParties: Option[Set[Party]] = readAsParty

}

object EventFilter {
  def checkPartyFilter(filterParties: Option[Set[Party]], eventWitnesses: Set[String]): Boolean =
    filterParties match {
      case Some(include) => eventWitnesses.exists(p => include.map(_.toString)(p))
      case None => eventWitnesses.nonEmpty // the witnesses should not be empty
    }

}
