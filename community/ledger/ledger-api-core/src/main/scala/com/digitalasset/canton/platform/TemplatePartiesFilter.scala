// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.daml.lf.data.Ref.NameTypeConRef

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
) extends ((NameTypeConRef, Set[String]) => Boolean) {
  def apply(templateId: NameTypeConRef, parties: Set[String]) = {
    val matchesByWildcard: Boolean =
      templateWildcardParties match {
        case Some(include) => parties.exists(p => include.map(_.toString)(p))
        case None => parties.nonEmpty // the witnesses should not be empty
      }

    def matchesByTemplateId(templateId: NameTypeConRef): Boolean =
      relation.get(templateId) match {
        case Some(Some(include)) =>
          parties.exists(include.map(_.toString)) // specific parties for the template
        case Some(None) => parties.nonEmpty // party wildcard
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
