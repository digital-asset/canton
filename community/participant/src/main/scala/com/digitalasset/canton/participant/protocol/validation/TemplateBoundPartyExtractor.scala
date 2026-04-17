// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{ActionDescription, FullTransactionViewTree}
import com.digitalasset.canton.discard.Implicits.*

/** Extracts per-party template IDs from a parsed transaction request.
  *
  * For each party that is a signatory or actor in the transaction, collects
  * the template IDs of all actions where that party participates. This is
  * the input to TemplateBoundPartyValidator.
  */
object TemplateBoundPartyExtractor {

  /** Extract a mapping from party to the set of template IDs of actions
    * where that party is a signatory or actor.
    *
    * @param rootViewTrees the root view trees from the parsed transaction request
    * @return map from party ID to the template IDs of their actions
    */
  def extractTemplateIdsByParty(
      rootViewTrees: Seq[FullTransactionViewTree]
  ): Map[LfPartyId, Set[String]] = {
    val builder = scala.collection.mutable.Map.empty[LfPartyId, Set[String]]

    rootViewTrees.foreach { viewTree =>
      val vpd = viewTree.viewParticipantData
      vpd.actionDescription match {
        case exercise: ActionDescription.ExerciseActionDescription =>
          val templateIdStr = exercise.templateId.toString
          // actors are the parties exercising the choice
          exercise.actors.foreach { party =>
            builder.updateWith(party) {
              case Some(existing) => Some(existing + templateIdStr)
              case None => Some(Set(templateIdStr))
            }.discard
          }

        case _: ActionDescription.CreateActionDescription =>
          // Create actions: the signatories are determined by the contract,
          // which is in the created contracts list. For template-bound party
          // validation, we care about creates because the party becomes a
          // signatory. Extract from the created core contracts.
          vpd.createdCore.foreach { created =>
            val templateIdStr = created.contract.templateId.toString
            created.contract.signatories.foreach { party =>
              builder.updateWith(party) {
                case Some(existing) => Some(existing + templateIdStr)
                case None => Some(Set(templateIdStr))
              }.discard
            }
          }

        case _: ActionDescription.FetchActionDescription =>
          // Fetch actions don't create or exercise — no template-bound party
          // constraint check needed (fetching is read-only).
          ()

        case _: ActionDescription.LookupByKeyActionDescription =>
          // LookupByKey is read-only — no template-bound constraint needed.
          ()
      }
    }

    builder.toMap
  }
}
