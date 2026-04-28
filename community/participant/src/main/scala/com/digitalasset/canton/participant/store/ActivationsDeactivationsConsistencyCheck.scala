// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.participant.store.ActiveContractStore.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail.{
  Add,
  Archive,
  Create,
  Purge,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId

import scala.annotation.tailrec

@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ActivationsDeactivationsConsistencyCheck {

  /** Checks whether a new change is consistent with previous changes
    * @param cid
    *   ID of the contract
    * @param toc
    *   Time of change of the new change
    * @param changes
    *   All the changes, ordered by time of change. If two changes have the same toc, the activation
    *   should come before the deactivation.
    * @return
    *   List of issues.
    */
  def apply(
      cid: LfContractId,
      toc: TimeOfChange,
      changes: NonEmpty[Seq[(TimeOfChange, ActivenessChangeDetail)]],
  ): List[AcsWarning] = {
    var latestCreateO: Option[TimeOfChange] = None
    var earliestArchivalO: Option[TimeOfChange] = None
    var latestArchivalOrPurgeO: Option[TimeOfChange] = None

    /*
      We only generate warnings that relate to the new changes. Also, since the order of the two
      time of change in the AcsWarning matters, we potentially need to reorder
     */
    def existingToc(prevToc: TimeOfChange, currentToc: TimeOfChange) =
      if (prevToc == toc) List(currentToc)
      else if (currentToc == toc) List(prevToc)
      else Nil

    def doubleContractArchival(prevArchival: TimeOfChange, currentToc: TimeOfChange) =
      existingToc(prevArchival, currentToc).map(DoubleContractArchival(cid, _, toc))

    def doubleContractCreation(prevToc: TimeOfChange, currentToc: TimeOfChange) =
      existingToc(prevToc, currentToc).map(DoubleContractCreation(cid, _, toc))

    def updateStateVariables(change: ActivenessChangeDetail, toc: TimeOfChange): Unit =
      change match {
        case _: Create => latestCreateO = Some(toc)
        case Archive =>
          if (earliestArchivalO.isEmpty) earliestArchivalO = Some(toc)

          latestArchivalOrPurgeO = Some(toc)
        case Purge => latestArchivalOrPurgeO = Some(toc)
        case _ => ()
      }

    /*
      Checks whether change are consistent. Returns the first errors.
     */
    @tailrec
    def check(
        changes: Iterator[(TimeOfChange, ActivenessChangeDetail)],
        prevState: (TimeOfChange, ActivenessChangeDetail),
    ): List[AcsWarning] = {
      val (prevToc, prevChange) = prevState

      val isActive = prevChange.changeType match {
        case ChangeType.Activation => true
        case ChangeType.Deactivation => false
      }
      val isInactive = !isActive

      if (changes.hasNext) {
        val (currentToc, currentChange) = changes.next()

        val doubleCreation = currentChange match {
          case Create(_) =>
            latestCreateO.toList.flatMap(doubleContractCreation(_, currentToc))
          case _ => Nil
        }

        val addPurge = currentChange match {
          case _: Add if isActive => doubleContractCreation(prevToc, currentToc)
          case Purge if isInactive =>
            latestArchivalOrPurgeO.toList.flatMap(doubleContractArchival(_, currentToc))
          case _ => Nil
        }

        updateStateVariables(currentChange, currentToc)
        val warnings = addPurge ++ doubleCreation

        if (warnings.nonEmpty)
          warnings
        else
          check(changes, (currentToc, currentChange))
      } else
        Nil
    }

    val (firstToc, firstChange) = changes.head1
    updateStateVariables(firstChange, firstToc)

    check(changes.tail1.iterator, changes.head1)
  }
}
