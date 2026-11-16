// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

sealed trait PersistentParticipantEventUpdateCategory extends Product with Serializable {
  def asInt: Int
  def renderAsReassignment: Boolean =
    PersistentParticipantEventUpdateCategory.renderAsReassignment(this)
  def renderAsTransaction: Boolean = !renderAsReassignment
}

object PersistentParticipantEventUpdateCategory {
  abstract class ParticipantEventUpdateCategory(override val asInt: Int)
      extends PersistentParticipantEventUpdateCategory
  case object Purge extends ParticipantEventUpdateCategory(1)
  case object AcsImport extends ParticipantEventUpdateCategory(2)
  case object OnlinePartyReplication extends ParticipantEventUpdateCategory(3)
  case object AcsImportTxStyle extends ParticipantEventUpdateCategory(4)
  case object PurgeTxStyle extends ParticipantEventUpdateCategory(5)

  val allCategories: Seq[PersistentParticipantEventUpdateCategory] = Seq(
    Purge,
    AcsImport,
    OnlinePartyReplication,
    AcsImportTxStyle,
    PurgeTxStyle,
  )

  val renderAsReassignment: PersistentParticipantEventUpdateCategory => Boolean = {
    case Purge | AcsImport | OnlinePartyReplication => true
    case _ => false
  }

  private val formIntMap: Map[Int, PersistentParticipantEventUpdateCategory] =
    allCategories
      .map(category => category.asInt -> category)
      .toMap

  def fromInt(i: Int): PersistentParticipantEventUpdateCategory =
    formIntMap.getOrElse(
      i,
      throw new IllegalStateException(
        s"Invalid Int $i - no such PersistentParticipantEventUpdateCategory can be found."
      ),
    )

  assert(allCategories.sizeIs == 5)
  assert(allCategories.toSet.sizeIs == 5)
  assert(formIntMap.sizeIs == 5)
}
