// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.syntax.functor.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  GenericAcsChange,
  InternalizedAcsChange,
  InternalizedContractStakeholdersAndReassignmentCounter,
}
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.CommitmentSnapshot
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType
import com.digitalasset.canton.util.Mutex
import com.digitalasset.canton.{InternedPartyId, LfPartyId, lfPartyOrdering}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.{Map, SortedSet}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
abstract class GenericRunningCommitments[T: Pretty](
    initRt: RecordTime,
    initialCommitments: IterableOnce[(SortedSet[T], LtHash16)],
)(implicit ordering: Ordering[T])
    extends HasLoggerName {

  protected val commitments: TrieMap[SortedSet[T], LtHash16] = TrieMap.from(initialCommitments)

  private val lock = new Mutex()
  @volatile private var rt: RecordTime = initRt
  private val deltaB = Map.newBuilder[SortedSet[T], LtHash16]
  private var freshStakeholderGroupsSinceLastSnapshot: Long = 0

  /** The latest (immutable) snapshot. Taking the snapshot also garbage collects empty commitments.
    */
  def snapshot(): CommitmentSnapshot[T] = {

    /* Delete all hashes that have gone empty since the last snapshot;
      returns the corresponding stakeholder sets */
    def garbageCollect(
        candidates: Map[SortedSet[T], LtHash16]
    ): Set[SortedSet[T]] = {
      val deletedB = Set.newBuilder[SortedSet[T]]
      candidates.foreach { case (stkhs, h) =>
        if (h.isEmpty) {
          deletedB += stkhs
          commitments -= stkhs
        }
      }
      deletedB.result()
    }

    {
      lock.exclusive {
        val delta = deltaB.result()
        deltaB.clear()
        val deleted = garbageCollect(delta)
        val activeDelta = (delta -- deleted).fmap(_.getByteString())
        val freshStakeholderGroups = freshStakeholderGroupsSinceLastSnapshot
        freshStakeholderGroupsSinceLastSnapshot = 0
        val groupCountDelta = freshStakeholderGroups - deleted.size
        // Note that it's crucial to eagerly (via fmap, as opposed to, say mapValues) snapshot the LtHash16 values,
        // since they're mutable
        CommitmentSnapshot(
          rt,
          commitments.readOnlySnapshot().toMap.fmap(_.getByteString()),
          activeDelta,
          deleted,
          groupCountDelta,
        )
      }
    }
  }

  def update(rt: RecordTime, change: GenericAcsChange[T])(implicit
      loggingContext: NamedLoggingContext
  ): Unit =
    lock.exclusive {
      this.rt = rt
      change.activations.foreach { case (cid, stakeholdersAndReassignmentCounter) =>
        val sortedStakeholders =
          SortedSet(stakeholdersAndReassignmentCounter.stakeholders.toSeq*)
        val h = commitmentForGroup(sortedStakeholders)
        AcsCommitmentProcessor.addContractToCommitmentDigest(
          h,
          cid,
          stakeholdersAndReassignmentCounter.reassignmentCounter,
        )
        loggingContext.debug(
          s"Adding to commitment activation cid $cid reassignmentCounter ${stakeholdersAndReassignmentCounter.reassignmentCounter}"
        )
        deltaB += sortedStakeholders -> h
      }
      change.deactivations.foreach { case (cid, stakeholdersAndReassignmentCounter) =>
        val sortedStakeholders =
          SortedSet(stakeholdersAndReassignmentCounter.stakeholders.toSeq*)
        val h = commitmentForGroup(sortedStakeholders)
        AcsCommitmentProcessor.removeContractFromCommitmentDigest(
          h,
          cid,
          stakeholdersAndReassignmentCounter.reassignmentCounter,
        )
        loggingContext.debug(
          s"Removing from commitment deactivation cid $cid reassignmentCounter ${stakeholdersAndReassignmentCounter.reassignmentCounter}"
        )
        deltaB += sortedStakeholders -> h
      }
    }

  private def commitmentForGroup(group: SortedSet[T]): LtHash16 = {
    val freshHash = LtHash16()
    val oldHash = commitments.putIfAbsent(group, freshHash)
    oldHash match {
      case Some(old) => old
      case None =>
        freshStakeholderGroupsSinceLastSnapshot += 1
        freshHash
    }
  }

  def watermark: RecordTime = rt

  def reinitialize(snapshot: Map[SortedSet[T], CommitmentType], recordTime: RecordTime): Unit =
    lock.exclusive {
      // delete all active
      deltaB.clear()
      commitments.clear()
      snapshot.foreach { case (stkhd, cmt) =>
        commitments += stkhd -> LtHash16.tryCreate(cmt)
      }
      rt = recordTime
    }

  @SuppressWarnings(Array("com.digitalasset.canton.ConcurrentMapSize"))
  def size: Int = commitments.size
}

class InternalizedRunningCommitments(
    initRt: RecordTime,
    initialCommitments: IterableOnce[(SortedSet[InternedPartyId], LtHash16)],
    stringInterning: StringInterning,
) extends GenericRunningCommitments[InternedPartyId](initRt, initialCommitments) {

  /** We also need (at least temporarily) a non-internalized version of update for
    * [[AcsCommitmentProcessor]]
    *
    * TODO(i29876) Get rid of the mixed InternalizedRunningCommitments#update (internalized and
    * non-internalized)
    */
  def update(rt: RecordTime, change: AcsChange)(implicit
      loggingContext: NamedLoggingContext
  ): Unit =
    update(rt, internalizeAcsChange(change))

  private def internalizeContractStakeholders(
      counter: ContractStakeholdersAndReassignmentCounter
  ): InternalizedContractStakeholdersAndReassignmentCounter =
    InternalizedContractStakeholdersAndReassignmentCounter(
      counter.stakeholders.map(stringInterning.party.internalize),
      counter.reassignmentCounter,
    )

  private def internalizeAcsChange(change: AcsChange): InternalizedAcsChange =
    InternalizedAcsChange(
      activations = change.activations.map { case (contractId, stakeholdersAndCounter) =>
        contractId -> internalizeContractStakeholders(stakeholdersAndCounter)
      },
      deactivations = change.deactivations.map { case (contractId, stakeholdersAndCounter) =>
        contractId -> internalizeContractStakeholders(stakeholdersAndCounter)
      },
    )
}

class RunningCommitments(
    initRt: RecordTime,
    initialCommitments: IterableOnce[(SortedSet[LfPartyId], LtHash16)],
) extends GenericRunningCommitments[LfPartyId](initRt, initialCommitments)
