// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{Chain, EitherT}
import cats.kernel.Order
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractStore}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*

import java.util.ConcurrentModificationException
import scala.Ordered.orderingToOrdered
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Implements an [[ActiveContractStore!]] in memory. */
class InMemoryActiveContractStore(override val loggerFactory: NamedLoggerFactory)(implicit
    val ec: ExecutionContext
) extends ActiveContractStore
    with NamedLogging
    with InMemoryPrunableByTime[AcsError] {

  import ActiveContractStore.*
  import InMemoryActiveContractStore.*

  /** Invariant: Never maps to [[ContractStatus.Nonexistent]] */
  private[this] val table = TrieMap.empty[LfContractId, ContractStatus]

  override def createContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(show"Creating contracts at $toc: $contractIds")
      contractIds.to(LazyList).traverse_ { contractId =>
        updateTable(contractId, _.addCreation(contractId, toc))
      }
    })

  override def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(show"Archiving contracts at $toc: $contractIds")
      contractIds.to(LazyList).traverse_ { contractId =>
        updateTable(contractId, _.addArchival(contractId, toc))
      }
    })

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] =
    Future.successful {
      val snapshot = table.readOnlySnapshot()
      contractIds
        .to(LazyList)
        .mapFilter(contractId =>
          snapshot.get(contractId).flatMap(_.latestState.map(contractId -> _))
        )
        .toMap
    }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Either[AcsError, SortedMap[LfContractId, CantonTimestamp]]] =
    Future.successful {
      val snapshot = SortedMap.newBuilder[LfContractId, CantonTimestamp]
      table.foreach { case (contractId, entry) =>
        entry.activatedBy(timestamp).foreach { activationTimestamp =>
          snapshot += (contractId -> activationTimestamp)
        }
      }
      Right(snapshot.result())
    }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, CantonTimestamp]] =
    EitherT(Future.successful {
      Either.right(
        contractIds
          .to(LazyList)
          .mapFilter(contractId =>
            table.get(contractId).flatMap(_.activatedBy(timestamp)).map(contractId -> _)
          )
          .toMap
      )
    })

  override def transferInContracts(transferIns: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(s"Transferring-in contracts at $toc: $transferIns")
      transferIns.to(LazyList).traverse_ { case (contractId, sourceDomain) =>
        updateTable(contractId, _.addTransferIn(contractId, toc, sourceDomain))
      }
    })

  override def transferOutContracts(transferOuts: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(s"Transferring-out contracts at $toc: $transferOuts")
      transferOuts.to(LazyList).traverse_ { case (contractId, targetDomain) =>
        updateTable(contractId, _.addTransferOut(contractId, toc, targetDomain))
      }
    })

  override def doPrune(
      beforeAndIncluding: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, AcsError, Unit] = {
    table.foreach { case (coid, status) =>
      status.prune(beforeAndIncluding) match {
        case None => val _ = table.remove(coid)
        case Some(unchangedStatus) if unchangedStatus eq status => ()
        case Some(newStatus) =>
          val succeed = table.replace(coid, status, newStatus)
          if (!succeed)
            logger.warn(
              s"Active contract store modified at contract $coid while pruning requests. Skipping"
            )
      }
    }

    EitherT.rightT[Future, AcsError](())
  }

  override def deleteSince(
      criterion: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      table.foreach { case (coid, status) =>
        val newStatus = status.deleteSince(criterion)
        if (!(newStatus eq status)) {
          val succeed = table.replace(coid, status, newStatus)
          if (!succeed)
            throw new ConcurrentModificationException(
              s"Active contract store modified at contract $coid while deleting requests."
            )
        }
      }
    }

  override def contractCount(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] =
    Future.successful(table.values.count {
      // As the changes are ordered in the reverse order of timestamps, we take the last key here.
      status =>
        status.changes.lastKey.toc.timestamp <= timestamp
    })

  private[this] def updateTable(
      contractId: LfContractId,
      f: ContractStatus => Checked[AcsError, AcsWarning, ContractStatus],
  ): Checked[AcsError, AcsWarning, Unit] =
    MapsUtil.updateWithConcurrentlyChecked_(table, contractId, f(ContractStatus.Nonexistent), f)

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    Future.successful {
      ErrorUtil.requireArgument(
        fromExclusive <= toInclusive,
        s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
      )

      val changesByToc: Map[TimeOfChange, List[(LfContractId, ActivenessChange)]] = table.toList
        .flatMap { case (coid, status) =>
          status.changes
            .filter { case (ch, _) => ch.toc > fromExclusive && ch.toc <= toInclusive }
            .toList
            .map(ch => (coid, ch._1))
        }
        .groupBy(_._2.toc)

      val byTsAndChangeType: Map[TimeOfChange, Map[Boolean, List[LfContractId]]] = changesByToc
        .fmap(_.groupBy(_._2.isActivation).fmap(_.map(_._1)))

      byTsAndChangeType
        .to(LazyList)
        .sortBy { case (timeOfChange, _) => timeOfChange }
        .map { case (toc, changes) =>
          val activatedIds = changes.getOrElse(true, Iterable.empty).toSet
          val deactivatedIds = changes.getOrElse(false, Iterable.empty).toSet
          (
            toc,
            ActiveContractIdsChange(
              activations = activatedIds,
              deactivations = deactivatedIds,
            ),
          )
        }
    }

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[(LfContractId)]] = {
    for {
      contracts <- contractStore.find(
        filterId = None,
        filterPackage = Some(pkg),
        filterTemplate = None,
        limit = Int.MaxValue,
      )
      cids = contracts.map(_.contractId)
      states <- fetchStates(cids)
    } yield {
      states.collectFirst({ s =>
        s._2.status match {
          case ActiveContractStore.Active => s._1
        }
      })
    }
  }
}

object InMemoryActiveContractStore {
  import ActiveContractStore.*

  /** A contract status change consists of the actual [[ActivenessChange]] (timestamp, request counter, and kind)
    * and the details. The [[com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail]]
    * determines whether the actual [[ActivenessChange]]
    * is a creation/archival or a transfer-in/out. In the store, at most one
    * [[com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail]]
    * may be associated with the same [[ActivenessChange]].
    */
  type IndividualChange = (ActivenessChange, ActivenessChangeDetail)
  object IndividualChange {
    def create(toc: TimeOfChange): IndividualChange = Activation(toc) -> CreationArchivalDetail
    def archive(toc: TimeOfChange): IndividualChange = Deactivation(toc) -> CreationArchivalDetail
    def transferOut(toc: TimeOfChange, targetDomain: DomainId): IndividualChange =
      Deactivation(toc) -> TransferDetail(targetDomain)
    def transferIn(toc: TimeOfChange, sourceDomain: DomainId): IndividualChange =
      Activation(toc) -> TransferDetail(sourceDomain)
  }

  case class ActivenessChange(toc: TimeOfChange, isActivation: Boolean) {
    def assertSameDetail(
        detail1: ActivenessChangeDetail,
        detail2: ActivenessChangeDetail,
        contractId: LfContractId,
    ): Checked[AcsError, AcsWarning, Unit] =
      if (detail1 == detail2) Checked.result(())
      else {
        val irregularity =
          if (isActivation) SimultaneousActivation(contractId, toc, detail1, detail2)
          else SimultaneousDeactivation(contractId, toc, detail1, detail2)
        Checked.continue(irregularity)
      }

  }
  object Activation {
    def apply(toc: TimeOfChange): ActivenessChange = ActivenessChange(toc, isActivation = true)
  }
  object Deactivation {
    def apply(toc: TimeOfChange): ActivenessChange = ActivenessChange(toc, isActivation = false)
  }

  object ActivenessChange {

    /** Intended order is by [[com.digitalasset.canton.participant.util.TimeOfChange]]
      * and then activations (creates/transfer-in) before deactivation,
      * but this is the reversed order because we want to iterate over the earlier events in the [[ChangeJournal]]
      */
    implicit val reverseOrderingForActivenessChange: Ordering[ActivenessChange] =
      Ordering
        .by[ActivenessChange, (TimeOfChange, Boolean)](change => (change.toc, !change.isActivation))
        .reverse

    implicit val reverseOrderForActivenessChange: Order[ActivenessChange] = Order.fromOrdering
  }

  type ChangeJournal = SortedMap[ActivenessChange, ActivenessChangeDetail]

  /** Journal of the changes to contract.
    *
    * @param changes The journal of changes that have been recorded for the contract.
    *                Must be ordered by [[ActivenessChange.reverseOrderingForActivenessChange]].
    * @param latestCreation Tracks the latest creation of the contract, if any.
    *                       Used to detect when another change happens before the contract was created.
    *                       If the contract is created several times, only the latest creation is tracked.
    *                       Transfer-ins do not count as creations.
    * @param earliestArchival Tracks the earliest archival of the contract, if any.
    *                         Used to detect when another change happens after the contract was archived.
    *                         If the contract is archived several times, only the earliest archive is tracked.
    *                         Transfer-outs do not count as archivals.
    */
  case class ContractStatus private (
      changes: ChangeJournal,
      latestCreation: Option[TimeOfChange],
      earliestArchival: Option[TimeOfChange],
  ) {
    import IndividualChange.{archive, create, transferIn, transferOut}

    private[InMemoryActiveContractStore] def addCreation(
        contractId: LfContractId,
        creation: TimeOfChange,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val nextLatestCreation = latestCreation match {
        case None => Checked.result(Some(creation))
        case old @ Some(oldToc) if oldToc == creation => Checked.result(old)
        case old @ Some(oldToc) =>
          val newToc = if (creation > oldToc) Some(creation) else old
          Checked.continueWithResult(DoubleContractCreation(contractId, oldToc, creation), newToc)
      }

      // We don't report earlier changes if a double creation is detected.
      val earlierChanges =
        if (nextLatestCreation.successful)
          changesBefore(Activation(creation)).map(change =>
            ChangeBeforeCreation(contractId, creation, change.toc)
          )
        else List.empty

      for {
        nextChanges <- addIndividualChange(contractId, create(creation))
        nextLatestCreation <- nextLatestCreation.appendNonaborts(Chain.fromSeq(earlierChanges))
        nextEarliestArchival <- checkTimestampAgainstArchival(contractId, creation)
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)
    }

    private[InMemoryActiveContractStore] def addArchival(
        contractId: LfContractId,
        archival: TimeOfChange,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val nextEarliestArchival = earliestArchival match {
        case None => Checked.result(Some(archival))
        case old @ Some(oldToc) if oldToc == archival => Checked.result(old)
        case old @ Some(oldToc) =>
          val newToc = if (archival < oldToc) Some(archival) else old
          Checked.continueWithResult(DoubleContractArchival(contractId, oldToc, archival), newToc)
      }
      // We don't report later changes if a double archival is detected.
      val laterChanges =
        if (nextEarliestArchival.successful)
          changesAfter(Deactivation(archival)).map(change =>
            ChangeAfterArchival(contractId, archival, change.toc)
          )
        else List.empty

      for {
        nextChanges <- addIndividualChange(contractId, archive(archival))
        nextLatestCreation <- checkTimestampAgainstCreation(contractId, archival)
        nextEarliestArchival <- nextEarliestArchival.appendNonaborts(Chain.fromSeq(laterChanges))
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)
    }

    private[InMemoryActiveContractStore] def addTransferIn(
        contractId: LfContractId,
        transfer: TimeOfChange,
        sourceDomain: DomainId,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(contractId, transferIn(transfer, sourceDomain))
        nextLatestCreation <- checkTimestampAgainstCreation(contractId, transfer)
        nextEarliestArchival <- checkTimestampAgainstArchival(contractId, transfer)
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)

    private[InMemoryActiveContractStore] def addTransferOut(
        contractId: LfContractId,
        transfer: TimeOfChange,
        targetDomain: DomainId,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(contractId, transferOut(transfer, targetDomain))
        nextLatestCreation <- checkTimestampAgainstCreation(contractId, transfer)
        nextEarliestArchival <- checkTimestampAgainstArchival(contractId, transfer)
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)

    private[this] def addIndividualChange(
        contractId: LfContractId,
        entry: IndividualChange,
    ): Checked[AcsError, AcsWarning, ChangeJournal] = {
      val (change, detail) = entry
      changes.get(change) match {
        case None => Checked.result(changes + (change -> detail))
        case Some(oldDetail) =>
          change.assertSameDetail(oldDetail, detail, contractId).map(_ => changes)
      }
    }

    private[this] def checkTimestampAgainstCreation(
        contractId: LfContractId,
        toc: TimeOfChange,
    ): Checked[AcsError, AcsWarning, Option[TimeOfChange]] =
      latestCreation match {
        case old @ Some(creation) if toc < creation =>
          Checked.continueWithResult(ChangeBeforeCreation(contractId, creation, toc), old)
        case old => Checked.result(old)
      }

    private[this] def checkTimestampAgainstArchival(
        contractId: LfContractId,
        toc: TimeOfChange,
    ): Checked[AcsError, AcsWarning, Option[TimeOfChange]] =
      earliestArchival match {
        case old @ Some(archival) if toc > archival =>
          Checked.continueWithResult(ChangeAfterArchival(contractId, archival, toc), old)
        case old => Checked.result(old)
      }

    private[this] def changesAfter(bound: ActivenessChange): List[ActivenessChange] = {
      val laterChanges = mutable.SortedSet.newBuilder[ActivenessChange]
      val iterator = changes.keysIterator
      @tailrec def go(): Unit =
        if (iterator.hasNext) {
          val change = iterator.next()
          if (change < bound) {
            laterChanges += change
            go()
          }
        }
      go()
      laterChanges.result().toList
    }

    private[this] def changesBefore(bound: ActivenessChange): List[ActivenessChange] = {
      val laterChanges = mutable.SortedSet.newBuilder[ActivenessChange]
      val iterator = changes.keysIteratorFrom(bound)

      if (iterator.hasNext) {
        val change = iterator.next()
        // Skip the first change if it is the bound.
        if (change < bound) {
          laterChanges += change
        }
        iterator.foreach(change => laterChanges += change)
      }
      laterChanges.result().toList
    }

    /** If the contract is active right after the given `timestamp`,
      * returns the [[com.digitalasset.canton.participant.util.TimeOfChange]] of the latest creation or latest transfer-in.
      */
    def activatedBy(timestamp: CantonTimestamp): Option[CantonTimestamp] = {
      val iter = changes.iteratorFrom(ContractStatus.searchByTimestamp(timestamp))
      if (!iter.hasNext) { None }
      else {
        val (change, _) = iter.next()
        if (change.isActivation) Some(change.toc.timestamp) else None
      }
    }

    /** Returns the latest [[ActiveContractStore.ContractState]] if any */
    def latestState: Option[ContractState] = {
      changes.headOption.map { individualChange =>
        val (change, detail) = individualChange
        val status =
          if (change.isActivation) Active
          else
            detail match {
              case TransferDetail(targetDomain) => TransferredAway(targetDomain)
              case CreationArchivalDetail => Archived
            }
        ContractState(status, change.toc)
      }
    }

    def prune(beforeAndIncluding: CantonTimestamp): Option[ContractStatus] = {
      changes.keys
        .filter(change => !change.isActivation && change.toc.timestamp <= beforeAndIncluding)
        .lastOption match {

        case Some(pruneToc) =>
          // The assumption here is that the only way in which activation and deactivation will share the same
          // timestamp is when the contract is 'passing through' in which case the deactivation is logically always
          // second.
          contractStatusFromChangeJournal(changes.filter(_._1.toc > pruneToc.toc))

        case None =>
          // Skipping changes without deactivations mimics behavior of db-based ACS store
          Some(this)
      }

    }

    /** Returns a contract status that has all changes removed whose request counter is at least `criterion`. */
    def deleteSince(criterion: RequestCounter): ContractStatus = {
      val affected = changes.headOption.exists { case (change, details) =>
        change.toc.rc >= criterion
      }
      if (!affected) this
      else {
        val retainedChanges = changes.filter { case (change, _detail) => change.toc.rc < criterion }
        contractStatusFromChangeJournal(retainedChanges).getOrElse(ContractStatus.Nonexistent)
      }
    }

    private def contractStatusFromChangeJournal(journal: ChangeJournal): Option[ContractStatus] = {
      if (journal.nonEmpty) {
        val earliestArchival = journal.collect {
          case (change, detail) if !change.isActivation && !detail.isTransfer => change.toc
        }.lastOption
        val latestCreation = journal.collectFirst {
          case (change, details) if change.isActivation && !details.isTransfer => change.toc
        }
        Some(ContractStatus(journal, latestCreation, earliestArchival))
      } else None
    }
  }

  object ContractStatus {
    private def apply(
        changes: ChangeJournal,
        latestCreation: Option[TimeOfChange],
        earliestArchival: Option[TimeOfChange],
    ) =
      new ContractStatus(changes, latestCreation, earliestArchival)

    val Nonexistent = new ContractStatus(SortedMap.empty, None, None)

    private def searchByTimestamp(timestamp: CantonTimestamp) =
      Deactivation(TimeOfChange(RequestCounter.MaxValue, timestamp))
  }

}
