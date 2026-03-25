// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.acs

import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.performance.acs.ContractStore.StoreEntry
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.{ErrorUtil, Mutex}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap

/** A in-memory contract store with trivial indexing and query capabilities
  *
  * The contract store takes an index function which maps an instance to an index object L (a case
  * class), which allows to query the store using find(arg: L): Seq[Contract[TCid, T]].
  *
  * The store find / all commands will only return contracts for which no command has yet been
  * submitted that will consume the contract.
  *
  * @param name
  *   a name which we'll be using for logging
  * @param companion
  *   the template companion of the template we are watching
  * @param index
  *   the indexing function which takes an instance of a contract and returns some index object
  *   (query arguments)
  * @param filter
  *   filter to limit what is stored in the contract store
  * @param loggerFactory
  *   the logger factory
  * @param update
  *   update the contract (i.e. drop data to not waste storage)
  * @tparam TC
  *   the contract type (java code-gen)
  * @tparam TCid
  *   the contract id type (java code-gen)
  * @tparam T
  *   the contract template (java code-gen)
  * @tparam L
  *   the index expression
  */
class ContractStore[TC <: Contract[TCid, T], TCid <: ContractId[T], T, L](
    name: String,
    companion: ContractCompanion[TC, TCid, T],
    index: TC => L,
    filter: TC => Boolean,
    val loggerFactory: NamedLoggerFactory,
) extends BaseContractObserver[TC, TCid, T](companion)
    with NamedLogging
    with NoTracing {

  import com.digitalasset.canton.util.ShowUtil.*

  // our contract store with a flag indicating if there is a pending command which will consume the
  // contract if it succeeds
  private val store = TrieMap[ContractId[T], StoreEntry[TC]]()
  private val storeSize = new AtomicInteger(0)
  private val lock = new Mutex()

  // lookup data structure which tracks a list of contract ids that map to a given index L and a counter of how many of
  // these contracts are pending.
  private val lookup = TrieMap[L, (Int, Set[ContractId[T]])]()

  /** return one contract that is not pending and that matches the query argument */
  def one(item: L): Option[(String, TC)] =
    lookup.get(item).flatMap { case (_, contracts) =>
      contracts
        .flatMap(y => store.get(y).toList)
        .filterNot(_.pending)
        .map(c => (c.synchronizer, c.contract))
        .headOption
    }

  def take(
      item: L,
      num: Int,
      filter: (String, TC) => Boolean = ((_, _) => true),
  ): Seq[(String, TC)] =
    lookup
      .get(item)
      .toList
      .flatMap { case (_, contracts) =>
        contracts.iterator
          .flatMap(cid =>
            store
              .get(cid)
              .filterNot(_.pending)
              .filter(c => filter(c.synchronizer, c.contract))
              .map(c => (c.synchronizer, c.contract))
              .toList
          )
          .take(num)
      }

  /** find all contracts that are not pending and match the query argument */
  def find(item: L): Seq[(String, TC)] =
    lookup
      .get(item)
      .toList
      .flatMap(x =>
        x._2
          .flatMap(y => store.get(y).toList)
          .filterNot(_.pending)
          .map(c => (c.synchronizer, c.contract))
      )

  /** return number of contracts matching the query argument excluding pending contracts */
  def num(item: L): Int = lookup.get(item).map(x => x._2.size - x._1).getOrElse(0)

  def entirelyEmpty: Boolean = store.isEmpty

  def hasPending: Boolean = lookup.exists(_._2._1 > 0)

  def all: Iterable[(String, Boolean, Contract[TCid, T])] =
    store.values.map(c => (c.synchronizer, c.pending, c.contract))

  /** return all contracts which are not pending */
  def allAvailable: Iterable[Contract[TCid, T]] = store.values.filterNot(_.pending).map(_.contract)

  /** return total number of contracts in store, pending and non-pending */
  def totalNum: Int =
    storeSize.get()

  def setPending(cid: ContractId[T], isPending: Boolean): Boolean = (lock.exclusive {
    val current = store.get(cid)
    current.foreach { state =>
      logger.debug(s"Setting $cid to pending = $isPending")
      store.update(cid, state.copy(pending = isPending))
      // adjust pending counter in the lookup summary so we can
      // properly answer num(()) requests
      if (isPending != state.pending) {
        val idx = index(state.contract)
        val cur = lookup.get(idx)
        val incOrDec = if (isPending) 1 else -1
        cur.foreach { case (count, seq) =>
          ErrorUtil.requireState(
            count + incOrDec > -1,
            s"Invalid new pending counter: $count + $incOrDec.",
          )
          lookup.update(idx, (count + incOrDec, seq))
        }
      }
    }
    current.isDefined
  })

  protected def contractCreated(create: TC, index: L): Unit = {}
  protected def contractArchived(archive: TC, index: L): Unit = {}

  override def reset(): Unit = {
    store.clear()
    storeSize.set(0)
    lookup.clear()
  }

  protected def update(before: TC): TC = before

  private def addToStore(
      synchronizerId: String,
      create: TC,
      reassignmentCounter: Long,
  ): (Boolean, L) = {
    val idx = index(create)
    if (!store.contains(create.id)) {
      storeSize.incrementAndGet().discard
      store
        .put(
          create.id,
          StoreEntry(
            update(create),
            pending = false,
            reassignmentCounter = reassignmentCounter,
            synchronizerId,
          ),
        )
        .discard
      lookup.get(idx) match {
        case None => lookup += (idx -> ((0, Set(create.id))))
        case Some((pending, lst)) => lookup.update(idx, (pending, lst + create.id))
      }
      (true, idx)
    } else (false, idx)
  }

  override protected def processCreate_(synchronizerId: String)(create: TC): Unit =
    if (filter(create)) {
      (lock.exclusive {
        val (added, idx) = addToStore(synchronizerId, create, 0)
        if (added) {
          logger.debug(
            s"Observed create ${name.unquoted}, index=${idx.toString.unquoted}, cid=${create.id},$NL" +
              s"args=${create.data})"
          )
          contractCreated(create, idx)
        }
      })
    }

  override protected def processReassign_(synchronizerId: String, reassignmentCounter: Long)(
      contract: TC
  ): Unit =
    if (filter(contract)) {
      lock.exclusive {
        store
          .updateWith(contract.id) {
            case Some(value) =>
              val updated =
                if (value.reassignmentCounter < reassignmentCounter)
                  value.copy(
                    reassignmentCounter = reassignmentCounter,
                    synchronizer = synchronizerId,
                  )
                else value
              Some(updated)
            case None =>
              // at some point we need to deal with reassignment which before a create or after an archive
              logger.warn(
                s"Skipping out of order reassign for cid=${contract.id} for counter=$reassignmentCounter"
              )
              None
          }
          .discard
      }
    }

  override protected def processArchive_(archive: ContractId[T]): Unit = (lock.exclusive {
    logger.debug(s"Observed archive ${name.unquoted} $archive")
    store.remove(archive).foreach { entry =>
      storeSize.decrementAndGet().discard
      val idx = index(entry.contract)
      lookup.get(idx).foreach { case (numPending, contractIds) =>
        // decrement pending if necessary
        val newPending = if (entry.pending) numPending - 1 else numPending
        lookup.update(idx, (newPending, contractIds - entry.contract.id))
      }
      contractArchived(entry.contract, idx)
    }
  })

}

object ContractStore {
  final case class StoreEntry[C](
      contract: C,
      pending: Boolean,
      reassignmentCounter: Long,
      synchronizer: String,
  )
}
