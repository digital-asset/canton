// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Semigroup
import cats.implicits.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.EventPerPartyCausalState
import com.digitalasset.canton.participant.store.SingleDomainCausalDependencyStore
import com.digitalasset.canton.participant.store.SingleDomainCausalDependencyStore.CausalityWrite
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

//TODO(i9525): Ensure that the performance impact of these classes is low, currently they are not efficient but it may be enough

/** Class to create event vector clocks for events on a single domain.
  *
  * This class should not be used concurrently.
  */
class SingleDomainCausalTracker(
    val globalCausalOrderer: GlobalCausalOrderer,
    val store: SingleDomainCausalDependencyStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  def registerCausalityUpdate(
      update: CausalityUpdate
  )(implicit tc: TraceContext): Future[EventPerPartyCausalState] = {
    logger.debug(s"Requesting clock for causality update $update with rc ${update.rc}}.")

    assignClocksAndStore(update)
  }

  def registerCausalityMessages(
      causalityMessages: List[CausalityMessage]
  )(implicit tc: TraceContext): Future[Unit] = {
    if (causalityMessages.nonEmpty) {
      logger.debug(s"Received ${causalityMessages.length} causality messages")
    }
    globalCausalOrderer.registerCausalityMessages(causalityMessages)
  }

  def awaitAndFetchTransferOut(tid: TransferId, parties: Set[LfPartyId])(implicit
      tc: TraceContext
  ): Future[Map[LfPartyId, VectorClock]] = {
    globalCausalOrderer.awaitTransferOutRegistered(tid, parties)
  }

  // Assign vector clocks to events on the domain. Processes events in order of request counter.
  private def assignClocksAndStore(
      update: CausalityUpdate
  )(implicit tc: TraceContext): Future[EventPerPartyCausalState] = {

    def updatePartyClocksAndGet(
        parties: Set[LfPartyId],
        domain: DomainId,
        ts: CantonTimestamp,
        outId: Option[TransferId],
    ): Future[CausalityWrite] = {
      store.updateStateAndStore(
        update.rc,
        ts,
        parties.map(p => p -> Map(domain -> ts)).toMap,
        outId,
      )
    }

    val writeF: Future[CausalityWrite] = update match {
      case transaction: TransactionUpdate =>
        updatePartyClocksAndGet(
          transaction.hostedInformeeStakeholders,
          transaction.domain,
          transaction.ts,
          None,
        )

      case transferOut: TransferOutUpdate =>
        val parties = transferOut.hostedInformeeStakeholders
        val domain = transferOut.domain
        val write =
          updatePartyClocksAndGet(parties, domain, transferOut.ts, Some(transferOut.transferId))

        for {
          clocks <- write
        } yield {
          val vectorClocks =
            clocks.stateAtWrite.map(c => VectorClock(domain, transferOut.ts, c._1, c._2)).toSet
          logger.info(s"Store out state for ${transferOut.transferId} with rc ${transferOut.rc}")
          val () =
            globalCausalOrderer.domainCausalityStore.registerTransferOut(
              transferOut.transferId,
              vectorClocks,
            )
          clocks
        }

      case TransferInUpdate(parties, ts, domain, rc, transferId) =>
        awaitAndFetchTransferOut(transferId, parties).flatMap { vectorClocksAtTransferOut =>
          logger.info(s"Clock at transfer out: $vectorClocksAtTransferOut")

          // TODO(M40): Handle receiving the wrong causality information
          ErrorUtil.requireState(
            vectorClocksAtTransferOut.keySet == parties,
            s"Transfer in event does not have causality information. Have ${vectorClocksAtTransferOut.keySet}. Need ${parties}.",
          )

          val delta = vectorClocksAtTransferOut.map { case (pid, vc) =>
            val clockAtTransferOut = vc.clock
            val pidDelta = clockAtTransferOut + (domain -> ts)
            pid -> pidDelta
          }

          store.updateStateAndStore(rc, ts, delta, None)
        }
    }

    writeF.map { write =>
      EventPerPartyCausalState((update.domain), update.ts, update.rc.asLocalOffset)(
        write.stateAtWrite
      ) // TODO(#10497) check whether this `rc.asLocalOffset` is legit here
    }
  }

}

object SingleDomainCausalTracker {

  case class EventClock(domainId: DomainId, localTs: CantonTimestamp, offset: LocalOffset)(
      val waitOn: Map[DomainId, CantonTimestamp]
  )

  case class EventPerPartyCausalState(
      domainId: DomainId,
      localTs: CantonTimestamp,
      offset: LocalOffset,
  )(
      val dependencies: Map[LfPartyId, Map[DomainId, CantonTimestamp]]
  ) {
    lazy val waitOn: Map[DomainId, CantonTimestamp] = {
      val clocks = dependencies.map { case (_, domainClocks) => domainClocks }.toList
      val sup = bound(clocks)
      sup - domainId
    }

    lazy val clock: EventClock = {
      EventClock(domainId, localTs, offset)(waitOn)
    }
  }

  def bound(clocks: List[Map[DomainId, CantonTimestamp]]): Map[DomainId, CantonTimestamp] = {
    implicit val maxTimestampSemigroup: Semigroup[CantonTimestamp] =
      (x: CantonTimestamp, y: CantonTimestamp) => x.max(y)

    import cats.implicits.*
    clocks.foldLeft(new HashMap[DomainId, CantonTimestamp](): Map[DomainId, CantonTimestamp])({
      case (acc, clk) =>
        acc.combine(clk)
    })
  }

  type DomainPerPartyCausalState = mutable.Map[LfPartyId, Map[DomainId, CantonTimestamp]]
}
