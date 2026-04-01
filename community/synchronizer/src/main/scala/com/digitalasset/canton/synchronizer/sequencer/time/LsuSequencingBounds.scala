// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.transaction.{LsuAnnouncement, TopologyMapping}
import com.digitalasset.canton.util.ErrorUtil

import scala.concurrent.ExecutionContext

/** Used to specify time bounds around LSU.
  * @param upgradeTime
  *   Upgrade time from the previous synchronizer
  * @param lowerBoundSequencingTimeExclusive
  *   Strict lower bound on sequencing times. Defined as the highest effective time of the topology
  *   store on the predecessor.
  *
  * The following inequality holds:
  *
  * LSUAnnouncement.effective <= lowerBoundSequencingTimeExclusive < upgradeTime
  */
final case class LsuSequencingBounds private (
    lowerBoundSequencingTimeExclusive: CantonTimestamp,
    upgradeTime: CantonTimestamp,
) {
  require(
    lowerBoundSequencingTimeExclusive <= upgradeTime,
    s"lowerBoundSequencingTimeExclusive should be <= upgradeTime but found $lowerBoundSequencingTimeExclusive and $upgradeTime",
  )
}

object LsuSequencingBounds {
  def create(
      lowerBoundSequencingTimeExclusive: CantonTimestamp,
      upgradeTime: CantonTimestamp,
  ): Either[String, LsuSequencingBounds] =
    Either.cond(
      lowerBoundSequencingTimeExclusive <= upgradeTime,
      LsuSequencingBounds(lowerBoundSequencingTimeExclusive, upgradeTime),
      s"lowerBoundSequencingTimeExclusive should be <= upgradeTime but found $lowerBoundSequencingTimeExclusive and $upgradeTime",
    )

  def create(
      store: TopologyStore[SynchronizerStore]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[LsuSequencingBounds]] = {
    implicit val traceContext = errorLoggingContext.traceContext

    for {
      upgradeTimeO <- findUpgradeTimeFromPredecessor(store)

      lsuSequencingBounds <- upgradeTimeO
        .traverse { upgradeTime =>
          store
            .maxTimestamp(sequencedTime = SequencedTime(upgradeTime), includeRejected = true)
            .map {
              case Some((_, latestEffectiveTime)) =>
                /*
                Note: the require in the LsuSequencingBounds might throw if the invariant is not respected.
                This is fine because it means that something went terribly wrong: a topology change is effective after ugprade time.
                 */
                LsuSequencingBounds(
                  upgradeTime = upgradeTime,
                  lowerBoundSequencingTimeExclusive = latestEffectiveTime.value,
                ).some
              case None =>
                ErrorUtil.invalidState(
                  "Unable to find max timestamp of the topology store although there is an LsuAnnouncement in the store"
                )
            }
        }
        .map(_.flatten)
    } yield lsuSequencingBounds
  }

  def findUpgradeTimeFromPredecessor(
      store: TopologyStore[SynchronizerStore]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val psid = store.storeId.psid
    implicit val traceContext = errorLoggingContext.traceContext

    store
      .findPositiveTransactions(
        CantonTimestamp.MaxValue,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(TopologyMapping.Code.LsuAnnouncement),
        filterUid = Some(NonEmpty(Seq, psid.uid)),
        filterNamespace = None,
      )
      .map(
        _.collectOfMapping[LsuAnnouncement].result
          .filter(_.mapping.successorSynchronizerId == psid)
          .toList match {
          case Nil => None
          case one :: Nil => one.mapping.upgradeTime.some

          case _moreThanOne =>
            ErrorUtil.invalidState("Found more than one LsuAnnouncement mapping")
        }
      )
  }
}
