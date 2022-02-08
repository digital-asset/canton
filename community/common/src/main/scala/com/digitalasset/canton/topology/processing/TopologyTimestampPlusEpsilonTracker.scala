// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time._
import com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Compute and synchronise the effective timestamps
  *
  * Transaction validation and processing depends on the topology state at the given sequencer time.
  * Therefore, we would have to inspect every event first if there is a topology state and wait until all
  * the topology processing has finished before evaluation the transaction. This would be slow and sequential.
  *
  * Therefore, we future date our topology transactions with an "effective time", computed from
  * the sequencerTime + domainParameters.topologyChangeDelay.
  *
  * However, the domainParameters can change and so can the topologyChangeDelay. Therefore we need to be a bit careful
  * when computing the effective time and track the changes to the topologyChangeDelay parameter accordingly.
  *
  * This class (hopefully) takes care of this logic
  */
class TopologyTimestampPlusEpsilonTracker(
    val loggerFactory: NamedLoggerFactory
) extends NamedLogging
    with TimeAwaiter {

  /** track changes to epsilon carefully.
    *
    * increasing the epsilon is straight forward. reducing it requires more care
    */
  private case class MyState(
      // the epsilon is the time we add to the timestamp
      epsilon: NonNegativeFiniteDuration,
      // from when on should this one be valid (as with all topology transactions, the time is exclusive)
      validFrom: EffectiveTime,
  )

  // sorted list of epsilon updates
  private val state = new AtomicReference[List[MyState]](List())
  // protect us against broken domains that send topology transactions in times when they've just reduced the
  // epsilon in a way that could lead the second topology transaction to take over the epsilon change.
  private val uniqueUpdateTime = new AtomicReference[EffectiveTime](EffectiveTime.MinValue)

  private val lastEffectiveTimeProcessed =
    new AtomicReference[EffectiveTime](EffectiveTime.MinValue)
  private val sequentialWait =
    new AtomicReference[Future[EffectiveTime]](
      Future.successful(EffectiveTime.MinValue)
    )

  override protected def currentKnownTime: CantonTimestamp = lastEffectiveTimeProcessed.get().value

  private def adjustByEpsilon(
      sequencingTime: SequencedTime
  )(implicit traceContext: TraceContext): EffectiveTime = {
    @tailrec
    def go(items: List[MyState]): NonNegativeFiniteDuration = items match {
      case item :: _ if sequencingTime.value > item.validFrom.value =>
        item.epsilon
      case last :: Nil =>
        if (sequencingTime.value < last.validFrom.value)
          logger.error(
            s"Bad sequencing time $sequencingTime with last known epsilon update at ${last}"
          )
        last.epsilon
      case Nil =>
        logger.error(
          s"Epsilon tracker is not initialised at sequencing time ${sequencingTime}, will use default value ${DynamicDomainParameters.topologyChangeDelayIfAbsent}"
        )
        DynamicDomainParameters.topologyChangeDelayIfAbsent // we use this (0) as a safe default
      case _ :: rest => go(rest)
    }
    val epsilon = go(state.get())
    EffectiveTime(sequencingTime.value.plus(epsilon.duration))
  }

  // must call effectiveTimeProcessed in due time
  def adjustTimestampForUpdate(sequencingTime: SequencedTime)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[EffectiveTime] =
    synchronize(
      sequencingTime, {
        val adjusted = adjustByEpsilon(sequencingTime)
        val monotonic = {
          // if a broken domain manager sends us an update too early after an epsilon reduction, we'll catch that and
          // ensure that we don't store txs in an out of order way
          // i.e. if we get at t1 an update with epsilon_1 < epsilon_0, then we do have to ensure that no topology
          // transaction is sent until t1 + (epsilon_0 - epsilon_1) (as this is the threshold for any message just before t1)
          // but if the topology manager still does it, we'll just work-around
          uniqueUpdateTime.updateAndGet(cur =>
            if (cur.value >= adjusted.value) EffectiveTime(cur.value.immediateSuccessor)
            else adjusted
          )
        }
        if (monotonic != adjusted) {
          logger.error(
            s"Broken or malicious domain topology manager is sending transactions during epsilon changes at ts=$sequencingTime!"
          )
        }
        monotonic
      },
    )

  private def synchronize(
      sequencingTime: SequencedTime,
      computeEffective: => EffectiveTime,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[EffectiveTime] = {
    // note, this is a side effect free chain await
    def chainUpdates(previousEffectiveTime: EffectiveTime): Future[EffectiveTime] = {
      FutureUtil.logOnFailure(
        {
          val synchronizeAt = CantonTimestamp.min(previousEffectiveTime.value, sequencingTime.value)
          awaitKnownTimestamp(synchronizeAt) match {
            case None => Future.successful(computeEffective)
            case Some(value) =>
              logger.debug(
                s"Need to wait until topology processing has caught up at $sequencingTime"
              )
              value.map { _ =>
                logger.debug(s"Topology processing caught up at $sequencingTime")
                computeEffective
              }
          }
        },
        "chaining of sequential waits failed",
      )
    }
    val nextChainP = Promise[EffectiveTime]()
    val ret = sequentialWait.getAndUpdate(cur => cur.flatMap(_ => nextChainP.future))
    ret.onComplete {
      case Success(previousEffectiveTime) =>
        chainUpdates(previousEffectiveTime).map { effectiveTime =>
          nextChainP.success(effectiveTime)
        } // this is supervised
      case Failure(exception) => nextChainP.failure(exception)
    }
    nextChainP.future
  }

  def effectiveTimeProcessed(effectiveTime: EffectiveTime): Unit = {
    // set new effective time
    val updated =
      lastEffectiveTimeProcessed.updateAndGet(EffectiveTime.max(_, effectiveTime))
    notifyAwaitedFutures(updated.value)
  }

  def adjustTimestampForTick(sequencingTime: SequencedTime)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[EffectiveTime] = synchronize(
    sequencingTime, {
      val adjusted = adjustByEpsilon(sequencingTime)
      val monotonic = uniqueUpdateTime.updateAndGet(EffectiveTime.max(_, adjusted))
      effectiveTimeProcessed(monotonic)
      monotonic
    },
  )

  /** adjust epsilon if it changed
    *
    * @return None if epsilon is unchanged, otherwise we return the previous epsilon
    */
  def adjustEpsilon(
      effectiveTime: EffectiveTime,
      sequencingTime: SequencedTime,
      epsilon: NonNegativeFiniteDuration,
  )(implicit traceContext: TraceContext): Option[NonNegativeFiniteDuration] = {
    val currentState = state.get().headOption
    val currentEpsilonO = currentState.map(_.epsilon)
    val ext = MyState(epsilon, effectiveTime)
    ErrorUtil.requireArgument(
      currentState.exists(_.validFrom.value < effectiveTime.value) || currentState.isEmpty,
      s"Invalid epsilon adjustment from $currentState to $ext",
    )
    if (!currentEpsilonO.contains(epsilon)) {
      state.updateAndGet { lst =>
        lst
          // we prepend this new datapoint
          .foldLeft((List(ext), false)) {
            // this fold here will keep everything which is not yet valid and the first item which is valid before the sequencing time
            case ((acc, true), _) => (acc, true)
            case ((acc, false), elem) =>
              (acc :+ elem, elem.validFrom.value >= sequencingTime.value)
          }
          ._1
      }.discard
      currentEpsilonO
    } else None
  }

}

object TopologyTimestampPlusEpsilonTracker {

  /** compute effective time of sequencedTs and populate t+e tracker with the current epsilon */
  def initialiseFromStore(
      tracker: TopologyTimestampPlusEpsilonTracker,
      store: TopologyStore,
      sequencedTs: CantonTimestamp,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[EffectiveTime] = {
    for {
      topologyChangeDelay <- TopologyTimestampPlusEpsilonTracker.determineEpsilonFromStore(
        sequencedTs,
        store,
        loggerFactory,
      )
      // we initialise our tracker with the current topology change delay, using the sequenced timestamp
      // as the effective time. this is fine as we know that if epsilon(sequencedTs) = e, then this e was activated by
      // some transaction with effective time <= sequencedTs
      _ = tracker.adjustEpsilon(
        EffectiveTime(sequencedTs),
        SequencedTime(CantonTimestamp.MinValue),
        topologyChangeDelay,
      )
      effectiveTime <- tracker.adjustTimestampForTick(SequencedTime(sequencedTs))
    } yield effectiveTime

  }

  private[topology] def determineEpsilonFromStore(
      asOf: CantonTimestamp,
      store: TopologyStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[NonNegativeFiniteDuration] = {
    val snapshot =
      new StoreBasedTopologySnapshot(
        asOf,
        store,
        Map(),
        useStateTxs = true,
        packageDependencies = _ => EitherT.pure(Set()),
        loggerFactory,
      )
    snapshot
      .findDynamicDomainParametersOrDefault(warnOnUsingDefault = false)
      .map(_.topologyChangeDelay)
  }

}
