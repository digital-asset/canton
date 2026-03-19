// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, LogicalUpgradeTime, SynchronizerSuccessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

final case class AnnouncedLsu(
    successor: SynchronizerSuccessor,
    announcementEffectiveTime: EffectiveTime,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private lazy val postUpgradeTimeOffset: AtomicReference[Option[NonNegativeFiniteDuration]] =
    new AtomicReference(None)

  @nowarn("cat=deprecation")
  def computeAndCacheTimeOffset(
      syncCrypto: SyncCryptoClient[SyncCryptoApi],
      currentTimestamp: CantonTimestamp,
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Unit] =
    if (LogicalUpgradeTime.canProcessKnowingSuccessor(Some(successor), currentTimestamp)) {
      // short-circuit before we actually need the offset
      FutureUnlessShutdown.unit
    } else {
      postUpgradeTimeOffset.get() match {
        case Some(_) => FutureUnlessShutdown.unit
        case None =>
          for {
            upgradeAnnouncementEffectiveTimeTopology <- syncCrypto.snapshot(
              announcementEffectiveTime.value
            )
            upgradeAnnouncementTimeParameterChanges <-
              upgradeAnnouncementEffectiveTimeTopology.ipsSnapshot
                .listDynamicSynchronizerParametersChanges()
          } yield {
            val upgradeOffsetComputed = SequencerUtils.timeOffsetPastSynchronizerUpgrade(
              upgradeTime = successor.upgradeTime,
              parameterChanges = upgradeAnnouncementTimeParameterChanges,
            )
            logger.info(
              s"Computed synchronizer upgrade time offset: $upgradeOffsetComputed"
            )
            postUpgradeTimeOffset.set(Some(upgradeOffsetComputed))
          }
      }
    }

  def maybeOffsetTime(
      timestamp: CantonTimestamp
  )(implicit elc: ErrorLoggingContext): CantonTimestamp =
    if (LogicalUpgradeTime.canProcessKnowingSuccessor(Some(successor), timestamp)) {
      timestamp
    } else {
      timestamp + postUpgradeTimeOffset
        .get()
        .getOrElse(
          ErrorUtil.invalidState(
            "postUpgradeTimeOffset is expected to be initialized at this point."
          )
        )
    }

  def maybeOffsetTime(timestamp: Option[CantonTimestamp])(implicit
      elc: ErrorLoggingContext
  ): Option[CantonTimestamp] =
    timestamp.map(maybeOffsetTime)
}
