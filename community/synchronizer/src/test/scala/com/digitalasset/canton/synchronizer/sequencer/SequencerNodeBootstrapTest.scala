// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.synchronizer.sequencer.config.LsuSequencingBoundsOverride
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AnyWordSpec

class SequencerNodeBootstrapTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar
    with ArgumentMatchersSugar {

  private val psid = DefaultTestIdentities.physicalSynchronizerId
  private val upgradeTime = CantonTimestamp.Epoch.plusSeconds(10)
  private val boundsOverride =
    LsuSequencingBoundsOverride(CantonTimestamp.Epoch, upgradeTime)

  private def validate(
      isLsuSuccessorInit: Boolean,
      ignoreLsuPsidCheck: Boolean,
      overrideO: Option[LsuSequencingBoundsOverride],
      store: TopologyStore[SynchronizerStore],
  ): Either[String, Unit] =
    SequencerNodeBootstrap
      .validateLsuSuccessorInitialization(
        isLsuSuccessorInit,
        ignoreLsuPsidCheck,
        overrideO,
        psid,
        store,
      )(TraceContext.empty, parallelExecutionContext)
      .value
      .futureValueUS

  private def stubMatchingAnnouncement(store: TopologyStore[SynchronizerStore]): Unit = {
    val _ = doReturn(EitherT.rightT[FutureUnlessShutdown, Nothing](()))
      .when(store)
      .requireLsuAnnouncement(eqTo(psid))(any[TraceContext])
    when(
      store.findUpgradeTimeFromPredecessor()(
        eqTo(implicitly[SynchronizerStore <:< SynchronizerStore]),
        any[TraceContext],
      )
    ).thenReturn(FutureUnlessShutdown.pure(Some(upgradeTime)))
  }

  "SequencerNodeBootstrap.validateLsuSuccessorInitialization" should {
    "skip LSU checks for a non-LSU initialization" in {
      val store = mock[TopologyStore[SynchronizerStore]]

      validate(
        isLsuSuccessorInit = false,
        ignoreLsuPsidCheck = true,
        overrideO = None,
        store,
      ) shouldBe Right(())
    }

    "require an override when the PSID check is ignored for disaster recovery" in {
      val store = mock[TopologyStore[SynchronizerStore]]

      validate(
        isLsuSuccessorInit = true,
        ignoreLsuPsidCheck = true,
        overrideO = None,
        store,
      ) shouldBe Left(
        "Ignoring the LSU PSID check requires an LSU sequencing bounds override"
      )
    }

    "allow disaster recovery without an announcement when the override is valid" in {
      val store = mock[TopologyStore[SynchronizerStore]]
      when(
        store.findUpgradeTimeFromPredecessor()(
          eqTo(implicitly[SynchronizerStore <:< SynchronizerStore]),
          any[TraceContext],
        )
      ).thenReturn(FutureUnlessShutdown.pure(None))

      validate(
        isLsuSuccessorInit = true,
        ignoreLsuPsidCheck = true,
        overrideO = Some(boundsOverride),
        store,
      ) shouldBe Right(())
    }

    "reject invalid disaster-recovery bounds" in {
      val store = mock[TopologyStore[SynchronizerStore]]
      val invalidBounds =
        LsuSequencingBoundsOverride(upgradeTime.plusSeconds(1), upgradeTime)
      when(
        store.findUpgradeTimeFromPredecessor()(
          eqTo(implicitly[SynchronizerStore <:< SynchronizerStore]),
          any[TraceContext],
        )
      ).thenReturn(FutureUnlessShutdown.pure(None))

      validate(
        isLsuSuccessorInit = true,
        ignoreLsuPsidCheck = true,
        overrideO = Some(invalidBounds),
        store,
      ).left.value should include("lowerBoundSequencingTimeExclusive should be <= upgradeTime")
    }

    "require an announcement and derived bounds for ordinary LSU initialization" in {
      val store = mock[TopologyStore[SynchronizerStore]]
      stubMatchingAnnouncement(store)

      validate(
        isLsuSuccessorInit = true,
        ignoreLsuPsidCheck = false,
        overrideO = None,
        store,
      ) shouldBe Right(())
    }

    "reject ordinary LSU initialization when no announcement is available for bounds" in {
      val store = mock[TopologyStore[SynchronizerStore]]
      val _ = doReturn(EitherT.rightT[FutureUnlessShutdown, Nothing](()))
        .when(store)
        .requireLsuAnnouncement(eqTo(psid))(any[TraceContext])
      when(
        store.findUpgradeTimeFromPredecessor()(
          eqTo(implicitly[SynchronizerStore <:< SynchronizerStore]),
          any[TraceContext],
        )
      ).thenReturn(FutureUnlessShutdown.pure(None))

      validate(
        isLsuSuccessorInit = true,
        ignoreLsuPsidCheck = false,
        overrideO = None,
        store,
      ).left.value should include("does not contain an LSU announcement")
    }

    "reject a configured override when ordinary LSU initialization requires an announcement" in {
      val store = mock[TopologyStore[SynchronizerStore]]
      stubMatchingAnnouncement(store)

      validate(
        isLsuSuccessorInit = true,
        ignoreLsuPsidCheck = false,
        overrideO = Some(boundsOverride),
        store,
      ) shouldBe Left(
        "LsuSequencingBoundsOverride cannot be set if an LSU announcement exists in the topology store"
      )
    }
  }
}
