// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.BaseTest.testedProtocolVersion
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.SigningKeySpec.EcSecp256k1
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  CryptoTestHelper,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
  SequencingParameters,
  SequencingParametersWithValidity,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.crypto.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.BftOrderingSigningKeyUsage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.{
  DefaultLeaderSelectionPolicyConfig,
  DefaultSegmentLength,
  NoBlacklistingLeaderSelectionPolicyConfig,
  SegmentLength,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class CantonOrderingTopologyProviderTest
    extends AsyncWordSpec
    with CryptoTestHelper
    with BaseTest
    with HasExecutionContext {

  import CantonOrderingTopologyProviderTest.*
  implicit val pv: ProtocolVersion = testedProtocolVersion

  "Getting the ordering topology" should {
    "return the correct ordering topology" in {
      Table(
        (
          "optional activationTimestamp and maxEffectiveTimestamp",
          "expected pending topology changes",
        ),
        // Case 1: the topology snapshot query timestamp is the immediate successor of the maximum effective timestamp,
        //  i.e., the maximum activation timestamp is the same as the topology snapshot query timestamp; e.g.,
        //  a valid topology transaction is the last sequenced event and topology change delay is 0, then
        //  no topology changes are pending because the last topology change is already active at the topology
        //  snapshot query time.
        (Some(aTimestamp.immediateSuccessor -> aTimestamp), Some(false)),
        // Case 2: the topology snapshot query timestamp equals the maximum effective timestamp,
        //  i.e., the maximum activation timestamp is 1 microsecond later than the topology snapshot query timestamp;
        //  e.g., a valid topology transaction is the last sequenced event and topology change delay is 1 microsecond,
        //  then the corresponding topology change is pending because it is not already active at the topology
        //  snapshot query time.
        (Some(aTimestamp -> aTimestamp), Some(true)),
        // Case 3: bootstrap (genesis or LSU), invocation without an activation timestamp,
        //  then no max effective timestamp is available either, and pending topology changes are
        //  assumed absent without checking.
        (None, Some(false)),
      ).forEvery {
        case (activationTimestampAndMaxEffectiveTimestampO, expectedPendingTopologyChangesFlag) =>
          val nonDefaultSegmentLength = mkSegmentLength(DefaultSegmentLength.length.value + 1)
          val nonDefaultLeaderSelectionPolicy =
            DefaultLeaderSelectionPolicyConfig.copy(howLongToBlacklist =
              DefaultLeaderSelectionPolicyConfig.howLongToBlacklist match {
                case HowLongToBlacklist.Linear(_) =>
                  HowLongToBlacklist.NoBlacklisting
                case HowLongToBlacklist.NoBlacklisting => HowLongToBlacklist.Linear(Some(1L))
                case _: HowLongToBlacklist.LinearWithParameters =>
                  HowLongToBlacklist.NoBlacklisting
                case _: HowLongToBlacklist.Exponential =>
                  HowLongToBlacklist.NoBlacklisting
              }
            )
          Table[Option[Long], Option[Long], SegmentLength, Option[
            BlacklistLeaderSelectionPolicyConfig
          ], Option[BlacklistLeaderSelectionPolicyConfig], BlacklistLeaderSelectionPolicyConfig](
            (
              "segmentLength in config",
              "segmentLength in dynamic",
              "segmentLength expected to be used",
              "leader selection policy in config",
              "leader selection policy in dynamic",
              "leader selection policy expected to be used",
            ),
            // In pv=34 we use value in config if exists or default
            // In pv>34 we use value from sequencing parameters if exists or default
            (None, None, DefaultSegmentLength, None, None, DefaultLeaderSelectionPolicyConfig),
            (
              None,
              Some(nonDefaultSegmentLength.length.value),
              if (testedProtocolVersion == ProtocolVersion.v34) DefaultSegmentLength
              else nonDefaultSegmentLength,
              None,
              Some(NoBlacklistingLeaderSelectionPolicyConfig),
              if (testedProtocolVersion == ProtocolVersion.v34) DefaultLeaderSelectionPolicyConfig
              else NoBlacklistingLeaderSelectionPolicyConfig,
            ),
            (
              Some(123L),
              Some(100L),
              if (testedProtocolVersion == ProtocolVersion.v34) mkSegmentLength(123)
              else mkSegmentLength(100),
              Some(NoBlacklistingLeaderSelectionPolicyConfig),
              Some(nonDefaultLeaderSelectionPolicy),
              if (testedProtocolVersion == ProtocolVersion.v34)
                NoBlacklistingLeaderSelectionPolicyConfig
              else nonDefaultLeaderSelectionPolicy,
            ),
            (
              Some(123L),
              None,
              if (testedProtocolVersion == ProtocolVersion.v34) mkSegmentLength(123)
              else DefaultSegmentLength,
              Some(NoBlacklistingLeaderSelectionPolicyConfig),
              None,
              if (testedProtocolVersion == ProtocolVersion.v34)
                NoBlacklistingLeaderSelectionPolicyConfig
              else DefaultLeaderSelectionPolicyConfig,
            ),
          ).forEvery {
            case (
                  configSegmentLength,
                  dynamicSequencingParametersSegmentLength,
                  expectedSegmentLength,
                  configLeaderSelectionPolicy,
                  dynamicSequencingParametersLeaderSelectionPolicy,
                  expectedLeaderSelectionPolicy,
                ) =>
              val crypto =
                SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
              val pk =
                getSigningPublicKey(crypto, BftOrderingSigningKeyUsage, EcSecp256k1).futureValueUS
              val topologySnapshotMock = mock[TopologySnapshot]
              when(topologySnapshotMock.timestamp).thenReturn(aTimestamp)
              when(topologySnapshotMock.sequencerGroup())
                .thenReturn(
                  FutureUnlessShutdown.pure(
                    Some(
                      SequencerGroup(
                        NonEmptyUtil.fromUnsafe(someSequencerIds),
                        Seq.empty,
                        PositiveInt.one,
                      )
                    )
                  )
                )
              when(topologySnapshotMock.findDynamicSynchronizerParameters())
                .thenReturn(
                  FutureUnlessShutdown.pure(
                    Right(
                      DynamicSynchronizerParametersWithValidity(
                        DynamicSynchronizerParameters
                          .defaultValues(
                            testedProtocolVersion
                          ),
                        validFrom = aTimestamp,
                        validUntil = None,
                      )
                    ).withLeft[String]
                  )
                )
              when(topologySnapshotMock.findDynamicSequencingParameters()(any[TraceContext]))
                .thenReturn(
                  FutureUnlessShutdown.pure(
                    Right(
                      someDynamicSequencingParameters(
                        dynamicSequencingParametersSegmentLength,
                        dynamicSequencingParametersLeaderSelectionPolicy,
                      )
                    )
                  )
                )
              when(
                topologySnapshotMock.signingKeys(
                  eqTo(someSequencerIds),
                  eqTo(BftOrderingSigningKeyUsage),
                )(
                  any[TraceContext]
                )
              )
                .thenReturn(
                  FutureUnlessShutdown.pure(
                    someSequencerIds.map(sequencerId => sequencerId -> Seq(pk)).toMap
                  )
                )
              val synchronizerSnapshotSyncCryptoApiMock = mock[SynchronizerSnapshotSyncCryptoApi]
              when(synchronizerSnapshotSyncCryptoApiMock.ipsSnapshot).thenReturn(
                topologySnapshotMock
              )
              val cryptoApiMock = mock[SynchronizerCryptoClient]
              when(cryptoApiMock.awaitSnapshot(any[CantonTimestamp])(any[TraceContext]))
                .thenReturn(FutureUnlessShutdown.pure(synchronizerSnapshotSyncCryptoApiMock))
              when(cryptoApiMock.headSnapshot(any[TraceContext]))
                .thenReturn(synchronizerSnapshotSyncCryptoApiMock)

              activationTimestampAndMaxEffectiveTimestampO.foreach {
                case (activationTimestamp, maxEffectiveTimestamp) =>
                  when(
                    cryptoApiMock.awaitMaxTimestamp(
                      SequencedTime(activationTimestamp.immediatePredecessor)
                    )
                  )
                    .thenReturn(
                      FutureUnlessShutdown.pure(
                        Some((SequencedTime(aTimestamp), EffectiveTime(maxEffectiveTimestamp)))
                      )
                    )
              }
              new CantonOrderingTopologyProvider(
                cryptoApiMock,
                BftBlockOrdererConfig(
                  segmentLengthForPv34 = configSegmentLength,
                  leaderSelectionPolicyConfigForPv34 = configLeaderSelectionPolicy,
                ),
                loggerFactory,
                SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
              )
                .getOrderingTopologyAt(
                  activationTimestampAndMaxEffectiveTimestampO.map {
                    case (activationTimestamp, _) =>
                      TopologyActivationTime(activationTimestamp)
                  },
                  checkPendingChanges = true,
                )
                .futureUnlessShutdown()
                .futureValueUS
                .fold(fail("Ordering topology not returned")) { case (orderingTopology, _) =>
                  verify(cryptoApiMock, times(1)).member
                  activationTimestampAndMaxEffectiveTimestampO.fold {
                    // No activation timestamp tp query nor max effective time available -> bootstrap (genesis or LSU)
                    verify(cryptoApiMock, times(1)).headSnapshot(any[TraceContext])
                    verifyNoMoreInteractions(cryptoApiMock)
                  } { case (activationTimestamp, maxEffectiveTimestamp) =>
                    verify(cryptoApiMock, times(1)).awaitSnapshot(eqTo(activationTimestamp))(
                      any[TraceContext]
                    )
                    verify(cryptoApiMock, times(1)).awaitMaxTimestamp(
                      eqTo(SequencedTime(activationTimestamp.immediatePredecessor))
                    )(any[TraceContext])
                    verifyNoMoreInteractions(cryptoApiMock)
                  }
                  orderingTopology.areTherePendingCantonTopologyChanges shouldBe expectedPendingTopologyChangesFlag
                  orderingTopology.nodesTopologyInfo should contain theSameElementsAs someSequencerIds
                    .map(sequencerId =>
                      SequencerNodeId.toBftNodeId(sequencerId) -> NodeTopologyInfo(
                        keyIds = Set(FingerprintKeyId.toBftKeyId(pk.id))
                      )
                    )
                  orderingTopology.epochLength shouldBe expectedSegmentLength.epochLength(
                    someSequencerIds.size.toLong
                  )
                  orderingTopology.sequencingParameters.blacklistLeaderSelectionPolicyConfig shouldBe expectedLeaderSelectionPolicy
                  orderingTopology.maxBytesToDecompress shouldBe defaultMaxBytesToDecompress
                }
          }
      }
    }

    "getting members first known" in {
      val effectiveTimeForSequencers = Map(
        fakeSequencerId("1") -> aTimestamp,
        fakeSequencerId("2") -> aTimestamp.plusMillis(5),
      )
      val topologySnapshotMock = mock[TopologySnapshot]
      when(topologySnapshotMock.timestamp).thenReturn(aTimestamp)
      when(topologySnapshotMock.sequencerGroup())
        .thenReturn(
          FutureUnlessShutdown.pure(
            Some(
              SequencerGroup(NonEmptyUtil.fromUnsafe(someSequencerIds), Seq.empty, PositiveInt.one)
            )
          )
        )
      effectiveTimeForSequencers.foreach { case (sequencer, effectiveTime) =>
        when(topologySnapshotMock.memberFirstKnownAt(eqTo(sequencer))(any[TraceContext]))
          .thenReturn(
            FutureUnlessShutdown.pure(
              Some(SequencedTime(aTimestamp) -> EffectiveTime(effectiveTime))
            )
          )
      }
      val synchronizerSnapshotSyncCryptoApiMock = mock[SynchronizerSnapshotSyncCryptoApi]
      when(synchronizerSnapshotSyncCryptoApiMock.ipsSnapshot).thenReturn(topologySnapshotMock)
      val cryptoApiMock = mock[SynchronizerCryptoClient]
      when(cryptoApiMock.awaitSnapshot(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(synchronizerSnapshotSyncCryptoApiMock))
      new CantonOrderingTopologyProvider(
        cryptoApiMock,
        BftBlockOrdererConfig(),
        loggerFactory,
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      ).getFirstKnownAt(TopologyActivationTime(aTimestamp))
        .futureUnlessShutdown()
        .futureValueUS
        .fold(fail("Did not return members first known")) { memberFirstKnown =>
          verify(cryptoApiMock, times(1)).member
          verify(cryptoApiMock, times(1)).awaitSnapshot(eqTo(aTimestamp))(
            any[TraceContext]
          )
          verifyZeroInteractions(cryptoApiMock)
          memberFirstKnown should contain theSameElementsAs someSequencerIds
            .map(sequencerId =>
              SequencerNodeId.toBftNodeId(sequencerId) ->
                TopologyActivationTime
                  .fromEffectiveTime(EffectiveTime(effectiveTimeForSequencers(sequencerId)))
            )
        }

    }
  }
}

object CantonOrderingTopologyProviderTest {

  private def fakeSequencerId(name: String): SequencerId =
    SequencerId(UniqueIdentifier.tryCreate("ns", s"fake_$name"))

  private val aTimestamp = CantonTimestamp.Epoch
  private val someSequencerIds = Seq(fakeSequencerId("1"), fakeSequencerId("2"))
  private def mkSegmentLength(length: Long): SegmentLength = SegmentLength(
    PositiveLong.tryCreate(length)
  )
  private def someDynamicSequencingParameters(
      segmentLengthO: Option[Long],
      leaderSelectionPolicyConfigO: Option[BlacklistLeaderSelectionPolicyConfig],
  ) = {
    val pvr = SequencingParameters.protocolVersionRepresentativeFor(testedProtocolVersion)
    SequencingParametersWithValidity(
      (segmentLengthO, leaderSelectionPolicyConfigO) match {
        case (None, None) => SequencingParameters(None)(pvr)
        case (slO, lspO) =>
          SequencingParameters(
            Some(sequencingParameters(segmentLengthO, leaderSelectionPolicyConfigO).toByteString)
          )(pvr)
      },
      validFrom = aTimestamp,
      validUntil = None,
      synchronizerId = SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
    )
  }
  private def sequencingParameters(
      segmentLengthO: Option[Long],
      leaderSelectionPolicyConfigO: Option[BlacklistLeaderSelectionPolicyConfig],
  ): topology.SequencingParameters =
    topology.SequencingParameters.create(
      topology.SequencingParameters.DefaultPbftViewChangeTimeout,
      segmentLengthO.map(mkSegmentLength).getOrElse(DefaultSegmentLength),
      leaderSelectionPolicyConfigO.getOrElse(
        topology.SequencingParameters.DefaultLeaderSelectionPolicyConfig
      ),
    )(testedProtocolVersion)
}
