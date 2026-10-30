// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.all.*
import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.config.GeneratorsConfig.*
import com.digitalasset.canton.crypto.GeneratorsCrypto.*
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.GeneratorsDataTime.*
import com.digitalasset.canton.protocol.{GeneratorsProtocol, StaticSynchronizerParameters}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationBySender,
  AggregationId,
  AggregationRule,
  AggregationRuleInput,
  TrafficState,
  GeneratorsProtocol as GeneratorsProtocolSeq,
}
import com.digitalasset.canton.sequencing.traffic.{TrafficConsumed, TrafficPurchased}
import com.digitalasset.canton.synchronizer.block.update.InFlightAggregations
import com.digitalasset.canton.synchronizer.sequencer.store.VersionedStatus
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.DbSequencerStateManagerStore.AggregatedSignaturesOfSender
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.transaction.GeneratorsTransaction
import com.digitalasset.canton.topology.{GeneratorsTopology, MediatorId, Member, SequencerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.rpc.status.Status
import magnolify.scalacheck.auto.*
import org.scalacheck.util.Buildable
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters.*

final class GeneratorsSequencer(
    protocolVersion: ProtocolVersion,
    generatorsTopology: GeneratorsTopology,
    generatorsTransaction: GeneratorsTransaction,
    generatorsProtocolSeq: GeneratorsProtocolSeq,
    generatorsProtocol: GeneratorsProtocol,
) {
  import generatorsTopology.*
  import generatorsTransaction.*
  import generatorsProtocolSeq.*
  import generatorsProtocol.*

  /** `MemberLifecycleStatus.Onboarding` can only be expressed in proto v31, which is used starting
    * at protocol version 37. Serializing it to proto v30 is rejected, and onboarding members are
    * never created below that protocol version.
    */
  private val supportsOnboardingStatus: Boolean = protocolVersion >= ProtocolVersion.v37

  implicit val inFlightAggregationArb: Arbitrary[InFlightAggregation] = Arbitrary(
    for {
      rule <- Arbitrary.arbitrary[AggregationRule]
      maxSequencingTimestamp <- Arbitrary.arbitrary[CantonTimestamp]
      signatures <- boundedListGen(boundedListGen[Signature])
      senders <- rule.input match {
        case AggregationRuleInput.MediatorGroup(_) =>
          boundedListGen[MediatorId].map(_.widen[Member])
        case AggregationRuleInput.SequencerGroup => boundedListGen[SequencerId].map(_.widen[Member])
        case AggregationRuleInput.SenderDedup => Arbitrary.arbitrary[Member].map(List(_))
      }
      aggregatedSendersList <- Gen.sequence(
        senders.map(member =>
          Gen
            .choose(CantonTimestamp.MinValue, maxSequencingTimestamp)
            .map(sequencingTimestamp =>
              member -> AggregationBySender(sequencingTimestamp, signatures)
            )
        )
      )(Buildable.buildableArrayList)
      aggregatedSenders = SortedMap.from(aggregatedSendersList.asScala)
    } yield InFlightAggregation.tryCreate(
      aggregatedSenders,
      maxSequencingTimestamp,
      rule,
    )
  )

  private def sequencerMemberStatusGen(withOnboarding: Boolean): Gen[SequencerMemberStatus] = {
    val activeOrDisabledGen = for {
      member <- Arbitrary.arbitrary[Member]
      registeredAt <- Arbitrary.arbitrary[CantonTimestamp]
      lastAcknowledged <- Arbitrary.arbitrary[Option[CantonTimestamp]]
      // A member disabled while it was still onboarding has no registration timestamp. Proto v30
      // cannot represent that (it falls back to `CantonTimestamp.MaxValue`), so only generate it
      // where the v31 representation is available.
      disabledRegisteredAt <-
        if (withOnboarding) Gen.option(Gen.const(registeredAt))
        else Gen.const(Some(registeredAt))
      status <- Gen.oneOf[SequencerMemberStatus](
        SequencerMemberStatus.Active(member, registeredAt, lastAcknowledged),
        SequencerMemberStatus.Disabled(member, disabledRegisteredAt, lastAcknowledged),
      )
    } yield status

    if (withOnboarding)
      Gen.oneOf(
        activeOrDisabledGen,
        Arbitrary.arbitrary[Member].map(SequencerMemberStatus.Onboarding(_)),
      )
    else activeOrDisabledGen
  }

  private def sequencerPruningStatusGen(withOnboarding: Boolean): Gen[SequencerPruningStatus] =
    for {
      lowerBound <- Arbitrary.arbitrary[CantonTimestamp]
      now <- Arbitrary.arbitrary[CantonTimestamp]
      members <- boundedListGen(sequencerMemberStatusGen(withOnboarding))
    } yield SequencerPruningStatus(lowerBound, now, members.toSet)

  implicit val sequencerPruningStatusArb: Arbitrary[SequencerPruningStatus] = Arbitrary(
    sequencerPruningStatusGen(withOnboarding = supportsOnboardingStatus)
  )

  private def sequencerSnapshotGen(withOnboarding: Boolean): Gen[SequencerSnapshot] =
    for {
      lastTs <- Arbitrary.arbitrary[CantonTimestamp]
      latestBlockHeight <- Arbitrary.arbitrary[Long]
      previousTimestamps <- boundedMapGen[Member, Option[CantonTimestamp]]
      status <- sequencerPruningStatusGen(withOnboarding)
      inFlightAggregations <- boundedMapGen[AggregationId, InFlightAggregation]
      additional <- Arbitrary.arbitrary[Option[SequencerSnapshot.ImplementationSpecificInfo]]
      trafficPurchased <- boundedListGen[TrafficPurchased]
      trafficConsumed <- boundedListGen[TrafficConsumed]
    } yield SequencerSnapshot(
      lastTs,
      latestBlockHeight,
      previousTimestamps,
      status,
      InFlightAggregations.fromMap(inFlightAggregations),
      additional,
      protocolVersion,
      trafficPurchased,
      trafficConsumed,
    )

  implicit val sequencerSnapshotArb: Arbitrary[SequencerSnapshot] = Arbitrary(
    sequencerSnapshotGen(withOnboarding = supportsOnboardingStatus)
  )

  implicit val onboardingStateForSequencerV2: Arbitrary[OnboardingStateForSequencerV2] = Arbitrary(
    for {
      topologySnapshotO <- Gen.option(Arbitrary.arbitrary[GenericStoredTopologyTransaction])
      syncParamsAndSnapshotO <- Gen.option(
        Arbitrary.arbitrary[(StaticSynchronizerParameters, SequencerSnapshot)]
      )
    } yield OnboardingStateForSequencerV2(
      topologySnapshotO,
      syncParamsAndSnapshotO.map(_._1),
      syncParamsAndSnapshotO.map(_._2),
      protocolVersion,
    )
  )

  implicit val lsuTrafficState: Arbitrary[LsuTrafficState] = {
    implicit val arbMemberTrafficState: Arbitrary[(Member, TrafficState)] = Arbitrary(
      for {
        member <- Arbitrary.arbitrary[Member]
        trafficState <- Arbitrary.arbitrary[TrafficState]
      } yield (member, trafficState)
    )
    Arbitrary(
      boundedMapGen.map(
        LsuTrafficState(_)(LsuTrafficState.protocolVersionRepresentativeFor(protocolVersion))
      )
    )
  }

  implicit val versionedStatusArb: Arbitrary[VersionedStatus] = {
    implicit val protoAnyArb: Arbitrary[com.google.protobuf.any.Any] = Arbitrary(
      for {
        typeUrl <- Arbitrary.arbString.arbitrary
        value <- Arbitrary.arbString.arbitrary.map(com.google.protobuf.ByteString.copyFromUtf8)
      } yield com.google.protobuf.any.Any(typeUrl, value)
    )

    Arbitrary(
      for {
        code <- Gen.oneOf(io.grpc.Status.Code.values().toIndexedSeq).map(_.value())
        message <- Arbitrary.arbString.arbitrary
        details <- boundedListGen[com.google.protobuf.any.Any]
      } yield {
        val status = Status(
          code = code,
          message = message,
          details = details,
        )
        VersionedStatus.create(status, protocolVersion)
      }
    )
  }

  implicit val aggregatedSignaturesOfSenderArb: Arbitrary[AggregatedSignaturesOfSender] = Arbitrary(
    boundedListGen[List[Signature]](boundedListGen[Signature]).map(
      AggregatedSignaturesOfSender(_)(
        AggregatedSignaturesOfSender.protocolVersionRepresentativeFor(protocolVersion)
      )
    )
  )
}
