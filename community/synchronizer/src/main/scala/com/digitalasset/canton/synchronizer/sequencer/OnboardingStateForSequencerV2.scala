// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.admin.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class OnboardingStateForSequencerV2(
    topologySnapshot: Option[GenericStoredTopologyTransaction],
    staticSynchronizerParameters: Option[StaticSynchronizerParameters],
    sequencerSnapshot: Option[SequencerSnapshot],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      OnboardingStateForSequencerV2.type
    ]
) extends HasProtocolVersionedWrapperE[OnboardingStateForSequencerV2] {

  override protected val companionObj: OnboardingStateForSequencerV2.type =
    OnboardingStateForSequencerV2

  private def toProtoV30: Either[String, v30.OnboardingStateForSequencerV2] =
    sequencerSnapshot.traverse(_.toProtoV30).map { snapshot =>
      v30.OnboardingStateForSequencerV2(
        topologySnapshot.map(_.toByteString(representativeProtocolVersion.representative)),
        staticSynchronizerParameters.map(_.toByteString),
        snapshot,
      )
    }

  private def toProtoV31: v31.OnboardingStateForSequencerV2 =
    v31.OnboardingStateForSequencerV2(
      topologySnapshot.map(_.toByteString(representativeProtocolVersion.representative)),
      staticSynchronizerParameters.map(_.toByteString),
      sequencerSnapshot.map(_.toProtoV31),
    )
}

object OnboardingStateForSequencerV2
    extends VersioningCompanion2E[
      OnboardingStateForSequencerV2,
      OnboardingStateForSequencerV2,
    ] {
  override def name: String = "onboarding state for sequencer"

  def apply(
      topologySnapshot: Option[GenericStoredTopologyTransaction],
      staticSynchronizerParameters: Option[StaticSynchronizerParameters],
      sequencerSnapshot: Option[SequencerSnapshot],
      protocolVersion: ProtocolVersion,
  ): OnboardingStateForSequencerV2 = OnboardingStateForSequencerV2(
    topologySnapshot,
    staticSynchronizerParameters,
    sequencerSnapshot,
  )(protocolVersionRepresentativeFor(protocolVersion))

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec.applyE(ProtocolVersion.v36)(
      v30.OnboardingStateForSequencerV2
    )(supportedProtoVersion(_)(fromProtoV30), _.toProtoV30),
    ProtoVersion(31) -> VersionedProtoCodec.applyE(ProtocolVersion.v37)(
      v31.OnboardingStateForSequencerV2
    )(supportedProtoVersion(_)(fromProtoV31), _.toProtoV31.asRight[String]),
  )

  // Generic proto parsing
  private def fromProto[ProtoSnapshot](
      topologyTransaction: Option[ByteString],
      staticSynchronizerParameters: Option[ByteString],
      sequencerSnapshot: Option[ProtoSnapshot],
      snapshotFn: (ProtocolVersion, ProtoSnapshot) => ParsingResult[SequencerSnapshot],
      protoVersion: ProtoVersion,
  ): ParsingResult[OnboardingStateForSequencerV2] = {

    val paramsAndSnapshotEO = (staticSynchronizerParameters, sequencerSnapshot) match {
      case (None, None) => Right(None)
      case (Some(paramsP), Some(snapshotP)) =>
        for {
          params <- StaticSynchronizerParameters.fromTrustedByteString(paramsP)
          snapshot <- snapshotFn(params.protocolVersion, snapshotP)
        } yield Some((params, snapshot))
      case _ =>
        Left(
          ProtoDeserializationError.OtherError(
            "Onboarding state must contain both static synchronizer parameters and sequencer snapshot or neither of them"
          )
        )
    }
    for {
      topologySnapshot <-
        topologyTransaction.traverse(StoredTopologyTransaction.fromTrustedByteString)
      paramsAndSnapshotP <- paramsAndSnapshotEO
      staticSynchronizerParams = paramsAndSnapshotP.map(_._1)
      sequencerSnapshot = paramsAndSnapshotP.map(_._2)
      rpv <- protocolVersionRepresentativeFor(protoVersion)
    } yield OnboardingStateForSequencerV2(
      topologySnapshot,
      staticSynchronizerParams,
      sequencerSnapshot,
    )(rpv)
  }

  private def fromProtoV30(
      value: v30.OnboardingStateForSequencerV2
  ): ParsingResult[OnboardingStateForSequencerV2] =
    fromProto[v30.SequencerSnapshot](
      value.topologyTransaction,
      value.staticSynchronizerParameters,
      value.sequencerSnapshot,
      SequencerSnapshot.fromProtoV30,
      ProtoVersion(30),
    )

  private def fromProtoV31(
      value: v31.OnboardingStateForSequencerV2
  ): ParsingResult[OnboardingStateForSequencerV2] =
    fromProto[v31.SequencerSnapshot](
      value.topologyTransaction,
      value.staticSynchronizerParameters,
      value.sequencerSnapshot,
      SequencerSnapshot.fromProtoV31,
      ProtoVersion(31),
    )
}
