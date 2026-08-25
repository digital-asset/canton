// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.version.*

final case class OnboardingStateForSequencer(
    topologySnapshot: GenericStoredTopologyTransactions,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    sequencerSnapshot: SequencerSnapshot,
) extends HasProtocolVersionedWrapper[OnboardingStateForSequencer] {

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[sequencer.OnboardingStateForSequencer.type] =
    OnboardingStateForSequencer.protocolVersionRepresentativeFor(
      staticSynchronizerParameters.protocolVersion
    )

  override protected val companionObj: OnboardingStateForSequencer.type =
    OnboardingStateForSequencer

  private def toProtoV30: v30.OnboardingStateForSequencer = {
    val parameters = staticSynchronizerParameters.protoVersion match {
      case ProtoVersion(30) =>
        v30.OnboardingStateForSequencer.Parameters.V30(staticSynchronizerParameters.toProtoV30)
      case ProtoVersion(31) =>
        v30.OnboardingStateForSequencer.Parameters.V31(staticSynchronizerParameters.toProtoV31)
      case other =>
        throw new IllegalStateException(
          s"Cannot serialize synchronizer parameters to proto version $other"
        )
    }

    v30.OnboardingStateForSequencer(
      Some(topologySnapshot.toProtoV30),
      parameters,
      Some(sequencerSnapshot.toProtoV30),
    )
  }
}

object OnboardingStateForSequencer extends VersioningCompanion[OnboardingStateForSequencer] {
  override def name: String = "onboarding state for sequencer"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.OnboardingStateForSequencer
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private def fromProtoV30(
      value: v30.OnboardingStateForSequencer
  ): ParsingResult[OnboardingStateForSequencer] =
    for {
      topologySnapshot <- ProtoConverter.parseRequired(
        StoredTopologyTransactions.fromProtoV30,
        "topology_snapshot",
        value.topologySnapshot,
      )
      staticSynchronizerParams <- value.parameters match {
        case v30.OnboardingStateForSequencer.Parameters.V30(ssp) =>
          StaticSynchronizerParameters.fromProtoV30(ssp)
        case v30.OnboardingStateForSequencer.Parameters.V31(ssp) =>
          StaticSynchronizerParameters.fromProtoV31(ssp)
        case v30.OnboardingStateForSequencer.Parameters.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("parameters"))
      }
      sequencerSnapshot <- ProtoConverter.parseRequired(
        SequencerSnapshot.fromProtoV30(staticSynchronizerParams.protocolVersion, _),
        "sequencer_snapshot",
        value.sequencerSnapshot,
      )
    } yield OnboardingStateForSequencer(
      topologySnapshot,
      staticSynchronizerParams,
      sequencerSnapshot,
    )
}
