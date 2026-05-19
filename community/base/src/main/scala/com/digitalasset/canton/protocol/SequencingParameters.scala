// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Synchronizer-wide sequencing parameters.
  *
  * @param payload
  *   The opaque payload of the synchronizer-wide sequencing parameters; its content is
  *   sequencer-dependent and synchronizer owners are responsible for ensuring that it can be
  *   correctly interpreted by the sequencers in use. If no payload is provided, sequencer-specific
  *   default values are used. If the payload cannot be correctly interpreted or the parameters
  *   cannot be set due to dynamic conditions, their value will not change.
  */
final case class SequencingParameters(payload: Option[ByteString])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencingParameters.type
    ]
) extends HasProtocolVersionedWrapper[SequencingParameters]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: SequencingParameters.type =
    SequencingParameters

  override protected def pretty: Pretty[SequencingParameters] =
    prettyOfClass(
      paramWithoutValue("payload", _.payload.isDefined)
    )

  def toProtoV30: v30.DynamicSequencingParameters =
    v30.DynamicSequencingParameters(
      payload.fold(ByteString.empty())(identity)
    )
}

object SequencingParameters extends VersioningCompanion[SequencingParameters] {

  final case class MaxRequestSize(value: NonNegativeInt) extends AnyVal {
    def unwrap: Int = value.unwrap
  }

  def default(
      representativeProtocolVersion: RepresentativeProtocolVersion[
        SequencingParameters.type
      ]
  ): SequencingParameters =
    SequencingParameters(None)(representativeProtocolVersion)

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.DynamicSequencingParameters
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "sequencing parameters"

  def fromProtoV30(
      sequencingDynamicParameters: v30.DynamicSequencingParameters
  ): ParsingResult[SequencingParameters] = {
    val payload = sequencingDynamicParameters.payload
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencingParameters(Option.when(!payload.isEmpty)(payload))(rpv)
  }
}

/** Sequencing parameters and their validity interval.
  *
  * @param validFrom
  *   Start point of the validity interval (exclusive)
  * @param validUntil
  *   End point of the validity interval (inclusive)
  */
final case class SequencingParametersWithValidity(
    parameters: SequencingParameters,
    validFrom: CantonTimestamp,
    validUntil: Option[CantonTimestamp],
    synchronizerId: SynchronizerId,
)
