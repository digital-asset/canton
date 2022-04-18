// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.HasProtoV0

case class DomainNodeSequencerConfig(connection: SequencerConnection)
    extends HasProtoV0[v0.DomainNodeSequencerConfig] {
  override def toProtoV0: v0.DomainNodeSequencerConfig =
    v0.DomainNodeSequencerConfig(Some(connection.toProtoV0))
}

object DomainNodeSequencerConfig {
  def fromProtoV0(
      connectionConfigP: v0.DomainNodeSequencerConfig
  ): ParsingResult[DomainNodeSequencerConfig] =
    for {
      sequencerConnection <- ProtoConverter.parseRequired(
        SequencerConnection.fromProtoV0,
        "sequencerConnection",
        connectionConfigP.sequencerConnection,
      )
    } yield DomainNodeSequencerConfig(sequencerConnection)
}
