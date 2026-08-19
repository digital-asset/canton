// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

final case class SequencingParameters(payload: Option[ByteString]) extends PrettyPrinting {

  private[canton] def toInternal(
      protocolVersion: ProtocolVersion
  ): protocol.SequencingParameters = {
    val rpv = protocol.SequencingParameters.protocolVersionRepresentativeFor(protocolVersion)
    protocol.SequencingParameters(payload)(rpv)
  }

  override protected def pretty: Pretty[SequencingParameters] =
    prettyOfClass(
      param("payload", _.payload)
    )
}
