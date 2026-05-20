// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.implicits.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol
import com.digitalasset.canton.version.ProtoVersion
import com.google.protobuf.ByteString

final case class SequencingParameters(payload: Option[ByteString]) extends PrettyPrinting {

  private[canton] def toInternal: Either[String, protocol.SequencingParameters] =
    protocol.SequencingParameters
      .protocolVersionRepresentativeFor(ProtoVersion(30))
      .leftMap(_.message)
      .map(rpv => protocol.SequencingParameters(payload)(rpv))

  override protected def pretty: Pretty[SequencingParameters] =
    prettyOfClass(
      param("payload", _.payload)
    )
}
