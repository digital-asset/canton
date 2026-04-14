// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import cats.syntax.either.*
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}
import io.grpc.Status

object HandshakeValidator {

  /** Verify that a generic server and a generic client support the same protocol version. In
    * practice, this class is used for all handshakes (e.g. the participant-synchronizer one) except
    * the sequencer client-sequencer handshake.
    */
  def clientIsCompatible(
      serverVersion: ProtocolVersion,
      clientVersionsP: Seq[Int],
      minClientVersionP: Option[Int],
  ): Either[Status, Unit] = {
    // Client may mention a protocol version which is not known to the synchronizer
    val clientVersions = clientVersionsP.map(ProtocolVersion.parseUnchecked)
    val minClientVersion = minClientVersionP.map(ProtocolVersion.parseUnchecked)
    for {
      _ <- ProtocolVersionCompatibility
        .canClientConnectToServer(clientVersions, serverVersion, minClientVersion)
        .leftMap(_.asStatus)
    } yield ()
  }
}
