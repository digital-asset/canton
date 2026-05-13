// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import cats.syntax.either.*
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.version.{
  ClientBinaryIsTooOld,
  HandshakeError,
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
  UnstableProtocolRequiresMatchingBinaries,
}
import io.grpc.Status

object HandshakeValidator {

  /** Verify that a generic server and a generic client support the same protocol version. In
    * practice, this class is used for all handshakes (e.g. the participant-synchronizer one) except
    * the sequencer client-sequencer handshake.
    */
  def clientIsCompatible(
      serverProtocolVersion: ProtocolVersion,
      clientVersionsP: Seq[Int],
      minClientVersionP: Option[Int],
      clientBinaryVersion: Option[String],
      disableReleaseVersionHandshakeCheck: Boolean,
  ): Either[Status, Unit] = {
    // Client may mention a protocol version which is not known to the synchronizer
    val clientVersions = clientVersionsP.map(ProtocolVersion.parseUnchecked)
    val minClientVersion = minClientVersionP.map(ProtocolVersion.parseUnchecked)

    // New check introduced with pv35 to validate whether the binary used aligns with
    // the expectations to prevent ledger forks and other incompatibilities because
    // of users running unstable development snapshots in production.
    // We learned this the hard way ...
    val preCheckE: Either[HandshakeError, Unit] =
      if (serverProtocolVersion > ProtocolVersion.v34 && !disableReleaseVersionHandshakeCheck) {
        for {
          clientVersion <- clientBinaryVersion.toRight(
            ClientBinaryIsTooOld(
              serverProtocolVersion,
              "'Binary version not provided. Participant binary version is too old.'",
            ): HandshakeError
          )
          _ <- Either
            .cond(
              // server version must either be stable or the client binary version must match the server version,
              // otherwise the client may not be compatible with the server
              serverProtocolVersion.isStable || clientVersion == ReleaseVersion.current.toProtoPrimitive,
              (),
              UnstableProtocolRequiresMatchingBinaries(
                serverProtocolVersion,
                BuildInfo.version,
                clientVersion,
              ),
            )
        } yield ()
      } else Right(())
    preCheckE
      .flatMap(_ =>
        ProtocolVersionCompatibility
          .canClientConnectToServer(clientVersions, serverProtocolVersion, minClientVersion)
      )
      .leftMap(_.asStatus)
  }
}
