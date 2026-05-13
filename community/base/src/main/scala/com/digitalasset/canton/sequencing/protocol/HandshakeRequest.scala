// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

/** Handshake information exchanged between participant and sequencer
  *
  * @param clientVersion
  *   the Canton version of the client. If the protocol version used is unstable, we expect the
  *   client version to align with the server version. This is used to prevent nasty ledger forks
  *   due to changes in the PV. This was introduced with 3.5 / pv 35 after running twice into this
  *   issue.
  */
final case class HandshakeRequest(
    clientProtocolVersions: Seq[ProtocolVersion],
    minimumProtocolVersion: Option[ProtocolVersion],
    clientVersion: ReleaseVersion,
) {

  // IMPORTANT: changing the version handshakes can lead to issues with upgrading synchronizers - be very careful
  // when changing the handshake message format
  def toProtoV30: v30.SequencerConnect.HandshakeRequest =
    v30.SequencerConnect.HandshakeRequest(
      clientProtocolVersions.map(_.toProtoPrimitive),
      minimumProtocolVersion.map(_.toProtoPrimitive),
      clientVersion.toProtoPrimitive,
    )
}
