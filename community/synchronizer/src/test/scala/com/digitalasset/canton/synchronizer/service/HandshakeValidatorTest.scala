// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.synchronizer.service.HandshakeValidator
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status
import io.grpc.Status.Code
import org.scalatest.wordspec.AsyncWordSpec

class HandshakeValidatorTest extends AsyncWordSpec with BaseTest {
  "HandshakeValidator" should {
    "happy path" in {

      val tested = testedProtocolVersion.v

      // success because both support tested protocol version
      HandshakeValidator
        .clientIsCompatible(
          serverProtocolVersion = testedProtocolVersion,
          Seq(tested),
          minClientVersionP = None,
          Some(BuildInfo.version),
          disableReleaseVersionHandshakeCheck = false,
        )
        .value shouldBe ()
    }

    "succeed even if one client version is unknown to the server" in {
      val unknownProtocolVersion = 42000
      val tested = testedProtocolVersion.v

      // success because both support tested protocol version
      HandshakeValidator
        .clientIsCompatible(
          serverProtocolVersion = testedProtocolVersion,
          Seq(tested, unknownProtocolVersion),
          minClientVersionP = None,
          Some(BuildInfo.version),
          disableReleaseVersionHandshakeCheck = false,
        )
        .value shouldBe ()

      // failure: no common pv
      val status = HandshakeValidator
        .clientIsCompatible(
          serverProtocolVersion = testedProtocolVersion,
          Seq(unknownProtocolVersion),
          minClientVersionP = None,
          Some(BuildInfo.version),
          disableReleaseVersionHandshakeCheck = false,
        )
        .left
        .value

      status.getDescription shouldBe s"The protocol version required by the server ($testedProtocolVersion) is not among the supported protocol versions by the client: 42000. "
      status.getCode shouldBe Status.INVALID_ARGUMENT.getCode
      status.getCause shouldBe null
    }

    "take minimum protocol version into account" in {
      if (testedProtocolVersion.isStable) {
        val tested = testedProtocolVersion.v

        // testedProtocolVersion is lower than minimum protocol version
        val status = HandshakeValidator
          .clientIsCompatible(
            serverProtocolVersion = testedProtocolVersion,
            Seq(tested),
            minClientVersionP = Some(42),
            Some(BuildInfo.version),
            disableReleaseVersionHandshakeCheck = false,
          )
          .left
          .value

        status.getDescription shouldBe s"The version required by the synchronizer ($testedProtocolVersion) is lower than the minimum version configured by the participant (42).  "
        status.getCode shouldBe Code.INVALID_ARGUMENT
        status.getCause shouldBe null
      } else succeed
    }
  }

  "bail on mismatching binaries on dev pv" in {
    // testedProtocolVersion is lower than minimum protocol version
    val status = HandshakeValidator
      .clientIsCompatible(
        serverProtocolVersion = ProtocolVersion.dev,
        Seq(Int.MaxValue),
        minClientVersionP = Some(22),
        Some("Not the same binary"),
        disableReleaseVersionHandshakeCheck = false,
      )
      .left
      .value

    status.getDescription should include(
      "The server version is running an unstable protocol version dev which requires both client"
    )

  }

  "bail if pv35 connection doesn't include a binary" in {
    // testedProtocolVersion is lower than minimum protocol version
    HandshakeValidator
      .clientIsCompatible(
        serverProtocolVersion = ProtocolVersion.v35,
        Seq(35),
        minClientVersionP = Some(34),
        Some("Not the same binary but is okay"),
        disableReleaseVersionHandshakeCheck = false,
      )
      .valueOrFail("Handshake should have succeeded")

    // old clients should connect to pv34
    HandshakeValidator
      .clientIsCompatible(
        serverProtocolVersion = ProtocolVersion.v34,
        Seq(34),
        minClientVersionP = Some(34),
        None,
        disableReleaseVersionHandshakeCheck = false,
      )
      .valueOrFail("Handshake should have succeeded")

    val status = HandshakeValidator
      .clientIsCompatible(
        serverProtocolVersion = ProtocolVersion.v35,
        Seq(35),
        minClientVersionP = Some(34),
        None,
        disableReleaseVersionHandshakeCheck = false,
      )
      .left
      .value

    status.getDescription should include(
      "he server is running protocol 35 and does not support clients with binary"
    )

  }

}
