// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.service.HandshakeValidator
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
          serverVersion = testedProtocolVersion,
          Seq(tested),
          minClientVersionP = None,
        )
        .value shouldBe ()
    }

    "succeed even if one client version is unknown to the server" in {
      val unknownProtocolVersion = 42000
      val tested = testedProtocolVersion.v

      // success because both support tested protocol version
      HandshakeValidator
        .clientIsCompatible(
          serverVersion = testedProtocolVersion,
          Seq(tested, unknownProtocolVersion),
          minClientVersionP = None,
        )
        .value shouldBe ()

      // failure: no common pv
      val status = HandshakeValidator
        .clientIsCompatible(
          serverVersion = testedProtocolVersion,
          Seq(unknownProtocolVersion),
          minClientVersionP = None,
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
            serverVersion = testedProtocolVersion,
            Seq(tested),
            minClientVersionP = Some(42),
          )
          .left
          .value

        status.getDescription shouldBe "The version required by the synchronizer (34) is lower than the minimum version configured by the participant (42).  "
        status.getCode shouldBe Code.INVALID_ARGUMENT
        status.getCause shouldBe null
      } else succeed
    }
  }
}
