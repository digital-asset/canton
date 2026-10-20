// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.CryptoConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{CryptoSchemes, RequiredEncryptionSpecs, RequiredSigningSpecs}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters.InvalidStaticSynchronizerParameters
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, SynchronizerLimits}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

/** This test ensures that [[StaticSynchronizerParameters]] cannot be deserialized from a proto
  * version that is not compatible with the protocol version defined in the parameters themselves.
  *
  * This verifies that we cannot stuff e.g. synchronizer parameters with PV=36 into a gRPC message
  * such as `InitializeSequencerFromGenesisStateV2Request` using the `v30` alternative and have it
  * deserialize cleanly.
  *
  * It also checks how a violated default-value invariant is reported.
  */
final class StaticSynchronizerParametersTest extends AnyWordSpec with BaseTest {

  private def mkSsp(protocolVersion: ProtocolVersion): StaticSynchronizerParameters =
    createSsp(protocolVersion, SynchronizerLimits.default).value

  private def createSsp(
      protocolVersion: ProtocolVersion,
      synchronizerLimits: SynchronizerLimits,
  ): Either[InvalidStaticSynchronizerParameters, StaticSynchronizerParameters] = {
    val cryptoConfig = CryptoConfig()
    val schemes = CryptoSchemes.fromConfig(cryptoConfig).value

    StaticSynchronizerParameters
      .create(
        requiredSigningSpecs = RequiredSigningSpecs(
          schemes.signingSchemes.algorithmSpecs.allowed,
          schemes.signingSchemes.keySpecs.allowed,
        ),
        requiredEncryptionSpecs = RequiredEncryptionSpecs(
          schemes.encryptionSchemes.algorithmSpecs.allowed,
          schemes.encryptionSchemes.keySpecs.allowed,
        ),
        requiredSymmetricKeySchemes = schemes.symmetricKeySchemes.allowed,
        requiredHashAlgorithms = schemes.hashAlgorithms.allowed,
        requiredCryptoKeyFormats =
          cryptoConfig.provider.supportedCryptoKeyFormatsForProtocol(protocolVersion),
        requiredSignatureFormats =
          cryptoConfig.provider.supportedSignatureFormatsForProtocol(protocolVersion),
        topologyChangeDelay = StaticSynchronizerParameters.defaultTopologyChangeDelay,
        enableTransparencyChecks = false,
        protocolVersion = protocolVersion,
        serial = NonNegativeInt.zero,
        synchronizerLimits = synchronizerLimits,
      )
  }

  "StaticSynchronizerParameters" should {
    "read serialized parameters from old protocol versions" in {
      def parse(expectedPv: ProtocolVersion, expectedProtoVersion: Int): Assertion = {

        val bs = ByteString.copyFrom(
          getClass
            .getResourceAsStream(s"/StaticSynchronizerParameters_pv=${expectedPv.v}.bs")
            .readAllBytes()
        )

        val ssp = StaticSynchronizerParameters.fromTrustedByteString(())(bs).value

        ssp.protoVersion shouldBe ProtoVersion(expectedProtoVersion)
        ssp.protocolVersion shouldBe expectedPv
      }

      // Important: adding a new version here requires generating a new serialized version for the SSP
      // Run  mkSsp(ProtocolVersion.v37).writeToFile("community/common/src/test/resources/StaticSynchronizerParameters_pv=<PV>.bs")
      // Replace PV by the PV being added
      parse(ProtocolVersion.v34, 30)
      parse(ProtocolVersion.v36, 31)
      parse(ProtocolVersion.v37, 32)
    }

    "not be deserializable from protoV32 if PV=dev" in {
      val ssp = mkSsp(ProtocolVersion.dev)

      StaticSynchronizerParameters.fromProtoV33(ssp.toProtoV33).value shouldBe ssp

      inside(StaticSynchronizerParameters.fromProtoV32(ssp.toProtoV32)) {
        case Left(InvariantViolation(_, error)) =>
          error should include(
            s"Synchronizer parameters with PV ${ProtocolVersion.dev} cannot be deserialized from ${ProtoVersion(32)}"
          )
        case other => fail(s"Unexpected result: $other")
      }
    }
  }
}
