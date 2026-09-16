// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.CryptoConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{CryptoSchemes, RequiredEncryptionSpecs, RequiredSigningSpecs}
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, SynchronizerLimits}
import org.scalatest.wordspec.AnyWordSpec

/** This test ensures that [[StaticSynchronizerParameters]] cannot be deserialized from a proto
  * version that is not compatible with the protocol version defined in the parameters themselves.
  *
  * This verifies that we cannot stuff e.g. synchronizer parameters with PV=36 into a gRPC message
  * such as `InitializeSequencerFromGenesisStateV2Request` using the `v30` alternative and have it
  * deserialize cleanly.
  */
final class StaticSynchronizerParametersTest extends AnyWordSpec with BaseTest {

  private def mkSsp(protocolVersion: ProtocolVersion): StaticSynchronizerParameters = {
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
        synchronizerLimits = SynchronizerLimits.defaultFor(protocolVersion),
      )
      .value
  }

  "StaticSynchronizerParameters" should {
    "not be deserializable from protoV30 if PV=36" in {
      val ssp = mkSsp(ProtocolVersion.v36)

      StaticSynchronizerParameters.fromProtoV31(ssp.toProtoV31).value shouldBe ssp

      inside(StaticSynchronizerParameters.fromProtoV30(ssp.toProtoV30)) {
        case Left(InvariantViolation(_, error)) =>
          error should include(
            s"Synchronizer parameters with PV ${ProtocolVersion.v36} cannot be deserialized from ${ProtoVersion(30)}"
          )
        case other => fail(s"Unexpected result: $other")
      }
    }

    "not be deserializable from protoV31 if PV<36" in {
      val ssp = mkSsp(ProtocolVersion.v35)

      StaticSynchronizerParameters.fromProtoV30(ssp.toProtoV30).value shouldBe ssp

      inside(StaticSynchronizerParameters.fromProtoV31(ssp.toProtoV31)) {
        case Left(InvariantViolation(_, error)) =>
          error should include(
            s"Synchronizer parameters with PV ${ProtocolVersion.v35} cannot be deserialized from ${ProtoVersion(31)}"
          )
        case other => fail(s"Unexpected result: $other")
      }
    }
  }
}
