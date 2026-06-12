// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion as ApiHashingSchemeVersion
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.{
  HASHING_SCHEME_VERSION_V2,
  HASHING_SCHEME_VERSION_V3,
}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}

import java.util.UUID

final class InteractiveSubmissionVersioningIntegrationTest
    extends InteractiveSubmissionNuckSetupTest {
  "Interactive submission" should {

    val supportedHashingSchemeVersions: Set[HashingSchemeVersion] =
      HashingSchemeVersion.getHashingSchemeVersionsForProtocolVersion(testedProtocolVersion)
    forAll(supportedHashingSchemeVersions) { hashingSchemeVersion =>
      val expected = hashingSchemeVersion.toLedgerApiProto
      s"respect the requested version ($expected)" in { implicit env =>
        val command = createCycleCommand(aliceE, "c1")

        val prepared = epn.ledger_api.interactive_submission
          .prepare(Seq(aliceE), Seq(command), hashingSchemeVersion = expected)

        execAndWaitForTransaction(
          prepared,
          Map(aliceE.partyId -> env.global_secret.sign(prepared.preparedTransactionHash, aliceE)),
        )

        prepared.hashingSchemeVersion shouldBe expected

      }

      s"respect the requested version on the java-api ($expected)" in { implicit env =>
        val command = createCycleCommandJava(aliceE, "c1")

        val prepared = epn.ledger_api.javaapi.interactive_submission
          .prepare(Seq(aliceE), Seq(command), hashingSchemeVersion = expected)

        execAndWaitForTransaction(
          prepared,
          Map(aliceE.partyId -> env.global_secret.sign(prepared.preparedTransactionHash, aliceE)),
        )

        prepared.hashingSchemeVersion shouldBe expected
      }
    }

    "fail if unsupported hashing scheme version is used" onlyRunWith ProtocolVersion.v34 in {
      implicit env =>
        val command = createCycleCommand(aliceE, "c1")

        assertThrowsAndLogsCommandFailures(
          epn.ledger_api.interactive_submission
            .prepare(
              Seq(aliceE),
              Seq(command),
              hashingSchemeVersion = ApiHashingSchemeVersion.HASHING_SCHEME_VERSION_V3,
            ),
          le => {
            le.errorMessage should include regex "INVALID_ARGUMENT/FAILED_TO_PREPARE_TRANSACTION.*Hashing scheme version V3 is not supported on protocol version 34"
          },
        )
    }

    /*
     * This is the intended behavior because:
     * - Even from PV35, where LFS V2 is supported, a transaction / node will be assigned
     * LFS V2 only if it contains keys, otherwise LFSV1
     *
     * This means that HashingSchemeV2 keeps working on PV35 even for new transactions as long
     * as they don't contain keys (as tested above).
     */
    "fail to prepare a transaction on LF serialization V2 and hashing scheme V2" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        // Use a contract with key to force LFS V2
        val createBasicKeyCommand = contractWithKeyCreateCommand

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          cpn.ledger_api.javaapi.interactive_submission
            .prepare(
              Seq(aliceE),
              Seq(createBasicKeyCommand),
              hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
            ),
          _.errorMessage should include(
            "Cannot hash node with LF serialization version V2 using hashing scheme V2. Does the transaction use contract keys? Please using hashing scheme V3 or higher."
          ),
        )
    }

    "fail to execute a transaction on LF serialization V2 and reported hashing scheme V2" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        import env.*
        val createBasicKeyCommand = contractWithKeyCreateCommand

        // Get a V3 hash
        val prepared = cpn.ledger_api.javaapi.interactive_submission
          .prepare(
            Seq(aliceE),
            Seq(createBasicKeyCommand),
            hashingSchemeVersion = HASHING_SCHEME_VERSION_V3,
          )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.interactive_submission.execute_and_wait_for_transaction(
            prepared.getPreparedTransaction,
            Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
            submissionId = UUID.randomUUID().toString,
            // But incorrectly present it as a V2 Hash
            hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
          ),
          _.errorMessage should include(
            "Cannot hash node with LF serialization version V2 using hashing scheme V2. Does the transaction use contract keys? Please using hashing scheme V3 or higher."
          ),
        )
    }
  }
}
