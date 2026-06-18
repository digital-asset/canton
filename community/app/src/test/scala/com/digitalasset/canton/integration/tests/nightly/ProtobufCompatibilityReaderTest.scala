// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly

import better.files.File
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.tests.ProtobufCompatibilityTest
import com.digitalasset.canton.integration.tests.ProtobufCompatibilityTest.{
  protobufImageFileName,
  runCommandWithLogger,
}
import com.digitalasset.canton.integration.tests.manual.S3Synchronization
import com.digitalasset.canton.version.ReleaseVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Inside, LoneElement}

final class ProtobufCompatibilityReaderTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with LoneElement
    with S3Synchronization {

  // when adding an exception, replace `:<LINE>:<COLUMN>:` with `:` in the error message. For example:
  //
  // original buf error messages:
  // my.proto:17:5:Field "3" with name "bar" on message "Foo" changed type from "bytes" to "string".
  //
  // modified message:
  // my.proto:Field "3" with name "bar" on message "Foo" changed type from "bytes" to "string".
  //         ^ notice the removed line and column numbers
  val acceptedBreakingChangesByVersion = Map(
    (3, 4) -> Seq(
      // Contract id recomputation had to be removed
      """com/digitalasset/canton/admin/participant/v30/acs_import.proto:Previously present enum value "3" on enum "ContractImportMode" was deleted.""",
      """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Previously present field "1" with name "contract_id_mappings" on message "ImportAcsResponse" was deleted.""",
      // Internal classes that should have been marked as alpha/unstable
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "3" with name "message_id" on message "OrderingRequest" changed type from "bytes" to "string".""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "4" with name "payload" on message "OrderingRequest" changed cardinality from "optional with explicit presence" to "optional with implicit presence".""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "4" with name "payload" on message "OrderingRequest" changed type from "message" to "bytes".""",
      // LastErrorsAppender was removed, along with the corresponding Admin API endpoints
      """com/digitalasset/canton/admin/health/v30/status_service.proto:Previously present RPC "GetLastErrorTrace" on service "StatusService" was deleted.""",
      """com/digitalasset/canton/admin/health/v30/status_service.proto:Previously present RPC "GetLastErrors" on service "StatusService" was deleted.""",
      // Reviewed, expected breaking changes w.r.t. to 3.4.11
      """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Previously present RPC "ExportAcsOld" on service "ParticipantRepairService" was deleted.""",
      """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Previously present RPC "ImportAcsOld" on service "ParticipantRepairService" was deleted.""",
      """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Field "2" with name "workflow_id_prefix" on message "ImportAcsRequest" changed cardinality from "optional with implicit presence" to "optional with explicit presence".""",
      """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Field "3" with name "contract_import_mode" on message "ImportAcsRequest" changed cardinality from "optional with implicit presence" to "optional with explicit presence".""",
      """com/digitalasset/canton/admin/participant/v30/party_management_service.proto:Field "2" with name "synchronizer_id" on message "ImportPartyAcsRequest" changed cardinality from "optional with implicit presence" to "optional with explicit presence".""",
      """com/digitalasset/canton/admin/participant/v30/party_management_service.proto:Field "3" with name "workflow_id_prefix" on message "ImportPartyAcsRequest" changed cardinality from "optional with implicit presence" to "optional with explicit presence".""",
      """com/digitalasset/canton/admin/participant/v30/party_management_service.proto:Field "3" with name "workflow_id_prefix" on message "ImportPartyAcsRequest" changed type from "enum" to "string".""",
      """com/digitalasset/canton/admin/participant/v30/party_management_service.proto:Field "4" with name "contract_import_mode" on message "ImportPartyAcsRequest" changed type from "message" to "enum".""",
      """com/digitalasset/canton/mediator/admin/v30/mediator_inspection_service.proto:Field "1" with name "verdict" on message "VerdictsResponse" moved from outside to inside a oneof.""",
      """com/digitalasset/canton/protocol/v30/topology.proto:Field "17" with name "synchronizer_upgrade_announcement" on message "TopologyMapping" changed type from "com.digitalasset.canton.protocol.v30.SynchronizerUpgradeAnnouncement" to "com.digitalasset.canton.protocol.v30.LsuAnnouncement".""",
      """com/digitalasset/canton/protocol/v30/topology.proto:Field "18" with name "sequencer_connection_successor" on message "TopologyMapping" changed type from "com.digitalasset.canton.protocol.v30.SequencerConnectionSuccessor" to "com.digitalasset.canton.protocol.v30.LsuSequencerConnectionSuccessor".""",
      """com/digitalasset/canton/sequencer/admin/v30/sequencer_initialization_service.proto:Previously present RPC "InitializeSequencerFromPredecessor" on service "SequencerInitializationService" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present field "3" with name "failure" on message "HandshakeResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present field "2" with name "failure" on message "VerifyActiveResponse" was deleted.""",
      """com/digitalasset/canton/topology/admin/v30/topology_manager_read_service.proto:Previously present RPC "ListSequencerConnectionSuccessor" on service "TopologyManagerReadService" was deleted.""",
      """com/digitalasset/canton/topology/admin/v30/topology_manager_read_service.proto:Previously present RPC "ListSynchronizerUpgradeAnnouncement" on service "TopologyManagerReadService" was deleted.""",
      """com/digitalasset/canton/topology/admin/v30/topology_manager_read_service.proto:Previously present RPC "LogicalUpgradeState" on service "TopologyManagerReadService" was deleted.""",

      /// Backward compatibility
      """com/digitalasset/canton/admin/sequencer/v30/sequencer_connection.proto:Previously present field "3" with name "confirmation_response_factor" on message "SubmissionRequestAmplification" was deleted.""",
      """com/digitalasset/canton/admin/sequencer/v30/sequencer_connection.proto:Previously present field "4" with name "confirmation_response_patience" on message "SubmissionRequestAmplification" was deleted.""",
      """com/digitalasset/canton/crypto/v30/crypto.proto:Previously present enum value "5" on enum "SigningKeySpec" was deleted.""",
      """com/digitalasset/canton/crypto/v30/crypto.proto:Previously present enum value "4" on enum "SigningAlgorithmSpec" was deleted.""",
      """com/digitalasset/canton/mediator/admin/v30/mediator_inspection_service.proto:Previously present field "2" with name "complete" on message "VerdictsResponse" was deleted.""",
      """com/digitalasset/canton/mediator/admin/v30/mediator_inspection_service.proto:Previously present oneof "payload" on message "VerdictsResponse" was deleted.""",
      """com/digitalasset/canton/mediator/admin/v30/mediator_inspection_service.proto:Field "1" with name "verdict" on message "VerdictsResponse" moved from inside to outside a oneof.""",
      """com/digitalasset/canton/mediator/admin/v30/mediator_inspection_service.proto:Previously present field "4" with name "view_hash" on message "TransactionView" was deleted.""",
      """com/digitalasset/canton/participant/protocol/v30/submission_tracking.proto:Previously present field "6" with name "paid_traffic_cost" on message "CompletionInfo" was deleted.""",
      """com/digitalasset/canton/protocol/v30/synchronization.proto:Previously present field "7" with name "lsu_sequencing_test_message" on message "EnvelopeContent" was deleted.""",
      """com/digitalasset/canton/protocol/v30/topology.proto:Previously present enum value "2" on enum "ParticipantFeatureFlag" was deleted.""",
      """com/digitalasset/canton/protocol/v30/topology.proto:Field "17" with name "synchronizer_upgrade_announcement" on message "TopologyMapping" changed type from "com.digitalasset.canton.protocol.v30.LsuAnnouncement" to "com.digitalasset.canton.protocol.v30.SynchronizerUpgradeAnnouncement".""",
      """com/digitalasset/canton/protocol/v30/topology.proto:Field "18" with name "sequencer_connection_successor" on message "TopologyMapping" changed type from "com.digitalasset.canton.protocol.v30.LsuSequencerConnectionSuccessor" to "com.digitalasset.canton.protocol.v30.SequencerConnectionSuccessor".""",
      """com/digitalasset/canton/sequencer/admin/v30/sequencer_bft_administration_service.proto:Previously present field "3" with name "dynamic_sequencing_parameters_payload" on message "GetOrderingTopologyResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/admin/v30/sequencer_bft_administration_service.proto:Previously present field "4" with name "dynamic_sequencing_parameters_payload31" on message "GetOrderingTopologyResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/admin/v30/sequencer_bft_administration_service.proto:Previously present oneof "dynamic_sequencing_parameters" on message "GetOrderingTopologyResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_authentication_service.proto:Previously present field "3" with name "client_version" on message "ChallengeRequest" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present field "3" with name "client_version" on message "HandshakeRequest" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present reserved name "failure" on message "HandshakeResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present reserved range "[3]" on message "HandshakeResponse" is missing values: [3] were removed.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present reserved name "failure" on message "VerifyActiveResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto:Previously present reserved range "[2]" on message "VerifyActiveResponse" is missing values: [2] were removed.""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Previously present field "5" with name "ordering_start_instant" on message "OrderingRequest" was deleted.""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "3" with name "payload" on message "OrderingRequest" changed type from "string" to "bytes".""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "4" with name "ordering_start_instant" on message "OrderingRequest" changed cardinality from "optional with implicit presence" to "optional with explicit presence".""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "4" with name "ordering_start_instant" on message "OrderingRequest" changed type from "bytes" to "message".""",
      """com/digitalasset/canton/synchronizer/v30/synchronizer.proto:Previously present field "3" with name "is_late_upgrade" on message "SynchronizerPredecessor" was deleted.""",
      """com/digitalasset/canton/admin/health/v30/status_service.proto:Previously present field "3" with name "version" on message "NotInitialized" was deleted.""",
      """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Previously present field "2" with name "epoch_number" on message "BatchRequest" was deleted.""",
      // Added DABFT leaders and blacklisted nodes to `get_ordering_topology` console admin function's output
      """com/digitalasset/canton/sequencer/admin/v30/sequencer_bft_administration_service.proto:Previously present field "5" with name "leader_sequencer_ids" on message "GetOrderingTopologyResponse" was deleted.""",
      """com/digitalasset/canton/sequencer/admin/v30/sequencer_bft_administration_service.proto:Previously present field "6" with name "blacklisted_sequencer_ids" on message "GetOrderingTopologyResponse" was deleted.""",
      // Removal of LookupByKeyActionDescription
      """com/digitalasset/canton/protocol/v30/participant_transaction.proto:Previously present field "4" with name "lookup_by_key" on message "ActionDescription" was deleted.""",
      """com/digitalasset/canton/protocol/v30/participant_transaction.proto:Previously present reserved name "lookup_by_key" on message "ActionDescription" was deleted.""",
      """com/digitalasset/canton/protocol/v30/participant_transaction.proto:Previously present reserved range "[4]" on message "ActionDescription" is missing values: [4] were removed.""",
    ),
    (3, 5) -> Seq(
      // Changed for 3.5.1-rc4
      """com/digitalasset/daml/lf/archive/daml_lf2.proto:Previously present enum value "5001" on enum "BuiltinFunction" was deleted.""",
      """com/digitalasset/daml/lf/transaction.proto:Previously present field "1002" with name "external_call_results" on message "Exercise" was deleted.""",
      // Added ML-DSA support to 3.6, crypto handshake handles missing schemes with 3.5
      """com/digitalasset/canton/crypto/v30/crypto.proto:Previously present enum value "5" on enum "SigningKeySpec" was deleted.""",
      """com/digitalasset/canton/crypto/v30/crypto.proto:Previously present enum value "4" on enum "SigningAlgorithmSpec" was deleted.""",
      // Transaction snapshot does not need to be included in the forward/backward compatibility testing
      """<input>:1:1:Previously present file "com/digitalasset/daml/lf/snapshot.proto" was deleted.""",
      // Removal of LookupByKeyActionDescription
      """com/digitalasset/canton/protocol/v30/participant_transaction.proto:Previously present field "4" with name "lookup_by_key" on message "ActionDescription" was deleted.""",
      """com/digitalasset/canton/protocol/v30/participant_transaction.proto:Previously present reserved name "lookup_by_key" on message "ActionDescription" was deleted.""",
      """com/digitalasset/canton/protocol/v30/participant_transaction.proto:Previously present reserved range "[4]" on message "ActionDescription" is missing values: [4] were removed.""",
      // removal of lookup_by_key from Update
      """com/digitalasset/daml/lf/archive/daml_lf2.proto:Previously present field "8" with name "lookup_by_key" on message "Update" was deleted.""",
      """com/digitalasset/daml/lf/archive/daml_lf2.proto:Previously present reserved range "[8]" on message "Update" is missing values: [8] were removed.""",
      // Import package change only: com.digitalasset.canton.admin.topology -> com.digitalasset.canton.topology.admin
      """com/digitalasset/canton/admin/participant/v30/synchronizer_connectivity_service.proto:Field "5" with name "synchronizer_predecessor" on message "Result" changed type from "com.digitalasset.canton.admin.topology.v30.SynchronizerPredecessor" to "com.digitalasset.canton.topology.admin.v30.SynchronizerPredecessor".""",
      // Added `subscription_liveness_limits` to `SequencerConnections` for silent subscription detection
      """com/digitalasset/canton/admin/sequencer/v30/sequencer_connection.proto:Previously present field "6" with name "subscription_liveness_limits" on message "SequencerConnections" was deleted.""",
    ),
  )

  "protobuf" should {

    val current = ReleaseVersion.current

    // Supported versions are >= 3.4.
    // For 3.6.x, this results in List((3, 4), (3, 5))
    val previousSupportedMinors = (4 until current.minor).map(m => (current.major, m)).toList

    // Test against previous patch versions of the current release AND all previously supported minors
    val versionsToTest =
      Option(current)
        .filter(_.patch > 0)
        .map(_.majorMinor)
        .toList ::: previousSupportedMinors

    versionsToTest.foreach { majorMinor =>
      S3Dump.getDumpBaseDirectoriesForVersion(Some(majorMinor)).foreach { case (dumpRef, version) =>
        s"be compatible with $version" in {
          val protoImageFile = dumpRef.localDownloadPath / protobufImageFileName

          File.temporaryFile(suffix = protobufImageFileName) { protoImageFileWithoutAlphaMessages =>
            ProtobufCompatibilityTest.removeAlphaProtoVersionMessagesFromImage(
              source = protoImageFile,
              target = protoImageFileWithoutAlphaMessages,
            )
            // run the compatibility test against the buf image that doesn't contain alpha messages
            val processLogger = new BufferedProcessLogger()
            val _ = runCommandWithLogger(
              processLogger,
              "scripts/ci/buf-checks.sh",
              protoImageFileWithoutAlphaMessages.toJava.getAbsolutePath,
            )
            val errorOutput =
              processLogger.outputLines().map(_.replaceAll(raw".proto:\d+:\d+:", ".proto:"))

            val acceptedBreakingChanges = acceptedBreakingChangesByVersion(majorMinor)

            val unacceptableBreakages = errorOutput.filter(!acceptedBreakingChanges.contains(_))
            if (unacceptableBreakages.nonEmpty) {
              fail(
                s"""Detected a backwards breaking change in a protobuf file.
                   |Check with release owners whether this breaking change is acceptable or not.
                   |If it IS acceptable, add the entire line (removing the line number + column) to `acceptedBreaking` in this test.
                   |
                   |Breaking changes:
                   |${unacceptableBreakages.mkString("\n")}
                   |""".stripMargin
              )
            }

            val superfluousExceptions = acceptedBreakingChanges.filter(!errorOutput.contains(_))
            if (superfluousExceptions.nonEmpty) {
              fail(
                s"""Some exceptions in `acceptedBreakingChanges` are not valid anymore. Remove them from the list.
                   |
                   |${superfluousExceptions.mkString("\n")}""".stripMargin
              )
            }
          }
          succeed
        }
      }
    }
  }
}
