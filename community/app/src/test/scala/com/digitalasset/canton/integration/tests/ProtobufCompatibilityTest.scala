// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.File
import buf.alpha.image.v1.image.Image
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.tests.ProtobufCompatibilityTest.{
  protobufImageFileName,
  runCommand,
  runCommandWithLogger,
}
import com.digitalasset.canton.integration.tests.manual.{DataContinuityTest, S3Synchronization}
import com.digitalasset.canton.version.{AlphaProtoVersion, ReleaseVersion}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertions, Inside, LoneElement}
import scalapb.options.ScalapbProto

import java.nio.file.Files
import scala.sys.process.ProcessLogger

private object ProtobufCompatibilityTest {
  import Assertions.*
  val protobufImageFileName: String = "protobuf_image.bin.gz"
  val pvAnnotatedProtos = "protocol_version_annotated_protobufs.json"

  def runCommand(command: String, args: String*): Unit = {
    val processLogger = new BufferedProcessLogger()
    val exitCode = runCommandWithLogger(processLogger, command, args*)
    if (exitCode != 0) fail(processLogger.output(linePrefix = s"$command: "))
  }

  def runCommandWithLogger(logger: ProcessLogger, command: String, args: String*): Int =
    sys.process.Process(command, args.toSeq).!(logger)

  private val alphaProtoVersionName = classOf[AlphaProtoVersion].getName

  def removeAlphaProtoVersionMessagesFromImage(source: File, target: File): Unit = {
    // read the file
    val in = source.newGzipInputStream()
    val imageBytes = in.readAllBytes()
    in.close()

    // filter out messages tagged with AlphaProtoVersion
    val img = Image.parseFrom(imageBytes)
    val updatedImgFiles = img.file.map { imgFile =>
      // first identify messages that are tagged with AlphaProtoVersion
      val (nonAlphaMessages, alphaMessages) = imgFile.messageType.partition(
        _.options.forall(
          _.extension(ScalapbProto.message).forall(
            !_.companionExtends.contains(alphaProtoVersionName)
          )
        )
      )

      // If there are alpha messages, we also have to remove service methods that use the alpha messages,
      // otherwise the image is inconsisent with services pointing to messages that don't exist in the image.

      // The input and output type of methods convert the message names to the format .package.name.
      // The leading dot signals that they are used within the same proto file.
      // We currently don't detect a service method that uses a message tagged as alpha from a different proto file
      // as the request or the response type.
      // However, buf will fail regardless, because the post-processed image isn't consistent anymore.

      // the method's input/output type is in the format: .$package.$name
      val alphas = alphaMessages.map(desc => s".${imgFile.getPackage}.${desc.getName}").toSet

      val potentiallyUpdatedServices = if (alphas.nonEmpty) {
        imgFile.service.map(svc =>
          svc.copy(method = svc.method.filter { m =>
            // only retain methods that don't use messages as request/input or response/output that are tagged as alpha messages
            m.inputType.forall(!alphas.contains(_)) && m.outputType.forall(!alphas.contains(_))
          })
        )

      } else imgFile.service

      imgFile.copy(messageType = nonAlphaMessages, service = potentiallyUpdatedServices)
    }

    val updatedImg = img.copy(file = updatedImgFiles)

    // write the same file again
    val os = target.newGzipOutputStream()
    updatedImg.writeTo(os)
    os.finish()
  }
}

final class ProtobufCompatibilityWriterTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with LoneElement
    with S3Synchronization {

  "buf" should {

    // the protobuf image will be uploaded automatically during a release, via publish-data-continuity-dumps-to-s3.sh
    "generate the protobuf image for the data continuity dump" in {
      val dumpDir = DataContinuityTest.baseDbDumpPath / ReleaseVersion.current.fullVersion
      Files.createDirectories(dumpDir.path)

      // proto snapshot
      val protobufSnapshotForVersion = dumpDir.toTempFile(protobufImageFileName)

      if (protobufSnapshotForVersion.file.exists) {
        fail(s"Target file $protobufSnapshotForVersion already exists.")
      }

      runCommand("buf", "build", "-o", protobufSnapshotForVersion.path.toFile.getAbsolutePath)

      ProtobufCompatibilityTest.removeAlphaProtoVersionMessagesFromImage(
        source = protobufSnapshotForVersion.file,
        target = protobufSnapshotForVersion.file,
      )
    }
  }
}

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
    ),
    (3, 5) -> Seq(
      // LSU changes. Fine because not released yet.
      """com/digitalasset/canton/admin/participant/v30/synchronizer_connectivity_service.proto:Field "4" with name "sequencer_successors" on message "PerformManualLsuRequest" changed cardinality from "map" to "optional with explicit presence".""",
      """com/digitalasset/canton/admin/participant/v30/synchronizer_connectivity_service.proto:Field "4" with name "sequencer_successors" on message "PerformManualLsuRequest" moved from outside to inside a oneof.""",
      """com/digitalasset/canton/admin/participant/v30/synchronizer_connectivity_service.proto:Field "4" with name "sequencer_successors" on message "PerformManualLsuRequest" changed type from "com.digitalasset.canton.admin.participant.v30.PerformManualLsuRequest.SequencerSuccessorsEntry" to "com.digitalasset.canton.admin.participant.v30.PerformManualLsuRequest.SequencerSuccessors".""",
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
