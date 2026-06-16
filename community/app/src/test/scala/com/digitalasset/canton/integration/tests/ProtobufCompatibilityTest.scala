// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.File
import buf.alpha.image.v1.image.Image
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.tests.ProtobufCompatibilityTest.{
  protobufImageFileName,
  runCommand,
}
import com.digitalasset.canton.integration.tests.manual.{DataContinuityTest, S3Synchronization}
import com.digitalasset.canton.version.{AlphaProtoVersion, ReleaseVersion}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertions, Inside, LoneElement}
import scalapb.options.ScalapbProto

import java.nio.file.Files
import scala.sys.process.ProcessLogger

object ProtobufCompatibilityTest {
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
