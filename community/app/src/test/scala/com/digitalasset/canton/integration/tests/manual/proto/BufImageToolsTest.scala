// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.proto

import better.files.File
import buf.alpha.image.v1.image.Image
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.MessageCategory.*
import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.{
  MessageCategory,
  RequestType,
  ResponseType,
}
import com.digitalasset.canton.util.JarResourceUtils
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AnyWordSpec

/* Ensure that the tools that we have to parse and trim buf images work correctly.
 *
 * Useful commands
 * ---------------
 *
 * Creating the buf image:
 *   - Add `community/common/src/test/protobuf` to `bug.work.yaml`
 *   - Run `buf build -o protocontinuity_v30.bin.gz --path community/common/src/test/protobuf/com/digitalasset/canton/test/protocontinuity/v30/main.proto`
 *
 * Converting a binary buf image to json:
 *   - `buf build input.bin.gz -o output.json`
 */
final class BufImageToolsTest extends AnyWordSpec with BaseTest {
  private def trimPkgName(str: String) =
    str.replace("com.digitalasset.canton.test.protocontinuity.v30.", "")

  // Remove package name for strings to increase readability
  private def trimPkgNames(structure: BufImageStructure): BufImageStructure = structure
    .focus(_.forest)
    .modify(_.map { case (key, values) => trimPkgName(key) -> values.map(trimPkgName) })
    .focus(_.messageCategories)
    .modify(_.map { case (key, values) => trimPkgName(key) -> values })
    .focus(_.roots)
    .modify(_.map(trimPkgName))
    .focus(_.endpoints)
    .modify(_.map { case (key, (request, response)) =>
      trimPkgName(key) -> (RequestType(trimPkgName(request.str)), ResponseType(
        trimPkgName(response.str)
      ))
    })

  private val expectedForest: Map[String, Set[String]] = Map(
    "Alpha" -> Set(),
    "EndpointRequest" -> Set("Storage0", "ReqResp"),
    "EndpointResponse" -> Set("ReqResp"),
    "Endpoint2Request" -> Set(),
    "Endpoint2Request.Endpoint2Response" -> Set(),
    "IsolatedStable" -> Set(),
    "Msg0" -> Set(),
    "Msg3" -> Set(),
    "ReqResp" -> Set("ReqResp.Msg2", "ReqResp.Msg2.Msg4"),
    "ReqResp.Msg2" -> Set("Stable2"),
    "ReqResp.Msg2.Msg4" -> Set(),
    "Stable1" -> Set(),
    "Stable2" -> Set("Msg3"),
    "Storage0" -> Set("Msg0", "Storage0.OneOf"),
    "Storage0.OneOf" -> Set("Stable1", "Storage0.Storage1"),
    "Storage0.Storage1" -> Set(),
  )

  /*
  The image is generated from the data in
    community/common/src/test/protobuf/com/digitalasset/canton/test/protocontinuity/v30/main.proto
   */
  private def readBufImage(): Array[Byte] = {
    val in =
      File(JarResourceUtils.resourceFile("protocontinuity_old.bin.gz").toURI).newGzipInputStream()
    val imageBytes = in.readAllBytes()
    in.close()
    imageBytes
  }

  "Buf image tools" should {
    "parse binary" in {
      val bufImg: Image = Image.parseFrom(readBufImage())

      val structure: BufImageStructure = trimPkgNames(
        new BufImageParser(bufImg).parse()
      )

      structure shouldBe BufImageStructure(
        forest = expectedForest,
        messageCategories = Map(
          "Alpha" -> Set(Alpha),
          "EndpointRequest" -> Set(Request),
          "EndpointResponse" -> Set(Response),
          "Endpoint2Request" -> Set(Request),
          "Endpoint2Request.Endpoint2Response" -> Set(Response),
          "IsolatedStable" -> Set(Stable),
          "Msg0" -> Set(Request, Storage),
          "Msg3" -> Set(Request, Response, Stable),
          "ReqResp.Msg2.Msg4" -> Set(Request, Response),
          "ReqResp.Msg2" -> Set(Request, Response),
          "ReqResp" -> Set(Request, Response),
          "Stable1" -> Set(Request, Storage, Stable),
          "Stable2" -> Set(Request, Response, Stable),
          "Storage0" -> Set(Request, Storage),
          "Storage0.OneOf" -> Set(Request, Storage),
          "Storage0.Storage1" -> Set(Request, Storage),
        ),
        roots = Set(
          "EndpointRequest",
          "EndpointResponse",
          "Endpoint2Request",
          "Endpoint2Request.Endpoint2Response",
          "IsolatedStable",
          "Alpha",
        ),
        endpoints = Map(
          "ProtoContinuityService.Endpoint" -> (RequestType("EndpointRequest"), ResponseType(
            "EndpointResponse"
          )),
          "ProtoContinuityService.Endpoint2" -> (RequestType("Endpoint2Request"), ResponseType(
            "Endpoint2Request.Endpoint2Response"
          )),
        ),
      )
    }

    "create sub-images for messages categories" in {
      val img: Image = Image.parseFrom(readBufImage())
      val structure: BufImageStructure = new BufImageParser(img).parse()

      /*
      - Trim the image to keep only one category
      - Write into a file
      - Read the structure of the written file
       */
      def trimImage(keep: MessageCategory): BufImageStructure = {
        val updatedImg = new BufImageTrimmer(img, structure).trim(keep)
        val file = File.newTemporaryFile(prefix = s"buf_trim_${keep}_")
        logger.info(s"Writing trimmed image in $file")

        val os = file.newGzipOutputStream()
        updatedImg.writeTo(os)
        os.finish()

        val in = file.newGzipInputStream()
        val imageBytes = in.readAllBytes()
        in.close()

        val bufImg: Image = Image.parseFrom(imageBytes)
        trimPkgNames(new BufImageParser(bufImg).parse())
      }

      /*
      We don't assert on categories because:
      - They are a mean to an end.
      - The results are a bit surprising when trimming (e.g., we loose the request/response information since services are deleted)
       */

      val emptyServices = Map.empty[String, (String, String)]
      inside(trimImage(Storage)) { case BufImageStructure(forest, _, roots, `emptyServices`) =>
        roots shouldBe Set("Storage0")
        forest shouldBe Map(
          "Msg0" -> Set(),
          "Stable1" -> Set(),
          "Storage0" -> Set("Msg0", "Storage0.OneOf"),
          "Storage0.OneOf" -> Set("Stable1", "Storage0.Storage1"),
          "Storage0.Storage1" -> Set(),
        )
      }

      inside(trimImage(Stable)) { case BufImageStructure(forest, _, roots, `emptyServices`) =>
        roots shouldBe Set("Stable1", "Stable2", "IsolatedStable")
        forest shouldBe Map(
          "Stable1" -> Set(),
          "Stable2" -> Set("Msg3"),
          "IsolatedStable" -> Set(),
          "Msg3" -> Set(),
        )
      }

      inside(trimImage(Request)) { case BufImageStructure(forest, _, roots, `emptyServices`) =>
        roots shouldBe Set("EndpointRequest", "Endpoint2Request")
        forest shouldBe expectedForest -- Seq(
          "EndpointResponse",
          "Endpoint2Request.Endpoint2Response",
          "IsolatedStable",
          "Alpha",
        )
      }

      inside(trimImage(Response)) { case BufImageStructure(forest, _, roots, `emptyServices`) =>
        /*
        Endpoint2Request remains because it contains Endpoint2Response.
        Since it does not have any parent, it is also a root.
         */
        roots shouldBe Set(
          "EndpointResponse",
          "Endpoint2Request",
          "Endpoint2Request.Endpoint2Response",
        )

        val remove = Seq(
          "EndpointRequest",
          "IsolatedStable",
          "Alpha",
          "Storage0",
          "Msg0",
          "Stable1",
          "Storage0.Storage1",
          "Storage0.OneOf",
        )

        forest shouldBe expectedForest -- remove
      }

      inside(trimImage(Alpha)) { case BufImageStructure(forest, _, roots, `emptyServices`) =>
        roots shouldBe Set("Alpha")
        forest shouldBe Map("Alpha" -> Set())
      }
    }

    "create sub-images for services" in {
      val img: Image = Image.parseFrom(readBufImage())
      val structure: BufImageStructure = new BufImageParser(img).parse()

      /*
       - Trim image to keep only data that allows to detect removed endpoints in services
       - Write into a file
       - Read the structure of the written file
       */
      def createServiceImage(): BufImageStructure = {
        val updatedImg = new BufImageService(img, structure).create()
        val file = File.newTemporaryFile(prefix = s"buf_trim_service_")
        logger.info(s"Writing trimmed image in $file")

        val os = file.newGzipOutputStream()
        updatedImg.writeTo(os)
        os.finish()

        val in = file.newGzipInputStream()
        val imageBytes = in.readAllBytes()
        in.close()

        val bufImg: Image = Image.parseFrom(imageBytes)
        trimPkgNames(new BufImageParser(bufImg).parse())
      }

      inside(createServiceImage()) { case BufImageStructure(forest, _, roots, endpoints) =>
        roots shouldBe Set(
          "EndpointRequest",
          "EndpointResponse",
          "Endpoint2Request",
          "Endpoint2Request.Endpoint2Response",
        )
        forest shouldBe Map(
          "EndpointRequest" -> Set(),
          "EndpointResponse" -> Set(),
          "Endpoint2Request" -> Set(),
          "Endpoint2Request.Endpoint2Response" -> Set(),
        )
        endpoints shouldBe Map(
          "ProtoContinuityService.Endpoint" -> (RequestType("EndpointRequest"), ResponseType(
            "EndpointResponse"
          )),
          "ProtoContinuityService.Endpoint2" -> (RequestType("Endpoint2Request"), ResponseType(
            "Endpoint2Request.Endpoint2Response"
          )),
        )
      }
    }
  }
}
