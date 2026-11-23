// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.{Confidential, ConfidentialConfigWriter}
import com.typesafe.config.{ConfigFactory, ConfigObject}
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.{ConfigReader, ConfigWriter}

import scala.jdk.CollectionConverters.*

class ConfidentialTest extends AnyWordSpec with BaseTest {

  "HttpRequestHeaders" should {
    val raw = ConfigFactory.parseString(
      """Accept = "application/octet-stream"
        |authorization = "Bearer hunter2"
        |X-Custom-Token = "hunter3"""".stripMargin
    )

    val headers: Confidential.HttpRequestHeaders =
      ConfigReader[Confidential.HttpRequestHeaders].from(raw.root()).value

    def write(confidential: Boolean): Map[String, AnyRef] = {
      implicit val writer: ConfigWriter[Confidential.HttpRequestHeaders] =
        new ConfidentialConfigWriter(confidential).httpRequestHeadersWriter
      writer.to(headers) match {
        case obj: ConfigObject => obj.unwrapped().asScala.toMap
        case other => fail(s"Expected an object, got $other")
      }
    }

    "read a plain map and only mark credential headers as confidential" in {
      headers.exposed shouldBe Map("Accept" -> "application/octet-stream")
      headers.confidential shouldBe Map(
        "authorization" -> Confidential("Bearer hunter2"),
        "X-Custom-Token" -> Confidential("hunter3"),
      )
      headers.all shouldBe raw.root().unwrapped().asScala.toMap
    }

    "mask only the confidential headers when writing confidentially" in {
      write(confidential = true) shouldBe Map(
        "Accept" -> "application/octet-stream",
        "authorization" -> Confidential.Mask,
        "X-Custom-Token" -> Confidential.Mask,
      )
    }

    "round-trip through the flat format when writing non-confidentially" in {
      write(confidential = false) shouldBe raw.root().unwrapped().asScala.toMap
    }

    "hide the confidential headers in toString" in {
      headers.toString should not contain "hunter2"
    }
  }
}
