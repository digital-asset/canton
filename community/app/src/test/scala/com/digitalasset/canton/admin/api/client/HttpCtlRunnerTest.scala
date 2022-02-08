// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import cats.implicits._
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.commands.HttpAdminCommand
import com.digitalasset.canton.config.ProcessingTimeout
import org.scalatest.wordspec.AsyncWordSpec

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class HttpCtlRunnerTest extends AsyncWordSpec with BaseTest {

  "Runner" when {
    "running a successful command" should {

      val command = new HttpAdminCommand.Stub[String]("command", "result")

      "run successfully" in {
        new HttpCtlRunner(loggerFactory).run(
          "participant1",
          command,
          new java.net.URL("http://foo.bar"),
          null,
          ProcessingTimeout(),
        ) map { result =>
          result shouldBe "result"
        }
      }
    }
  }
}
