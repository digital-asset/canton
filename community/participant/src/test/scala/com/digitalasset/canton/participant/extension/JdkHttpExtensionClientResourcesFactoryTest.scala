// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import org.scalatest.wordspec.AnyWordSpec

import java.net.http.HttpClient
import java.time.Duration

class JdkHttpExtensionClientResourcesFactoryTest extends AnyWordSpec with BaseTest {

  private def makeConfig(
      name: String,
      useTls: Boolean = true,
      tlsInsecure: Boolean = false,
      connectTimeoutMs: Long = 500L,
      port: Int = 8443,
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = useTls,
      tlsInsecure = tlsInsecure,
      connectTimeout = NonNegativeFiniteDuration.ofMillis(connectTimeoutMs),
    )

  "JdkHttpExtensionClientResourcesFactory.settingsFor" should {

    "mark secure TLS configs as not insecure" in {
      val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "secure", useTls = true, tlsInsecure = false)
      )

      settings.version shouldBe HttpClient.Version.HTTP_1_1
      settings.insecureTls shouldBe false
    }

    "mark insecure TLS configs as insecure" in {
      val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "insecure", useTls = true, tlsInsecure = true)
      )

      settings.insecureTls shouldBe true
    }

    "preserve the per-extension connect timeout" in {
      val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "timeout", connectTimeoutMs = 1234L)
      )

      settings.connectTimeout shouldBe Duration.ofMillis(1234L)
    }

    "handle mixed secure and insecure configs independently" in {
      val secureSettings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "secure", useTls = true, tlsInsecure = false, connectTimeoutMs = 500L)
      )
      val insecureSettings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "insecure", useTls = true, tlsInsecure = true, connectTimeoutMs = 1500L)
      )

      secureSettings.insecureTls shouldBe false
      secureSettings.connectTimeout shouldBe Duration.ofMillis(500L)
      insecureSettings.insecureTls shouldBe true
      insecureSettings.connectTimeout shouldBe Duration.ofMillis(1500L)
    }
  }
}
