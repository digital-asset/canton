// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.encoder

import com.digitalasset.daml.lf.archive.DarDecoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import com.digitalasset.daml.lf.language.{LanguageVersion => LV}
import com.digitalasset.daml.lf.validation.Validation

class DarValidationSpec extends AnyFlatSpec with Matchers {

  private val versions = LV.allLfVersions.map(_.pretty)

  for (version <- versions) {

    s"Generated DAR for version $version" should "pass REPL validation" in {
      val resourceName = s"test-${version}.dar"
      val resource = getClass.getClassLoader.getResource(resourceName)

      withClue(s"Could not find $resourceName in classpath. Did the plugin generate it?") {
        resource should not be null
      }

      val Right(dar) = DarDecoder.readArchiveFromFile(new File(resource.toURI))

      val result = Validation.checkPackages(dar.all.toMap)

      result shouldBe Right(())
    }
  }
}
