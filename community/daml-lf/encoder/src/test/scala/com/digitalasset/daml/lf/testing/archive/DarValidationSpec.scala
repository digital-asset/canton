// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.encoder

import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.language.LanguageVersion as LV
import com.digitalasset.daml.lf.validation.Validation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class DarValidationSpec extends AnyFlatSpec with Matchers {

  private val versions = LV.allLfVersions.map(_.pretty)

  for (version <- versions) {

    s"Generated DAR for version $version" should "pass REPL validation" in {
      val resourceName = s"test-$version.dar"
      val resource = getClass.getClassLoader.getResource(resourceName)

      withClue(s"Could not find $resourceName in classpath. Did the plugin generate it?") {
        resource should not be null
      }

      val dar = DarDecoder.readArchiveFromFile(new File(resource.toURI)) match {
        case Right(value) => value
        case Left(err) => fail(s"Failed to decode $resourceName: $err")
      }

      Validation.checkPackages(dar.all.toMap) match {
        case Right(()) => succeed
        case Left(err) => fail(s"Validation failed for $resourceName: $err")
      }
    }
  }
}
