// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.{BaseTest, LfPackageId}
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser, Decode, Error as LfArchiveError}
import com.digitalasset.daml.lf.engine.{EngineLoggingConfig, Error as EngineError}
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

/** The cache is JVM-wide, so these tests do not assume it starts empty. */
class PackageProcessingCacheTest extends AnyWordSpec with BaseTest {

  private def readDar(path: String) =
    DarParser
      .readArchiveFromFile(new File(path))
      .valueOrFail(s"read $path")

  private lazy val examples = readDar(BaseTest.CantonExamplesPath)

  private lazy val engine = DAMLe.newEngine(
    enableLfDev = false,
    enableLfBeta = false,
    enableStackTraces = false,
    paranoidMode = true,
    submissionPhaseLogging = EngineLoggingConfig(),
    validationPhaseLogging = EngineLoggingConfig(),
    loggerFactory = loggerFactory,
  )

  "decodeArchive" should {
    "return the result of Decode.decodeArchive, and the same instance for an identical archive" in {
      val archive = examples.main
      val first = PackageProcessingCache.decodeArchive(archive).value
      first shouldBe Decode.decodeArchive(archive).value

      // A separately parsed copy of the same archive bytes hits the memo.
      val copy = DamlLf.Archive.parseFrom(archive.toByteArray)
      val second = PackageProcessingCache.decodeArchive(copy).value
      second._2 should be theSameInstanceAs first._2
    }

    "reject an archive whose payload does not match its hash, every time" in {
      val genuine = examples.main
      val other = examples.dependencies.headOption.value
      val tampered = genuine.toBuilder.setPayload(other.getPayload).build()

      // Decode the genuine archive first, so that a memo keyed by hash alone would wrongly hit.
      PackageProcessingCache.decodeArchive(genuine).isRight shouldBe true
      (1 to 2).foreach { _ =>
        PackageProcessingCache.decodeArchive(tampered).left.value shouldBe a[LfArchiveError.Parsing]
      }
    }

    "not let a payload with a different hash field hit the entry of another archive" in {
      val genuine = examples.main
      val sameBytesOtherHash = genuine.toBuilder.setHash("0" * 64).build()
      PackageProcessingCache.decodeArchive(genuine).isRight shouldBe true
      PackageProcessingCache.decodeArchive(sameBytesOtherHash).left.value shouldBe a[
        LfArchiveError.Parsing
      ]
    }
  }

  "the typecheck memo" should {
    "only skip the typecheck of packages it was told passed" in {
      val illformed = readDar(PackageServiceTest.badDarPath)
      val decoded = illformed.map(PackageProcessingCache.decodeArchive(_).value)
      val ids: Set[LfPackageId] = decoded.all.map(_._1).toSet

      // Without trust, the ill-formed package fails the typecheck ...
      engine.validateDar(decoded, alreadyTypechecked = _ => false).left.value shouldBe a[
        EngineError.Package.Validation
      ]
      // ... and failures are never recorded, so the memo does not trust it either.
      ids.foreach(id => PackageProcessingCache.isTypechecked(id) shouldBe false)
      engine
        .validateDar(decoded, alreadyTypechecked = PackageProcessingCache.isTypechecked)
        .left
        .value shouldBe a[EngineError.Package.Validation]

      // The predicate is what skips the typecheck; the other checks still run.
      engine.validateDar(decoded, alreadyTypechecked = ids.contains) shouldBe Right(())
    }

    "record package ids that were marked" in {
      val decoded = examples.map(PackageProcessingCache.decodeArchive(_).value)
      engine.validateDar(decoded, alreadyTypechecked = _ => false) shouldBe Right(())
      PackageProcessingCache.markTypechecked(decoded.all.map(_._1))
      decoded.all.foreach(pkg => PackageProcessingCache.isTypechecked(pkg._1) shouldBe true)
      engine.validateDar(
        decoded,
        alreadyTypechecked = PackageProcessingCache.isTypechecked,
      ) shouldBe Right(())
    }
  }
}
