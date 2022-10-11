// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import better.files.*
import cats.data.EitherT
import com.daml.daml_lf_dev.DamlLf
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.definitions.PackageServiceError
import com.daml.lf.CantonOnly
import com.daml.lf.archive.DarParser
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{String255, String256M}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.participant.LedgerSyncEvent
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.admin.PackageServiceTest.readCantonExamples
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.Future

object PackageServiceTest {

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def loadExampleDar() =
    DarParser
      .readArchiveFromFile(new File(BaseTest.CantonExamplesPath))
      .getOrElse(throw new IllegalArgumentException("Failed to read dar"))

  def readCantonExamples(): List[DamlLf.Archive] = {
    loadExampleDar().all
  }

  def readCantonExamplesBytes(): Array[Byte] =
    Files.readAllBytes(Paths.get(BaseTest.CantonExamplesPath))

  def badDarPath: String =
    ("community" / "participant" / "src" / "test" / "resources" / "daml" / "illformed.dar").toString
}

class PackageServiceTest extends AsyncWordSpec with BaseTest {
  private val examplePackages: List[Archive] = readCantonExamples()
  private val bytes = PackageServiceTest.readCantonExamplesBytes()
  val darName = String255.tryCreate("CantonExamples")
  private val eventPublisher = mock[ParticipantEventPublisher]
  when(eventPublisher.publish(any[LedgerSyncEvent])(anyTraceContext)).thenAnswer(Future.unit)
  val participantId = DefaultTestIdentities.participant1

  private class Env {
    val packageStore = new InMemoryDamlPackageStore(loggerFactory)
    val engine = CantonOnly.newDamlEngine(uniqueContractKeys = false, enableLfDev = false)
    val sut =
      new PackageService(
        engine,
        packageStore,
        eventPublisher,
        new SymbolicPureCrypto,
        _ => EitherT.rightT(()),
        new PackageInspectionOpsForTesting(participantId, loggerFactory),
        ProcessingTimeout(),
        loggerFactory,
      )
  }

  private def withEnv[T](test: Env => Future[T]): Future[T] = {
    val env = new Env()
    test(env)
  }

  lazy val cantonExamplesDescription = String256M.tryCreate("CantonExamples")

  "PackageService" should {
    "append DAR and packages from file" in withEnv { env =>
      import env.*

      val expectedPackageIdsAndState = examplePackages
        .map(DamlPackageStore.readPackageId)
        .map(PackageDescription(_, cantonExamplesDescription))
      val payload = BinaryFileUtil
        .readByteStringFromFile(CantonExamplesPath)
        .valueOrFail("could not load examples")
      for {
        hash <- sut
          .appendDarFromByteString(
            payload,
            "CantonExamples",
            vetAllPackages = false,
            synchronizeVetting = false,
          )
          .value
          .map(_.valueOrFail("append dar"))
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain.only(expectedPackageIdsAndState: _*)
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "append DAR and packages from bytes" in withEnv { env =>
      import env.*

      val expectedPackageIdsAndState = examplePackages
        .map(DamlPackageStore.readPackageId)
        .map(PackageDescription(_, cantonExamplesDescription))

      for {
        hash <- sut
          .appendDarFromByteString(
            ByteString.copyFrom(bytes),
            "some/path/CantonExamples.dar",
            false,
            false,
          )
          .value
          .map(_.valueOrFail("should be right"))
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain.only(expectedPackageIdsAndState: _*)
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "fetching dependencies" in withEnv { env =>
      import env.*

      val dar = PackageServiceTest.loadExampleDar()
      val mainPackageId = DamlPackageStore.readPackageId(dar.main)
      val dependencyIds = com.daml.lf.archive.Decode.assertDecodeArchive(dar.main)._2.directDeps
      for {
        _ <- sut
          .appendDarFromByteString(
            ByteString.copyFrom(bytes),
            "some/path/CantonExamples.dar",
            false,
            false,
          )
          .valueOrFail("appending dar")
        deps <- sut.packageDependencies(List(mainPackageId)).value
      } yield {
        // test for explict dependencies
        deps match {
          case Left(value) => fail(value)
          case Right(loaded) =>
            // all direct dependencies should be part of this
            (dependencyIds -- loaded) shouldBe empty
        }
      }
    }

    "appendDar validates the package" in withEnv { env =>
      import env.*

      val badDarPath = PackageServiceTest.badDarPath
      val payload = BinaryFileUtil
        .readByteStringFromFile(badDarPath)
        .valueOrFail(s"could not load bad dar file at $badDarPath")
      for {
        error <- leftOrFail(
          sut.appendDarFromByteString(
            payload,
            badDarPath,
            vetAllPackages = false,
            synchronizeVetting = false,
          )
        )("append illformed.dar")
      } yield {
        error match {
          case validation: PackageServiceError.Validation.ValidationError.Error =>
            validation.validationError shouldBe a[com.daml.lf.validation.ETypeMismatch]
          case _ => fail(s"$error is not a validation error")
        }
      }
    }
  }
}
