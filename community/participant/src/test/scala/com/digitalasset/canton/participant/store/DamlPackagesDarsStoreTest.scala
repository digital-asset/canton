// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.admin.PackageServiceTest.readCantonExamples
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import scala.concurrent.Future

trait DamlPackagesDarsStoreTest {
  this: AsyncWordSpec with BaseTest =>

  protected implicit def traceContext: TraceContext

  def damlPackageStore(mk: () => DamlPackageStore): Unit = {
    val damlPackages = readCantonExamples()
    val damlPackage = damlPackages(0)
    val packageId = DamlPackageStore.readPackageId(damlPackage)
    val damlPackage2 = damlPackages(1)
    val packageId2 = DamlPackageStore.readPackageId(damlPackage2)

    val darName = String255.tryCreate("CantonExamples")
    val darPath = this.getClass.getClassLoader.getResource(darName.unwrap + ".dar").getPath
    val darFile = new File(darPath)
    val darData = Files.readAllBytes(darFile.toPath)
    val hash = TestHash.digest("hash")

    "save and retrieve dar" in {
      val store = mk()
      for {
        _ <- store.append(
          List(damlPackage),
          "test",
          Some(Dar(DarDescriptor(hash, darName), darData)),
        )
        result <- store.getDar(hash)
        pkg <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(Dar(DarDescriptor(hash, darName), darData))
        pkg shouldBe Some(damlPackage)
      }
    }

    "list persisted dar hashes and filenames" in {
      val store = mk()
      for {
        _ <- store.append(
          List(damlPackage),
          "test",
          Some(Dar(DarDescriptor(hash, darName), darData)),
        )
        result <- store.listDars()
      } yield result should contain only DarDescriptor(hash, darName)
    }

    "preserve appended dar data even if the original one has the data modified" in {
      val store = mk()
      val dar = Dar(DarDescriptor(hash, darName), "dar contents".getBytes)
      for {
        _ <- store.append(List(damlPackage), "test", Some(dar))
        _ = ByteBuffer.wrap(dar.bytes).put("stuff".getBytes)
        result <- store.getDar(hash)
      } yield result shouldBe Some(Dar(DarDescriptor(hash, darName), "dar contents".getBytes))
    }

    "be able to persist the same dar many times at the same time" in {
      val store = mk()
      val dar = Dar(DarDescriptor(hash, darName), "dar contents".getBytes)
      for {
        _ <- Future.sequence(
          (0 until 4).map(_ => store.append(List(damlPackage), "test", Some(dar)))
        )
        result <- store.getDar(hash)

        pkg <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(Dar(DarDescriptor(hash, darName), "dar contents".getBytes))
        pkg shouldBe Some(damlPackage)
      }
    }

    "save and retrieve one Daml Package" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), "test", None)
        result <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(damlPackage)
      }
    }

    "save and retrieve multiple Daml Packages" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage, damlPackage2), "test", None)
        resPkg2 <- store.getPackage(packageId2)
        resPkg1 <- store.getPackage(packageId)
      } yield {
        resPkg1 shouldBe Some(damlPackage)
        resPkg2 shouldBe Some(damlPackage2)
      }
    }

    "list package id and state of stored packages" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), "test", None)
        result <- store.listPackages()
      } yield result should contain only PackageDescription(packageId, "test")
    }

    "be able to persist the same package many times at the same time" in {
      val store = mk()
      for {
        _ <- Future.sequence((0 until 4).map(_ => store.append(List(damlPackage), "test", None)))
        result <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(damlPackage)
      }
    }

    "list package ids by state" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), "test", None)
        result <- store.listPackages()
      } yield result.loneElement shouldBe PackageDescription(packageId, "test")
    }

    "list package id and state of stored packages where sourceDescription is empty" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), "", None)
        result <- store.listPackages()
      } yield result should contain only PackageDescription(packageId, "default")
    }

    "list several packages with a limit" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), "test", None)
        _ <- store.append(List(damlPackage2), "test2", None)
        result1 <- store.listPackages(Some(1))
        result2 <- store.listPackages(Some(2))
      } yield {
        result1.loneElement should (
          be(PackageDescription(packageId, "test")) or
            be(PackageDescription(packageId2, "test2"))
        )
        result2.toSet shouldBe Set(
          PackageDescription(packageId, "test"),
          PackageDescription(packageId2, "test2"),
        )
      }
    }

  }

}
