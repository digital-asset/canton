// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.infrastructure

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.benchtool.util.SimpleFileReader
import com.daml.lf.archive.{Dar, DarParser}
import com.daml.lf.data.Ref
import com.daml.testing.utils.{TestModels, TestResourceUtils}
import com.google.protobuf.ByteString

import scala.util.chaining.*
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object TestDars {
  private val TestDarInfix = "benchtool"
  private lazy val resources: List[String] = List(
    TestModels.com_daml_ledger_test_BenchtoolTestDar_1_15_path
  )

  val benchtoolDar: Dar[DamlLf.Archive] = TestModels.com_daml_ledger_test_BenchtoolTestDar_1_15_path
    .pipe(TestResourceUtils.resourceFileFromJar)
    .pipe(DarParser.assertReadArchiveFromFile)

  val benchtoolDarPackageId: Ref.PackageId =
    Ref.PackageId.assertFromString(benchtoolDar.main.getHash)

  def readAll(): Try[List[DarFile]] = {
    (TestDars.resources
      .foldLeft[Try[List[DarFile]]](Success(List.empty)) { (acc, resourceName) =>
        for {
          dars <- acc
          bytes <- SimpleFileReader.readResource(resourceName)
        } yield DarFile(resourceName, bytes) :: dars
      })
      .recoverWith { case NonFatal(ex) =>
        Failure(TestDarsError(s"Reading test dars failed. Details: ${ex.getLocalizedMessage}", ex))
      }
  }

  final case class TestDarsError(message: String, cause: Throwable)
      extends Exception(message, cause)

  final case class DarFile(name: String, bytes: ByteString)
}
