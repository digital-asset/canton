// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.archive.{Dar, DarDecoder}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.speedy.Compiler.{Config, NoPackageValidation, compilePackages}
import org.openjdk.jmh.annotations.*

import java.io.File

@State(Scope.Benchmark)
class SpeedyCompilationBench {

  private val com_daml_ledger_test_ModelTestDar_path =
    "./target/scala-2.13/classes/model-tests-3.1.0.dar"

  @Param(Array(""))
  var darPath: String = _

  private var dar: Dar[(PackageId, Package)] = _
  private var darMap: Map[PackageId, Package] = _
  private var pkgInterface: PackageInterface = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val darFile =
      if (darPath.isEmpty) {
        java.nio.file.Paths.get(com_daml_ledger_test_ModelTestDar_path).toAbsolutePath.toFile
      } else {
        new File(darPath)
      }
    dar = DarDecoder.assertReadArchiveFromFile(darFile)
    darMap = dar.all.toMap
    pkgInterface = PackageInterface(darMap)
  }

  @Benchmark
  def bench(): Unit = {
    val config = Config.Default
      .copy(packageValidation = NoPackageValidation)
    val res = compilePackages(pkgInterface, darMap, config)
    assert(res.isRight)
  }
}
