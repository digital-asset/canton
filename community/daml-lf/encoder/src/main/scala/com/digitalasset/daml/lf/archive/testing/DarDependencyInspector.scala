// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive.testing

import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2

import java.io.File

object DarDependencyInspector extends App {

  private case class Arguments(input: File)

  private val arguments = parseArgs(args.toList)
  inspect(arguments.input)

  private def inspect(inputDar: File): Unit = {
    val dar = DarDecoder.assertReadArchiveFromFile(inputDar)
    val includedPackages = dar.all.map(_._1).toSet
    val main = dar.main
    val missing = main._2.directDeps -- includedPackages
    val extras = dar.dependencies.map(_._1).toSet -- main._2.directDeps
    val stableById = StablePackagesV2.values.map(pkg => pkg.packageId -> pkg).toMap

    println(s"input=${inputDar.getAbsolutePath}")
    println(s"mainPackageId=${main._1}")
    println(s"mainName=${main._2.metadata.nameDashVersion}")
    println(s"includedPackages=${includedPackages.size}")
    println("directDeps:")
    main._2.directDeps.toList.sorted.foreach { dep =>
      val label = stableById.get(dep).fold("<not-in-stable-packages>")(_.moduleName.dottedName)
      println(s"  $dep  $label")
    }
    println("missingDeps:")
    missing.toList.sorted.foreach { dep =>
      val label = stableById.get(dep).fold("<not-in-stable-packages>")(_.moduleName.dottedName)
      println(s"  $dep  $label")
    }
    println("extraDeps:")
    extras.toList.sorted.foreach { dep =>
      val label = stableById.get(dep).fold("<not-in-stable-packages>")(_.moduleName.dottedName)
      println(s"  $dep  $label")
    }
  }

  private def parseArgs(rawArgs: List[String]): Arguments = rawArgs match {
    case "--input" :: input :: Nil =>
      Arguments(new File(input))
    case _ =>
      sys.error("usage: DarDependencyInspector --input <input.dar>")
  }
}
