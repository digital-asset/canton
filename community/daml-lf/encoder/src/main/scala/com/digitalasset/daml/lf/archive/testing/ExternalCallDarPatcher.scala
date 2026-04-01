// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive.testing

import com.digitalasset.daml.lf.archive.{ArchiveDecoder, ArchiveReader, Dar, DarDecoder, DarWriter}
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.language.Ast

import java.io.File
import java.util.jar.Manifest
import java.util.zip.ZipFile
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.Using

object ExternalCallDarPatcher extends App {

  import Encode.encodeArchive

  private val ManifestPath = "META-INF/MANIFEST.MF"
  private val ExternalModuleName = Ref.ModuleName.assertFromString("DA.External")
  private val ExternalCallName = Ref.DottedName.assertFromString("externalCall")

  final case class Arguments(input: File, dependenciesDar: File, output: File)
  private final case class DependencyEntry(
      entryName: String,
      bytes: Bytes,
      directDeps: Set[Ref.PackageId],
  )

  private val arguments = parseArgs(args.toList)

  patchDar(arguments.input, arguments.output)

  private def patchDar(inputDar: File, outputDar: File): Unit = {
    val dar = DarDecoder.assertReadArchiveFromFile(inputDar)
    val patchedMain = patchMainArchive(dar.main)
    val sdkVersion = readSdkVersion(inputDar)
    val dependencyEntries = readDependencyEntries(arguments.dependenciesDar)

    val encodedMain = encodeArchive(patchedMain, patchedMain._2.languageVersion)
    val mainPackageId = Ref.PackageId.assertFromString(encodedMain.getHash)
    val mainEntryName =
      s"${patchedMain._2.metadata.nameDashVersion}-$mainPackageId/${patchedMain._2.metadata.nameDashVersion}-$mainPackageId.dalf"

    val encodedDependencies =
      resolveDependencyClosure(patchedMain._2.directDeps, dependencyEntries).map { dependencyPackageId =>
        val dependency = dependencyEntries.getOrElse(
          dependencyPackageId,
          sys.error(
            s"Cannot find dependency $dependencyPackageId in ${arguments.dependenciesDar.getAbsolutePath}"
          ),
        )
        dependency.entryName -> dependency.bytes
      }

    DarWriter.encode(
      sdkVersion,
      Dar(
        main = mainEntryName -> Bytes.fromByteString(encodedMain.toByteString),
        dependencies = encodedDependencies,
      ),
      outputDar.toPath,
    )
  }

  private def readDependencyEntries(
      dependencyDar: File
  ): Map[Ref.PackageId, DependencyEntry] =
    Using.resource(new ZipFile(dependencyDar)) { zipFile =>
      zipFile.entries().asScala
        .filter(entry => !entry.isDirectory && entry.getName.endsWith(".dalf"))
        .map { entry =>
          val bytes = Using.resource(zipFile.getInputStream(entry))(Bytes.fromInputStream)
          val packageId = ArchiveReader.assertFromBytes(bytes).pkgId
          val (_, pkg) = ArchiveDecoder.assertFromBytes(bytes)
          val fileName = new File(entry.getName).getName
          packageId -> DependencyEntry(
            entryName = s"dependencies/$fileName",
            bytes = bytes,
            directDeps = pkg.directDeps,
          )
        }
        .toMap
    }

  private def resolveDependencyClosure(
      initialDependencies: Set[Ref.PackageId],
      availableDependencies: Map[Ref.PackageId, DependencyEntry],
  ): List[Ref.PackageId] = {
    @tailrec
    def go(
        pending: List[Ref.PackageId],
        resolved: Set[Ref.PackageId],
    ): Set[Ref.PackageId] = pending match {
      case Nil => resolved
      case dependency :: rest if resolved.contains(dependency) =>
        go(rest, resolved)
      case dependency :: rest =>
        val entry = availableDependencies.getOrElse(
          dependency,
          sys.error(
            s"Cannot find dependency $dependency in ${arguments.dependenciesDar.getAbsolutePath}"
          ),
        )
        go(entry.directDeps.toList ::: rest, resolved + dependency)
    }

    go(initialDependencies.toList, Set.empty).toList.sortBy(_.toString)
  }

  private def patchMainArchive(
      mainArchive: (Ref.PackageId, Ast.Package)
  ): (Ref.PackageId, Ast.Package) = {
    val (packageId, pkg) = mainArchive
    val module = pkg.modules.getOrElse(
      ExternalModuleName,
      sys.error(s"Cannot find module $ExternalModuleName in package ${pkg.metadata.nameDashVersion}")
    )
    val definition = module.definitions.getOrElse(
      ExternalCallName,
      sys.error(s"Cannot find value $ExternalCallName in module $ExternalModuleName")
    )
    val patchedDefinition = definition match {
      case Ast.DValue(typ, _) => Ast.DValue(typ, Ast.EBuiltinFun(Ast.BExternalCall))
      case other =>
        sys.error(
          s"Expected $ExternalModuleName:$ExternalCallName to be a value definition, got ${other.getClass.getSimpleName}"
        )
    }
    val patchedModule = module.copy(
      definitions = module.definitions.updated(ExternalCallName, patchedDefinition)
    )
    val patchedPackage = pkg.copy(modules = pkg.modules.updated(ExternalModuleName, patchedModule))
    packageId -> patchedPackage
  }

  private def readSdkVersion(inputDar: File): String =
    Using.resource(new ZipFile(inputDar)) { zipFile =>
      val manifestEntry = Option(zipFile.getEntry(ManifestPath)).getOrElse(
        sys.error(s"Cannot find $ManifestPath in DAR")
      )
      Using.resource(zipFile.getInputStream(manifestEntry)) { stream =>
        val manifest = new Manifest(stream)
        Option(manifest.getMainAttributes.getValue("Sdk-Version")).getOrElse(
          sys.error("Cannot find Sdk-Version in DAR manifest")
        )
      }
    }

  private def parseArgs(rawArgs: List[String]): Arguments = rawArgs match {
    case "--input" :: input :: "--dependencies-dar" :: dependenciesDar :: "--output" :: output :: Nil =>
      Arguments(new File(input), new File(dependenciesDar), new File(output))
    case _ =>
      sys.error(
        "usage: ExternalCallDarPatcher --input <input.dar> --dependencies-dar <dependencies.dar> --output <output.dar>"
      )
  }
}
