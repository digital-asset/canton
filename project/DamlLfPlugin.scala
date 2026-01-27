// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.{Def, *}
import sbt.Keys.*

import java.io.File

object DamlLfPlugin extends AutoPlugin {

  object autoImport {
    val lfSource = settingKey[File]("Directory containing lf files")
    val lfDarOut = settingKey[File]("Directory where DAR will be outputted")

    val lfVersions = settingKey[Seq[String]]("List of LF versions to generate DARs for")

    val lfGenerateDar = taskKey[Seq[File]]("Generate all DAR files")
  }

  import autoImport.*

  def prepareLfFiles(
      version: String,
      sourceDir: File,
      workDir: File,
      darOutputDir: File,
  ): (File, String) = {
    // Create the version-specific working directory
    val versionWorkDir = workDir / version
    IO.createDirectory(versionWorkDir)

    // Identify files to copy
    val allFiles = (sourceDir ** "*.lf").get

    val filesToCopy = allFiles.flatMap { file =>
      val name = file.getName

      if (name.endsWith(s"_${version}_.lf")) {
        // Case 1: Hardcoded version -> Copy as is (preserving existing name)
        Some(file -> (versionWorkDir / name))
      } else if (name.endsWith("_all_.lf")) {
        // Case 2: "All" version -> Rename to test_2.1.lf
        val newName = name.replace("_all_.lf", s"_$version.lf")
        Some(file -> (versionWorkDir / newName))
      } else {
        None
      }
    }

    IO.copy(filesToCopy, overwrite = true, preserveLastModified = true, preserveExecutable = true)

    val mangledVersion = version.replace(".", "_")
    val metadataContent = s"metadata ( 'testing-dar-$mangledVersion' : '0.0.0' )"
    val generatedMetadataName = s"metadata_$version.lf"
    val metadataFile = versionWorkDir / generatedMetadataName
    IO.write(metadataFile, metadataContent)

    // Sort files alphabetically to ensure deterministic order
    val sortedDestinations = filesToCopy.map(_._2).sortBy(_.getName)

    // Prepend the metadata file (it must be first)
    val allDestinations = metadataFile +: sortedDestinations

    val filePaths = allDestinations.map(_.getAbsolutePath).mkString(" ")
    val darOutputFile = darOutputDir / s"test-$version.dar"

    (darOutputFile, s" $filePaths --output ${darOutputFile.getAbsolutePath} --target $version")
  }

  private val unscopedProjectSettings = Seq(
    lfSource := sourceDirectory.value / "lf",
    lfDarOut := {
      target.value / "lf-dars" / configuration.value.name
    },
    lfVersions := Seq("2.1", "2.2", "2.dev"),

    // TODO(#30253): cache this task
    lfGenerateDar := Def.taskDyn {
      val lfVersionsVals = lfVersions.value
      val lfSourceVal = lfSource.value
      val lfTempDirVal = IO.createTemporaryDirectory
      val lfDarOutDirVal = lfDarOut.value
      val log = streams.value.log

      if (!lfDarOutDirVal.exists()) {
        log.info(s"Creating output directory: ${lfDarOutDirVal.getAbsolutePath}")
        IO.createDirectory(lfDarOutDirVal)
      }

      import sbt.Scoped.richTaskSeq
      lfVersionsVals.map { lfVersion =>
        val (darLoc, strArg) = prepareLfFiles(lfVersion, lfSourceVal, lfTempDirVal, lfDarOutDirVal)
        /* NOTE: this prints the entire strArg to log, which causes /scripts/ci/check-logs.sh to report it as error
        since this will mention the string "Exception". Currently, we whitelist any string that contains "running
        com.digitalasset.daml.lf.archive.testing.DamlLfEncoder" (see /project/errors-in-log-to-ignore.txt) */
        (BuildCommon.DamlProjects.`daml-lf-encoder` / Compile / run)
          .toTask(strArg)
          .map(_ => darLoc)
      }.join
    }.value,
    resourceGenerators += lfGenerateDar.taskValue,
  )

  override def projectSettings: Seq[Def.Setting[_]] =
    inConfig(Test)(unscopedProjectSettings)

}
