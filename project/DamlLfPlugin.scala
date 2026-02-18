// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*
import sbt.nio.{Keys => _, *}
import sbt.util.HashFileInfo
import xsbti.compile.CompileAnalysis

import java.io.File
import scala.jdk.CollectionConverters.*

import _root_.io.circe._
import _root_.io.circe.syntax._
import _root_.io.circe.generic.auto._

object DamlLfPlugin extends AutoPlugin {

  object LfVersions {
    private val v2_1 = "2.1"
    private val v2_2 = "2.2"
    private val v2_3_1 = "2.3-rc1"
    // keep v2_3 pointed to latest revision
    private val v2_3 = v2_3_1
    private val v2_dev = "2.dev"

    val explicitVersions: Map[String, String] = Map(
      "v2_1" -> v2_1,
      "v2_2" -> v2_2,
      "v2_3_1" -> v2_3_1,
      "v2_3" -> v2_3,
      "v2_dev" -> v2_dev,
    )

    private val defaultLfVersion = v2_2
    private val devLfVersion = v2_dev
    private val latestStableLfVersion = v2_2

    val namedVersions: Map[String, String] = Map(
      "defaultLfVersion" -> defaultLfVersion,
      "devLfVersion" -> devLfVersion,
      "latestStableLfVersion" -> latestStableLfVersion,
    )

    private[DamlLfPlugin] val allLfVersions = List(v2_1, v2_2, v2_3, v2_dev)
    private val stableLfVersions = List(v2_1, v2_2)
    // DEPRECATED langauge lists
    private val compilerLfVersions = allLfVersions

    val versionLists = Map(
      "allLfVersions" -> allLfVersions,
      "stableLfVersions" -> stableLfVersions,
      "compilerInputLfVersions" -> compilerLfVersions,
      "compilerOutputLfVersions" -> compilerLfVersions,

      // DEPRECATED, use compiler(Input/Output)Versions instead
      "compilerLfVersions" -> compilerLfVersions,
    )

    case class LfVersionReport(
        explicitVersions: Seq[String],
        namedVersions: Map[String, String],
        versionLists: Map[String, List[String]],
    )
  }

  object autoImport {
    val lfSourceDirectory = settingKey[File]("Directory containing lf files")
    val lfVersions = settingKey[Seq[String]]("List of LF versions to generate DARs for")
    val lfDarOutput = settingKey[File]("Directory where DAR will be outputted")
    val generateLfVersionJson = taskKey[Seq[File]]("Generates the LF version JSON file")
  }

  import autoImport.*

  private val unscopedProjectSettings = Seq(
    lfSourceDirectory := sourceDirectory.value / "lf",
    lfVersions := LfVersions.allLfVersions,
    lfDarOutput := { target.value / "lf-dars" / configuration.value.name },
    resourceGenerators += generateDar.taskValue,
  )

  def generateJsonLogic = Def.task {
    // 1. Gather data
    val report = LfVersions.LfVersionReport(
      explicitVersions = LfVersions.explicitVersions.values.toList,
      namedVersions = LfVersions.namedVersions,
      versionLists = LfVersions.versionLists,
    )

    // 2. Generate JSON
    val jsonString = report.asJson.spaces2

    // 3. Write file (to target/scala-2.12/resource_managed/main/...)
    val outputFile = (Compile / resourceManaged).value / "daml-lf-versions.json"
    IO.write(outputFile, jsonString)

    streams.value.log.info(s"Generated LF version JSON at: $outputFile")

    Seq(outputFile)
  }

  override def projectSettings: Seq[Def.Setting[_]] =
    inConfig(Test)(unscopedProjectSettings)

  private def generateDar = Def.taskDyn {
    import BuildCommon.DamlProjects.`daml-lf-encoder`

    val lfVers = lfVersions.value
    val lfSourceDir = lfSourceDirectory.value
    val lfDarOut = lfDarOutput.value
    val streams = Keys.streams.value
    val encoderClasspath = (`daml-lf-encoder` / Compile / fullClasspath).value

    if (!lfDarOut.exists()) IO.createDirectory(lfDarOut)

    // compute hashes of the encoder classpath, to trigger regeneration if anything changes
    val encoderHashes = encoderClasspath.view
      .flatMap(_.metadata.get(Keys.analysis))
      .flatMap(_.readStamps.getAllProductStamps.asScala.values)
      .flatMap(_.getHash.asScala)
      .toSet

    val (outputDarFiles, argsOpts) = lfVers
      .map(prepareLfEncoderArgs(streams, _, lfSourceDir, lfDarOut, encoderHashes))
      .unzip

    import sbt.Scoped.richTaskSeq
    argsOpts.flatten
      .map { args =>
        /* NOTE: this prints the entire strArg to log, which causes /scripts/ci/check-logs.sh to report it as error
      since this will mention the string "Exception". Currently, we whitelist any string that contains "running
      com.digitalasset.daml.lf.archive.testing.DamlLfEncoder" (see /project/errors-in-log-to-ignore.txt) */
        (`daml-lf-encoder` / Compile / run).toTask(args)
      }
      .join
      .map(_ => outputDarFiles)
  }

  // returns None if the output DAR is already cached
  private def prepareLfEncoderArgs(
      streams: TaskStreams,
      lfVersion: String,
      sourceDir: File,
      darOutputDir: File,
      // hashes for the encoder classpath
      encoderHashes: Set[String],
  ): (File, Option[String]) = {
    import CacheImplicits._
    val sourceFiles = (sourceDir ** "*.lf").get
      .filter(f => f.getName.endsWith(s"_${lfVersion}_.lf") || f.getName.endsWith("_all_.lf"))
    val darOutputFile = darOutputDir / s"test-$lfVersion.dar"

    val cacheInput = (
      lfVersion,
      sourceFiles.map(FileInfo.hash(_)).toSet,
      darOutputFile.toString,
      encoderHashes,
    )
    val cacheStore = streams.cacheStoreFactory.make(s"lfGenerateDar-$lfVersion")
    implicit val inputFormat = tuple4Format(
      StringJsonFormat, // lfVersions
      immSetFormat(HashFileInfo.format), // sourceFiles
      StringJsonFormat, // darOutputFile
      immSetFormat(StringJsonFormat), // encoderHashes
    )
    val trackedArgs = Tracked.inputChanged[
      (String, Set[HashFileInfo], String, Set[String]),
      Option[String],
    ](cacheStore) {
      case (true, _) =>
        val tempDir = IO.createTemporaryDirectory
        val filesToCopy = sourceFiles.map { file =>
          val name = file.getName
          val newName =
            if (name.endsWith("_all_.lf")) name.replace("_all_.lf", s"_$lfVersion.lf") else name
          (file, tempDir / newName)
        }
        IO.copy(filesToCopy)

        val mangledVersion = lfVersion.replace(".", "_")
        val metadataContent = s"metadata ( 'testing-dar-$mangledVersion' : '0.0.0' )"
        val metadataFile = tempDir / s"metadata_$lfVersion.lf"
        IO.write(metadataFile, metadataContent)

        // Sort files alphabetically to ensure deterministic order
        // and prepend the metadata file (it must be first)
        val allSourceFiles = metadataFile +: filesToCopy.map(_._2).sortBy(_.getName)

        val filePaths = allSourceFiles.map(_.getAbsolutePath).mkString(" ")
        Some(
          s" $filePaths --suppress-testing-purpose-warning --output ${darOutputFile.getAbsolutePath} --target $lfVersion"
        )
      case (false, _) => None // no need to generate the dar file as it is already cached
    }

    (darOutputFile, trackedArgs(cacheInput))
  }
}
