import java.io.{File, FileReader, FileWriter, StringReader}
import java.util.Map as JMap
import com.esotericsoftware.yamlbeans.{YamlConfig, YamlReader, YamlWriter}
import sbt.Keys.*
import sbt.{io as _, *}
import sbt.nio.FileStamp
import sbt.util.HashFileInfo

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.sys.process.*
import scala.util.control.NonFatal

object DamlPlugin extends AutoPlugin {

  object autoImport {
    val damlCompilerVersion =
      settingKey[String]("The Daml version to use for DAR and code generation")
    val damlUseCustomVersion =
      settingKey[Boolean]("Use a custom daml sdk version built via daml-sdk-head in the daml repo")
    val dpmRegistry = settingKey[String]("dpm registry from which to install daml components")
    val damlInstall = taskKey[Unit]("Use dpm to install daml components")

    val damlSourceDirectory = settingKey[File]("Directory containing daml projects")
    val damlCompileDirectory =
      settingKey[File]("Directory to put the daml projects in for building")
    val damlBuildOrder =
      settingKey[Seq[String]](
        "List of directory names used to sort the Daml building by order in this list"
      )
    val damlDarOutput = settingKey[File]("Directory to put generated DAR files in")
    val damlDarLfVersions =
      settingKey[Seq[String]](
        "Lf versions for which to generate DAR files. If more than one is specified, output DARs are suffixed with their LF version."
      )
    val damlExtractMainDalf =
      settingKey[Boolean]("Extract main dalf from output DARs")
    val useVersionedDarName = settingKey[Boolean](
      "If enabled, the output DAR file name is <project-name>-<project-version>.dar otherwise it is <project-name>.dar"
    )
    val damlPinnedProjects = settingKey[Seq[File]](
      "List of relative paths to Daml projects (containing daml.yaml) that are pinned to resources"
    )
    // Java codegen settings and tasks
    val damlJavaCodegenOutput =
      settingKey[File]("Directory to put Java sources generated from DARs")
    val damlJavaCodegen = taskKey[Seq[(File, File, String)]](
      "Java codegen settings: Daml project directory, Daml archive file, name of the generated Java package"
    )
    val damlGenerateJava = taskKey[Seq[File]]("Generate Java code from Daml")

    // TypeScript codegen settings and tasks
    val damlTsCodegenOutput =
      settingKey[File]("Directory to put TypeScript sources generated from DARs")
    val damlTsCodegen = taskKey[Seq[(File, File)]](
      "Typescript codegen settings: Daml project directory, Daml archive file"
    )
    val damlGenerateTs = taskKey[Seq[File]]("Generate TypeScript code from daml")

    // From https://github.com/DACH-NY/the-real-canton-coin/pull/357:
    val damlDependencies = taskKey[Seq[File]]("Paths to DARs that this project depends on")
    val damlBuild = taskKey[Seq[File]]("Build a Daml Archive from Daml source")
    val damlPinProjects =
      taskKey[Seq[File]]("Update the checked in DAR with a DAR built with the current Daml version")
    val damlStudio = taskKey[Unit]("Open Daml studio for all projects in scope")

    lazy val baseDamlPluginSettings: Seq[Def.Setting[_]] = Seq(
      sourceGenerators += damlGenerateJava.taskValue,
      resourceGenerators += damlBuild.taskValue,
      damlSourceDirectory := sourceDirectory.value / "daml",
      damlCompileDirectory := target.value / "daml",
      damlDarOutput := resourceManaged.value,
      damlDarLfVersions := Seq("default"),
      damlExtractMainDalf := false,
      damlDependencies := Seq(),
      damlJavaCodegenOutput := codegenOutput(configuration.value, target.value, "java"),
      damlTsCodegenOutput := codegenOutput(configuration.value, target.value, "ts"),
      damlBuildOrder := Seq(),
      damlJavaCodegen := Seq(),
      damlTsCodegen := Seq(),
      useVersionedDarName := false,
      damlGenerateJava := damlGenerateJavaTask.value,
      damlGenerateTs := damlGenerateTsTask.value,
      managedSourceDirectories += damlJavaCodegenOutput.value,
      damlBuild := damlBuildTask.value,
      // Declare dependency so that Daml packages in test scope may depend on packages in compile scope.
      (Test / damlBuild) := (Test / damlBuild).dependsOn(Compile / damlBuild).value,
      damlPinProjects := damlPinProjectsTask.value,
    )
  }

  import autoImport._

  class BufferedLogger extends ProcessLogger {
    private val buffer = mutable.Buffer[String]()

    override def out(s: => String): Unit = buffer.append(s)
    override def err(s: => String): Unit = buffer.append(s)
    override def buffer[T](f: => T): T = f

    /** Output the buffered content to a String applying an optional line prefix.
      */
    def output(linePrefix: String = ""): String =
      buffer.map(l => s"$linePrefix$l").mkString(System.lineSeparator)
  }

  def damlStablePackagesManifest = Def.task {
    damlInstall.value
    val damlVersion = damlCompilerVersion.value
    val registry = dpmRegistry.value
    IO.withTemporaryDirectory { dir =>
      val values = Map[String, Any](
        "override-components" -> Map(
          "damlc" -> Map("version" -> damlVersion).asJava,
          "daml-script" -> Map("version" -> damlVersion).asJava,
          "codegen" -> Map("version" -> damlVersion).asJava,
        ).asJava
      ).asJava
      // write package configuration file
      writeYaml(dir / "daml.yaml", values)
      val rawResolution = runCommand(
        command = Seq("dpm", "resolve"),
        workingDir = dir,
        extraEnv = Seq("DPM_REGISTRY" -> registry),
      )(
        failureMessage = "dpm resolve failed"
      )
      val resolution = readYamlString(rawResolution)
      val damlcLocation = resolution
        .get("packages")
        .asInstanceOf[JMap[String, Any]]
        .get(dir.getCanonicalFile.toString)
        .asInstanceOf[JMap[String, Any]]
        .get("components")
        .asInstanceOf[JMap[String, Any]]
        .get("damlc")
        .asInstanceOf[String]
      val targetLocation =
        (Compile / resourceManaged).value / "compiler" / "damlc" / "stable-packages"
      if (!targetLocation.exists()) {
        IO.createDirectory(targetLocation)
        IO.createDirectory(targetLocation / "lf-v2" / "daml-prim")
        IO.createDirectory(targetLocation / "lf-v2" / "daml-stdlib")
      }

      // val stablePackagesManifestLocation = new File(damlcLocation + "/damlc-dist-dpm/resources/stable-packages-manifest-v2.txt")
      val damlStdlibLocation =
        new File(damlcLocation + "/damlc-dist-dpm/resources/stable-packages/lf-v2/daml-stdlib")
      val damlPrimLocation =
        new File(damlcLocation + "/damlc-dist-dpm/resources/stable-packages/lf-v2/daml-prim")
      // IO.copyFile(stablePackagesManifestLocation, targetLocation / "stable-packages-manifest-v2.txt")
      IO.copyDirectory(damlStdlibLocation, targetLocation / "lf-v2" / "daml-stdlib")
      IO.copyDirectory(damlPrimLocation, targetLocation / "lf-v2" / "daml-prim")

      val a1 = IO.listFiles(targetLocation / "lf-v2" / "daml-stdlib").toSeq
      val a2 = IO.listFiles(targetLocation / "lf-v2" / "daml-prim").toSeq
      // val a3 = Seq(targetLocation / "stable-packages-manifest-v2.txt")
      a1 ++ a2 // ++ a3
    }
  }

  def testingDarsFromS3(name: String, sha1sum: String) = Def.task {
    IO.createDirectory((Test / resourceManaged).value)
    val resource = (Test / resourceManaged).value / s"$name.dar"
    val log = streams.value.log
    if (!resource.exists() || sbt.io.Hash.toHex(sbt.io.Hash(resource)) != sha1sum) {
      IO.withTemporaryDirectory { dir =>
        runCommand(
          command = Seq(
            "aws",
            "s3",
            "cp",
            s"s3://canton-public-releases/test-artifacts/$name-$sha1sum.dar",
            resource.getAbsolutePath,
            "--no-sign-request",
          ),
          workingDir = dir,
          extraEnv = Seq(),
        )(
          failureMessage = s"downloading $name-$sha1sum.dar from s3 failed"
        )
      }
    }
    Seq(resource)
  }

  override lazy val buildSettings: Seq[Def.Setting[_]] = Seq(
    damlCompilerVersion := Dependencies.daml_compiler_version,
    damlUseCustomVersion := Dependencies.use_custom_daml_version,
    dpmRegistry := Dependencies.dpm_registry,
    damlInstall := installDaml.value,
    damlPinnedProjects := Seq(),
  )

  override lazy val projectSettings: Seq[Def.Setting[_]] =
    inConfig(Compile)(baseDamlPluginSettings) ++
      inConfig(Test)(baseDamlPluginSettings)

  // in-memory cache for damlInstall
  // we don't use a file cache, because sbt file cache and dpm cache can be out-of-sync on CI
  private var installedDamlVersion: Option[String] = None

  private def installDaml = Def.task {
    val damlVersion = damlCompilerVersion.value
    val registry = dpmRegistry.value
    val useCustomVersion = damlUseCustomVersion.value
    val streams = Keys.streams.value

    if (!useCustomVersion && !installedDamlVersion.contains(damlVersion)) {
      streams.log.info(s"Installing daml $damlVersion")

      // create a temporary daml.yaml file with damlc and daml-script overrides
      // invoke `dpm install package` on it
      IO.withTemporaryDirectory { dir =>
        val values = Map[String, Any](
          "override-components" -> Map(
            "damlc" -> Map("version" -> damlVersion).asJava,
            "daml-script" -> Map("version" -> damlVersion).asJava,
            "codegen" -> Map("version" -> damlVersion).asJava,
          ).asJava
        ).asJava
        // write package configuration file
        writeYaml(dir / "daml.yaml", values)
        runCommand(
          command = Seq("dpm", "install", "package"),
          workingDir = dir,
          extraEnv = Seq("DPM_REGISTRY" -> registry),
        )(
          failureMessage = "dpm install failed"
        )
      }
      installedDamlVersion = Some(damlVersion)
    }
  }

  /** Verify that the versions in the daml.yaml file match what is being used in the sbt project. If
    * a mismatch is found a [[sbt.internal.MessageOnlyException]] will be thrown.
    */
  private def checkProjectVersions(
      damlProjectFile: File
  ): Unit = {
    require(
      damlProjectFile.exists,
      s"supplied daml.yaml must exist [${damlProjectFile.absolutePath}]",
    )
    val values = readYaml(damlProjectFile)

    def ensureMatchingVersion(sbtVersion: String, fieldName: String): Unit = {
      val damlVersion = values.get(fieldName).toString
      // With Daml 0.13.56 characters are no longer allowed in project versions as
      // GHC does not like non-numbers in versions.
      val sbtNonSnapshotVersion = sbtVersion
        .stripSuffix("-SNAPSHOT")
        // Take into account the 3.0.0-SNAPSHOT.100000000 naming scheme
        .replaceAll("-SNAPSHOT.([0-9]+)$", "")

      if (sbtNonSnapshotVersion != damlVersion) {
        throw new MessageOnlyException(
          s"daml.yaml $fieldName value [$damlVersion] does not match the '-SNAPSHOT'-stripped value in our sbt project [$sbtVersion] in file [$damlProjectFile]"
        )
      }
    }
  }

  private def damlGenerateJavaTask = Def.task {
    // for the time being we assume if we're using code generation then the DARs must first be built
    damlBuild.value

    val streams = Keys.streams.value
    val settings = damlJavaCodegen.value
    val outputDirectory = damlJavaCodegenOutput.value
    val damlVersion = damlCompilerVersion.value

    import CacheImplicits._
    val cacheInput = (
      settings.map { case (dir, dar, p) => (dir.toString, FileInfo.hash(dar), p) }.toSet,
      outputDirectory.toString,
      damlVersion,
    )
    val cacheStore = streams.cacheStoreFactory.make("damlGenerateJava")
    val cacheFormat = basicCache(
      tuple3Format(
        immSetFormat(
          tuple3Format(
            StringJsonFormat,
            HashFileInfo.format,
            StringJsonFormat,
          )
        ), // damlJavaCodegen
        StringJsonFormat, // damlJavaCodegenOutput
        StringJsonFormat, // damlCompilerVersion
      ),
      FileStamp.Formats.seqFileJsonFormatter,
    )
    val cachedOutput =
      Cache.cached(cacheStore) { (_: (Set[(String, HashFileInfo, String)], String, String)) =>
        IO.delete(outputDirectory)
        settings.foreach { case (damlPackageDirectory, darFile, packageName) =>
          generateJavaCode(
            streams.log,
            damlPackageDirectory,
            darFile,
            if (packageName.contains("java")) packageName else s"$packageName.java",
            outputDirectory,
            damlVersion,
          )
        }
        // return all generated Java files
        (outputDirectory ** "*.java").get
      }(cacheFormat)
    cachedOutput(cacheInput)
  }

  private def damlGenerateTsTask = Def.task {
    damlBuild.value

    val streams = Keys.streams.value
    val settings = damlTsCodegen.value
    val outputDirectory = damlTsCodegenOutput.value
    val damlVersion = damlCompilerVersion.value

    import CacheImplicits._
    val cacheInput = (
      settings.map { case (dir, dar) => (dir.toString, FileInfo.hash(dar)) }.toSet,
      outputDirectory.toString,
      damlVersion,
    )
    val cacheStore = streams.cacheStoreFactory.make("damlGenerateTs")
    val cacheFormat = basicCache(
      tuple3Format(
        immSetFormat(
          tuple2Format(
            StringJsonFormat,
            HashFileInfo.format,
          )
        ), // damlTsCodegen
        StringJsonFormat, // damlTsCodegenOutput
        StringJsonFormat, // damlCompilerVersion
      ),
      FileStamp.Formats.seqFileJsonFormatter,
    )
    val cachedOutput =
      Cache.cached(cacheStore) { (_: (Set[(String, HashFileInfo)], String, String)) =>
        IO.delete(outputDirectory)
        settings.foreach { case (damlPackageDirectory, darFile) =>
          generateTsCode(streams.log, damlPackageDirectory, darFile, outputDirectory, damlVersion)
        }
        // return all generated files
        (outputDirectory ** "*").get
      }(cacheFormat)
    cachedOutput(cacheInput)
  }

  private def damlBuildTask = Def.task {
    damlInstall.value
    val streams = Keys.streams.value
    val dependencies = damlDependencies.value
    val outputDirectory = damlDarOutput.value
    val outputLfVersions = damlDarLfVersions.value.toSet
    val buildDirectory = damlCompileDirectory.value
    val sourceDirectory = damlSourceDirectory.value
    val useVersionedDarFileName = useVersionedDarName.value
    val damlVersion = damlCompilerVersion.value
    val buildDependencies = damlBuildOrder.value
    val shouldExtractMainDalf = damlExtractMainDalf.value
    val relativePinnedProjectFiles = damlPinnedProjects.value
      .map(p => sourceDirectory.toPath.resolve("daml.yaml").relativize(p.toPath).normalize().toFile)
      .toSet

    if (outputLfVersions.isEmpty) {
      throw new MessageOnlyException(
        s"DamlPlugin: Cannot have 0 damlDarLfVersions, must specify at least one."
      )
    }

    def buildOrder(fst: File, snd: File): Boolean = {
      def indexOf(file: File): Int = {
        val asString = file.toString
        buildDependencies.indexWhere(asString.contains(_))
      }
      val fstIdx = indexOf(fst)
      val sndIdx = indexOf(snd)
      if (fstIdx == -1 && sndIdx == -1) {
        fst.toString < snd.toString
      } else if (fstIdx == -1) {
        false
      } else if (sndIdx == -1) {
        true
      } else {
        fstIdx < sndIdx
      }
    }

    val allDamlFiles = (sourceDirectory ** "*.daml").get
    val damlProjectFiles = (sourceDirectory ** "daml.yaml").get
    // we don't really know dependencies between daml files, so just assume if any change then we need to rebuild all packages
    val filesHash =
      (allDamlFiles.toSet ++ damlProjectFiles ++ dependencies).map(FileInfo.hash(_))

    import CacheImplicits._
    val cacheInput = (
      filesHash,
      outputDirectory.toString,
      outputLfVersions,
      useVersionedDarFileName,
      damlVersion,
      shouldExtractMainDalf,
      relativePinnedProjectFiles,
    )
    val cacheStore = streams.cacheStoreFactory.make("damlBuild")
    // implicit resolution fails
    val cacheFormat = basicCache(
      tuple7Format(
        immSetFormat[HashFileInfo], // source files and dependencies
        StringJsonFormat, // outputDirectory
        immSetFormat(StringJsonFormat), // output lf version
        BooleanJsonFormat, // useVersionedDarFileName
        StringJsonFormat, // daml compiler version
        BooleanJsonFormat, // shouldExtractMainDalf
        immSetFormat[File], // list of pinned projects
      ),
      FileStamp.Formats.seqFileJsonFormatter,
    )

    val cachedOutputDars =
      Cache.cached(cacheStore) {
        (_: (Set[HashFileInfo], String, Set[String], Boolean, String, Boolean, Set[File])) =>
          // build the daml files in a sorted way, using the build order definition
          for {
            projectFile <- damlProjectFiles.sortWith(buildOrder)
            relativeProjectFile =
              sourceDirectory.toPath.relativize(projectFile.toPath).normalize().toFile
            if !relativePinnedProjectFiles.contains(relativeProjectFile)
            outputLfVersion <- outputLfVersions.toSeq
            file <-
              buildDamlProject(
                streams.log,
                sourceDirectory,
                buildDirectory,
                outputDirectory,
                outputLfVersion,
                outputLfVersions.size > 1,
                useVersionedDarFileName,
                relativeProjectFile,
                damlVersion,
                shouldExtractMainDalf,
              )
          } yield file
      }(cacheFormat)
    cachedOutputDars(cacheInput)
  }

  /** We intentionally take the unusual step of checking in certain DARs to ensure stable package
    * ids across different compilers. This task builts the DAR and updates the checked in version.
    */
  private def damlPinProjectsTask = Def.task {
    damlInstall.value
    val streams = Keys.streams.value
    val buildDirectory = damlCompileDirectory.value
    val outputDirectory = damlDarOutput.value
    val outputLfVersions = damlDarLfVersions.value.toSet
    val useVersionedDarFileName = useVersionedDarName.value
    val damlVersion = damlCompilerVersion.value
    val pinnedProjects = damlPinnedProjects.value
    val destinationDirectory = resourceDirectory.value / "dar"

    if (pinnedProjects.nonEmpty) {
      if (!destinationDirectory.exists()) IO.createDirectory(destinationDirectory)
      for {
        projectPath <- pinnedProjects
        outputLfVersion <- outputLfVersions.toSeq
        file <- buildDamlProject(
          streams.log,
          projectPath,
          buildDirectory,
          destinationDirectory,
          outputLfVersion,
          outputLfVersions.size > 1,
          useVersionedDarFileName,
          new File("daml.yaml"),
          damlVersion,
          false,
        )
      } yield file
    } else {
      streams.log.info("No damlPinnedProjects configured to update.")
      Seq.empty
    }
  }

  private def buildDamlProject(
      log: Logger,
      sourceDirectory: File,
      buildDirectory: File,
      outputDirectory: File,
      outputLfVersion: String,
      useLfVersionAsSuffix: Boolean,
      useVersionedDarName: Boolean,
      relativeDamlProjectFile: File,
      damlVersion: String,
      shouldExtractMainDalf: Boolean,
  ): Seq[File] = {

    val originalDamlProjectFile =
      sourceDirectory.toPath.resolve(relativeDamlProjectFile.toPath).toFile
    require(
      originalDamlProjectFile.exists,
      s"supplied daml.yaml must exist [${originalDamlProjectFile.absolutePath}]",
    )

    val projectBuildDirectory =
      buildDirectory.toPath.resolve(relativeDamlProjectFile.toPath).toAbsolutePath.getParent.toFile

    log.debug(
      s"building ${originalDamlProjectFile.getAbsoluteFile.getParentFile} in $projectBuildDirectory"
    )

    // copy project directory into target tree
    // the reason for this is that `daml build` caches files in a `.daml` directory of the source tree
    // making sbt to believe that the source code changed
    IO.delete(projectBuildDirectory) // to not let deleted files stick around in build directory
    IO.copyDirectory(originalDamlProjectFile.getAbsoluteFile.getParentFile, projectBuildDirectory)

    val damlYamlMap = readYaml(originalDamlProjectFile)
    val damlProjectName = damlYamlMap.get("name").toString
    val damlProjectVersion = damlYamlMap.get("version").toString
    val pluginNameSuffix =
      if (damlYamlMap.containsKey("canton-daml-plugin-name-suffix"))
        s"-${damlYamlMap.get("canton-daml-plugin-name-suffix").toString}"
      else
        ""

    val lfVersionSuffix =
      if (useLfVersionAsSuffix)
        s"-v${outputLfVersion.replace(".", "")}"
      else
        ""

    val versionSuffix =
      if (useVersionedDarName)
        s"-${damlYamlMap.get("version").toString}"
      else
        ""
    val outputDar =
      outputDirectory / s"$damlProjectName$pluginNameSuffix$versionSuffix$lfVersionSuffix.dar"
    val outputDalf =
      outputDirectory / s"$damlProjectName$pluginNameSuffix$versionSuffix$lfVersionSuffix.dalf"

    val processLogger = new BufferedLogger

    val buildOptions = Seq(
      "--ghc-option",
      "-Werror",
      "-Wupgrade-interfaces",
      "-Wupgrade-exceptions",
    )
    val outputOpts = Seq("--output", outputDar.getAbsolutePath)
    val targetOpts =
      if (outputLfVersion == "default") Seq.empty else Seq("--target", outputLfVersion)

    runCommand(
      Seq("dpm", "build") ++ buildOptions ++ outputOpts ++ targetOpts,
      projectBuildDirectory,
      extraEnv = Seq("DAML_VERSION" -> damlVersion),
    )(failureMessage = s"dpm build failed [$originalDamlProjectFile]")

    if (shouldExtractMainDalf) {
      IO.withTemporaryDirectory { dir =>
        runCommand(
          Seq(
            "unzip",
            outputDar.getAbsolutePath,
            s"$damlProjectName-$damlProjectVersion-*/$damlProjectName-$damlProjectVersion-*.dalf",
          ),
          workingDir = dir,
          extraEnv = Seq(),
        )(failureMessage = s"unzip failed")
        val extractedFiles = sbt.io
          .PathFinder(dir)
          .glob(s"$damlProjectName-$damlProjectVersion-*")
          .glob(s"$damlProjectName-$damlProjectVersion-*.dalf")
          .get
        IO.copyFile(extractedFiles.head, outputDalf)
      }

      Seq(outputDar, outputDalf)
    } else {
      Seq(outputDar)
    }
  }

  private lazy val yamlConfig = new YamlConfig()
  yamlConfig.writeConfig.setWriteClassname(YamlConfig.WriteClassName.NEVER)
  yamlConfig.writeConfig.setIndentSize(2)

  private def readYamlString(raw: String): JMap[String, Any] = {
    val reader = new YamlReader(new StringReader(raw), yamlConfig)
    try reader.read(classOf[JMap[String, Any]])
    finally reader.close()
  }

  private def readYaml(file: File): JMap[String, Any] = {
    val reader = new YamlReader(new FileReader(file), yamlConfig)
    try reader.read(classOf[JMap[String, Any]])
    finally reader.close()
  }

  private def writeYaml(file: File, values: JMap[String, Any]): Unit = {
    val writer = new YamlWriter(new FileWriter(file), yamlConfig)
    try writer.write(values)
    finally writer.close()
  }

  private def codegenOutput(config: Configuration, target: File, codegen: String): File =
    if (config.name == "compile") target / s"daml-codegen-$codegen"
    else target / s"$config-daml-codegen-$codegen"

  private def generateJavaCode(
      log: Logger,
      damlPackageDirectory: File,
      darFile: File,
      packageName: String,
      outputDir: File,
      damlVersion: String,
  ): Unit = {
    require(
      damlPackageDirectory.exists,
      s"supplied daml package directory must exist [${damlPackageDirectory.absolutePath}]",
    )

    if (!darFile.exists())
      throw new MessageOnlyException(
        s"Codegen asked to generate code from nonexistent file: $darFile"
      )

    log.debug(s"Running codegen-java for $darFile into $outputDir")

    val outputOpts = Seq("--output-directory", outputDir.getAbsolutePath)

    // run the dpm process using the working directory of the daml.yaml file
    runCommand(
      Seq("dpm", "codegen-java") ++ outputOpts ++ Seq(s"${darFile.getAbsolutePath}=$packageName"),
      damlPackageDirectory,
      extraEnv = Seq("DAML_VERSION" -> damlVersion),
    )(failureMessage = s"dpm codegen-java failed [${darFile.getName}]")
  }

  /** Calls the dpm Codegen for the provided DAR file (hence, is suitable to use in a
    * sourceGenerator task)
    */
  private def generateTsCode(
      log: Logger,
      damlPackageDirectory: File,
      darFile: File,
      outputDir: File,
      damlVersion: String,
  ): Unit = {
    require(
      damlPackageDirectory.exists,
      s"supplied daml package directory must exist [${damlPackageDirectory.absolutePath}]",
    )

    if (!darFile.exists())
      throw new MessageOnlyException(
        s"Codegen asked to generate code from nonexistent file: $darFile"
      )

    log.debug(s"Running codegen-js for $darFile into $outputDir")

    val outputOpts = Seq("--output-directory", outputDir.getAbsolutePath)

    // run the dpm process using the working directory of the daml.yaml file
    runCommand(
      Seq("dpm", "codegen-js") ++ outputOpts ++ Seq(darFile.getAbsolutePath),
      damlPackageDirectory,
      extraEnv = Seq("DAML_VERSION" -> damlVersion),
    )(failureMessage = s"dpm codegen-js failed [${darFile.getName}]")
  }

  private def runCommand(command: Seq[String], workingDir: File, extraEnv: Seq[(String, String)])(
      failureMessage: String
  ): String = {
    val logger = new BufferedLogger()
    val commandName = command.head
    try Process(command, cwd = Some(workingDir), extraEnv: _*) !! logger
    catch {
      case NonFatal(cause) =>
        val logs = logger.output(s"$commandName: ")
        throw new MessageOnlyException(s"$failureMessage: ${cause.getMessage}\n" + logs)
    }
  }
}
