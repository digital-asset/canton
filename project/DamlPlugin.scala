import java.io.{File, FileReader, FileWriter, IOException}
import java.util.{Map => JMap}

import com.esotericsoftware.yamlbeans.{YamlReader, YamlWriter}
import sbt.Keys._
import sbt.util.CacheStoreFactory
import sbt.util.FileFunction.UpdateFunction
import sbt.{Def, _}

import scala.collection.mutable
import scala.sys.process._
import scala.util.{Failure, Success, Try}

object DamlPlugin extends AutoPlugin {

  sealed trait Codegen
  object Codegen {
    object Java extends Codegen
    object Scala extends Codegen
  }

  object autoImport {
    val damlCodeGeneration =
      taskKey[Seq[(File, File, String)]](
        "List of tuples (Daml project directory, Daml archive file, name of the generated Java package)"
      )
    val damlSourceDirectory = settingKey[File]("Directory containing daml projects")
    val damlCompileDirectory =
      settingKey[File]("Directory to put the daml projects in for building")
    val damlBuildOrder =
      settingKey[Seq[String]](
        "List of directory names used to sort the Daml building by order in this list"
      )
    val damlDarOutput = settingKey[File]("Directory to put generated DAR files in")
    val useVersionedDarName = settingKey[Boolean](
      "If enabled, the output DAR file name is <project-name>-<project-version>.dar otherwise it is <project-name>.dar"
    )
    val damlScalaCodegenOutput =
      settingKey[File]("Directory to put Scala sources generated from DARs")
    val damlJavaCodegenOutput =
      settingKey[File]("Directory to put Java sources generated from DARs")
    val damlCompilerVersion =
      settingKey[String]("The Daml version to use for DAR and code generation")
    val damlLanguageVersions =
      settingKey[Seq[String]]("The Daml-lf language versions supported by canton")
    val damlFixedDars = settingKey[Seq[String]](
      "Which DARs do we check in to avoid problems with package id versioning across daml updates"
    )
    val damlProjectVersionOverride =
      settingKey[Option[String]]("Allows hardcoding daml project version")
    val damlEnableProjectVersionOverride =
      settingKey[Boolean]("Enables overriding the Daml project-version key")

    val damlGenerateCode = taskKey[Seq[File]]("Generate scala code from Daml")
    // From https://github.com/DACH-NY/the-real-canton-coin/pull/357:
    val damlDependencies = taskKey[Seq[File]]("Paths to DARs that this project depends on")
    val damlBuild = taskKey[Seq[File]]("Build a Daml Archive from Daml source")
    val damlStudio = taskKey[Unit]("Open Daml studio for all projects in scope")
    val damlCheckProjectVersions =
      taskKey[Unit]("Ensure that the versions specified in our SBT project match Daml projects")
    val damlUpdateProjectVersions =
      taskKey[Unit](
        "Update the versions used by our Daml projects to match the current values of the SBT project"
      )
    val damlUpdateFixedDars =
      taskKey[Unit]("Update the checked in DAR with a DAR built with the current Daml version")
    val damlEnableJavaCodegen =
      settingKey[Boolean]("Enable Java codegen")
    val damlEnableScalaCodegen =
      settingKey[Boolean]("Enable Scala codegen")

    lazy val baseDamlPluginSettings: Seq[Def.Setting[_]] = Seq(
      sourceGenerators += damlGenerateCode.taskValue,
      resourceGenerators += damlBuild.taskValue,
      damlSourceDirectory := sourceDirectory.value / "daml",
      damlCompileDirectory := target.value / "daml",
      damlDarOutput := resourceManaged.value,
      damlDependencies := Seq(),
      damlScalaCodegenOutput := sourceManaged.value / "daml-codegen-scala",
      damlJavaCodegenOutput := sourceManaged.value / "daml-codegen-java",
      damlBuildOrder := Seq(),
      damlCodeGeneration := Seq(),
      damlEnableJavaCodegen := true,
      damlEnableScalaCodegen := false,
      useVersionedDarName := false,
      damlGenerateCode := {
        // for the time being we assume if we're using code generation then the DARs must first be built
        damlBuild.value

        val settings = damlCodeGeneration.value
        val scalaOutputDirectory = damlScalaCodegenOutput.value
        val javaOutputDirectory = damlJavaCodegenOutput.value
        val cacheDirectory = streams.value.cacheDirectory
        val log = streams.value.log
        val enableJavaCodegen = damlEnableJavaCodegen.value
        val enableScalaCodegen = damlEnableScalaCodegen.value

        val cache = FileFunction.cached(cacheDirectory, FileInfo.hash) { input =>
          val codegens =
            (if (enableScalaCodegen) Seq((Codegen.Scala, scalaOutputDirectory)) else Seq.empty) ++
              (if (enableJavaCodegen) Seq((Codegen.Java, javaOutputDirectory)) else Seq.empty)
          codegens.foreach { case (_, outputDirectory) => IO.delete(outputDirectory) }
          settings.flatMap { case (damlProjectDirectory, darFile, packageName) =>
            codegens
              .flatMap { case (codegen, outputDirectory) =>
                generateCode(
                  log,
                  damlProjectDirectory,
                  darFile,
                  packageName,
                  codegen,
                  outputDirectory,
                  damlCompilerVersion.value,
                )
              }
          }.toSet
        }
        cache(settings.map(_._2).toSet).toSeq
      },
      managedSourceDirectories += damlJavaCodegenOutput.value,
      damlBuild := {
        val dependencies = damlDependencies.value
        val outputDirectory = damlDarOutput.value
        val buildDirectory = damlCompileDirectory.value
        val sourceDirectory = damlSourceDirectory.value
        // we don't really know dependencies between daml files, so just assume if any change then we need to rebuild all packages
        val cacheDir = streams.value.cacheDirectory
        val allDamlFiles = damlSourceDirectory.value ** "*.daml"
        val damlProjectFiles = damlSourceDirectory.value ** "daml.yaml"
        val useVersionedDarFileName = useVersionedDarName.value

        val buildDependencies = damlBuildOrder.value

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
        val log = streams.value.log

        val cache =
          FileFunction.cached(cacheDir) { _ => // ignoring the cache as we don't know the dependency

            // build the daml files in a sorted way, using the build order definition
            val projectFiles = damlProjectFiles.get.toList.sortWith(buildOrder)
            projectFiles.flatMap { projectFile =>
              buildDamlProject(
                log,
                sourceDirectory,
                buildDirectory,
                outputDirectory,
                useVersionedDarFileName,
                sourceDirectory.toPath.relativize(projectFile.toPath).toFile,
                damlCompilerVersion.value,
                damlLanguageVersions.value,
              )
            }.toSet
          }

        cache(allDamlFiles.get.toSet ++ dependencies).toSeq
      },
      // Declare dependency so that Daml packages in test scope may depend on packages in compile scope.
      (Test / damlBuild) := (Test / damlBuild).dependsOn(Compile / damlBuild).value,
      damlCheckProjectVersions := {
        val projectVersionOverride = ProjectVersionOverride(
          damlEnableProjectVersionOverride.value,
          damlProjectVersionOverride.value.getOrElse(version.value),
        )
        val damlProjectFiles = (damlSourceDirectory.value ** "daml.yaml").get

        damlProjectFiles.foreach(
          checkProjectVersions(
            projectVersionOverride,
            damlCompilerVersion.value,
            _,
          )
        )
      },
      damlUpdateProjectVersions := {
        // With Daml 0.13.56 characters are no longer allowed in project versions as
        // GHC does not like non-numbers in versions.
        val projectVersion = {
          val reg = "^([0-9]+\\.[0-9]+\\.[0-9]+)(-[^\\s]+)?$".r
          version.value match {
            case reg(vers, _) => vers
            case _ => throw new IllegalArgumentException(s"can not parse version ${version.value}")
          }
        }

        val overrideVersion = damlProjectVersionOverride.value
        val damlProjectFiles = (damlSourceDirectory.value ** "daml.yaml").get

        val projectVersionOverride = ProjectVersionOverride(
          damlEnableProjectVersionOverride.value,
          overrideVersion.getOrElse(projectVersion),
        )
        damlProjectFiles.foreach(
          updateProjectVersions(
            projectVersionOverride,
            damlCompilerVersion.value,
            _,
          )
        )
      },
      damlUpdateFixedDars := {
        val sourceDirectory = damlDarOutput.value
        val destinationDirectory = resourceDirectory.value / "dar"
        val fixedDars = damlFixedDars.value

        fixedDars.foreach(updateFixedDar(sourceDirectory, destinationDirectory, _))
      },
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

  override lazy val globalSettings: Seq[Def.Setting[_]] = Seq(
    damlCompilerVersion := Dependencies.daml_compiler_version,
    damlLanguageVersions := Dependencies.daml_language_versions,
    damlCodeGeneration := Seq(),
    damlFixedDars := Seq(),
    damlProjectVersionOverride := None,
    damlEnableProjectVersionOverride := true,
  )

  override lazy val projectSettings: Seq[Def.Setting[_]] =
    inConfig(Compile)(baseDamlPluginSettings) ++
      inConfig(Test)(baseDamlPluginSettings)

  /** Verify that the versions in the daml.yaml file match what is being used in the sbt project.
    * If a mismatch is found a [[sbt.internal.MessageOnlyException]] will be thrown.
    */
  private def checkProjectVersions(
      projectVersionOverride: ProjectVersionOverride,
      damlVersion: String,
      damlProjectFile: File,
  ): Unit = {
    require(
      damlProjectFile.exists,
      s"supplied daml.yaml must exist [${damlProjectFile.absolutePath}]",
    )

    val values = readDamlYaml(damlProjectFile)
    projectVersionOverride.foreach(ensureMatchingVersion(_, "version"))
    ensureMatchingVersion(damlVersion, "sdk-version")

    def ensureMatchingVersion(sbtVersion: String, fieldName: String): Unit = {
      val damlVersion = values.get(fieldName).toString
      // With Daml 0.13.56 characters are no longer allowed in project versions as
      // GHC does not like non-numbers in versions.
      val sbtNonSnapshotVersion = sbtVersion.stripSuffix("-SNAPSHOT")
      if (sbtNonSnapshotVersion != damlVersion) {
        throw new MessageOnlyException(
          s"daml.yaml $fieldName value [$damlVersion] does not match the '-SNAPSHOT'-stripped value in our sbt project [$sbtVersion] in file [$damlProjectFile]"
        )
      }
    }
  }

  /** Write the project and daml versions of our sbt project to the given daml.yaml project file.
    */
  private def updateProjectVersions(
      projectVersionOverride: ProjectVersionOverride,
      damlVersion: String,
      damlProjectFile: File,
  ): Unit = {
    require(
      damlProjectFile.exists,
      s"supplied daml.yaml must exist [${damlProjectFile.absolutePath}]",
    )

    val values = readDamlYaml(damlProjectFile)
    projectVersionOverride.foreach(values.put("version", _))

    values.put("sdk-version", damlVersion)

    val writer = new YamlWriter(new FileWriter(damlProjectFile))
    try {
      writer.write(values)
    } finally writer.close()
  }

  /** We intentionally take the unusual step of checking in certain DARs to ensure stable package ids across different Daml versions.
    * This task will take the dynamically built DAR and update the checked in version.
    */
  private def updateFixedDar(
      sourceDirectory: File,
      destinationDirectory: File,
      filename: String,
  ): Unit = {
    val sourcePath = sourceDirectory / filename
    val destinationPath = destinationDirectory / filename

    if (!sourcePath.exists) {
      throw new MessageOnlyException(
        s"Cannot update fixed DAR as DAR at path not found: [$sourcePath]"
      )
    }

    IO.copyFile(sourcePath, destinationPath)
  }

  private def buildDamlProject(
      log: Logger,
      sourceDirectory: File,
      buildDirectory: File,
      outputDirectory: File,
      useVersionedDarName: Boolean,
      relativeDamlProjectFile: File,
      damlVersion: String,
      damlLanguageVersions: Seq[String],
  ): Seq[File] = {

    val originalDamlProjectFile =
      sourceDirectory.toPath.resolve(relativeDamlProjectFile.toPath).toFile
    require(
      originalDamlProjectFile.exists,
      s"supplied daml.yaml must exist [${originalDamlProjectFile.absolutePath}]",
    )
    val url =
      s"https://storage.googleapis.com/daml-binaries/split-releases/$damlVersion/"

    def platform = {
      val osName = System.getProperty("os.name").toLowerCase
      val osArch = System.getProperty("os.arch").toLowerCase

      if (osName.startsWith("mac os x"))
        "macos"
      else if (osArch.startsWith("aarch") || osArch.startsWith("arm"))
        "linux-arm"
      else "linux-intel"
    }

    val damlc = ensureArtifactAvailable(
      url = url,
      artifactFilename = s"damlc-$damlVersion-$platform.tar.gz",
      damlVersion = damlVersion,
      tarballPath = Seq("damlc", "damlc"),
    )

    // so far canton system dars depend on daml-script, but maybe daml-triggers or others some day?
    val damlLibsDependencyTypes = Seq("daml-script")
    val damlLibsDependencyVersions = damlLanguageVersions.foldLeft(Seq(""))(_ :+ "-" + _)
    val damlScriptDars = for {
      depType <- damlLibsDependencyTypes
      depVersion <- damlLibsDependencyVersions
    } yield {
      (depType, s"$depType$depVersion")
    }
    val daml3ScriptDars =
      Seq(("daml-script", "daml3-script-1.dev"), ("daml-script", "daml3-script-stable-1.dev"))

    val damlLibsEnv = (for {
      (depType, artifactName) <- damlScriptDars ++ daml3ScriptDars
    } yield {
      ensureArtifactAvailable(
        url = url + s"$depType/",
        artifactFilename = s"$artifactName.dar",
        damlVersion = damlVersion,
        localSubdir = Some("daml-libs"),
      )
    }).headOption.map("DAML_SDK" -> _.getParentFile.getParentFile.getAbsolutePath).toSeq

    val projectBuildDirectory =
      buildDirectory.toPath.resolve(relativeDamlProjectFile.toPath).toAbsolutePath.getParent.toFile

    log.debug(
      s"building ${originalDamlProjectFile.getAbsoluteFile.getParentFile} in $projectBuildDirectory"
    )

    // copy project directory into target tree
    // the reason for this is that `daml build` caches files in a `.daml` directory of the source tree
    // making sbt to believe that the source code changed
    IO.copyDirectory(originalDamlProjectFile.getAbsoluteFile.getParentFile, projectBuildDirectory)

    val damlYamlMap = readDamlYaml(originalDamlProjectFile)
    val damlProjectName = damlYamlMap.get("name").toString
    val outputDar =
      if (!useVersionedDarName) outputDirectory / s"$damlProjectName.dar"
      else {
        val projectVersion = damlYamlMap.get("version").toString
        outputDirectory / s"$damlProjectName-$projectVersion.dar"
      }
    val processLogger = new BufferedLogger

    val result = Process(
      command = damlc.getAbsolutePath :: "build" ::
        // TODO(#16362): Consider removing the flag and split the definitions accordingly
        "--warn-bad-interface-instances=yes" ::
        "--project-root" :: projectBuildDirectory.toString ::
        "--output" :: outputDar.getAbsolutePath :: Nil,
      cwd = projectBuildDirectory,
      extraEnv = damlLibsEnv: _*, // env variable set so that damlc finds daml-script dar
    ) ! processLogger

    if (result != 0) {
      throw new MessageOnlyException(s"""
          |damlc build failed [$originalDamlProjectFile]:
          |${processLogger.output("  ")}
        """.stripMargin.trim)
    }

    Seq(outputDar)
  }

  private def readDamlYaml(damlProjectFile: File): JMap[String, Object] = {
    val reader = new YamlReader(new FileReader(damlProjectFile))
    try {
      reader.read(classOf[JMap[String, Object]])
    } finally reader.close()
  }

  private def ensureArtifactAvailable(
      url: String,
      artifactFilename: String,
      damlVersion: String,
      tarballPath: Seq[String] = Seq.empty,
      localSubdir: Option[String] = None,
  ): File = {
    import better.files.File

    val root =
      localSubdir.foldLeft(
        File(System.getProperty("user.home")) / ".cache" / "daml-build" / damlVersion
      )(_ / _)

    val artifact =
      if (tarballPath.nonEmpty) tarballPath.foldLeft(root)(_ / _) else root / artifactFilename

    this.synchronized {
      if (!artifact.exists) {
        val logger = new BufferedLogger()
        logger.out(s"Downloading missing $artifactFilename to ${root.path}")
        root.createDirectoryIfNotExists(createParents = true)

        Try {
          val curlWithBasicOptions = "curl" :: "-sSL" :: "--fail" :: Nil
          val credentials = url match {
            case artifactory if artifactory.startsWith("https://digitalasset.jfrog.io/") =>
              "--netrc" :: Nil // on dev machines look up artifactory credentials in ~/.netrc per https://everything.curl.dev/usingcurl/netrc
            case _maven => Nil // maven does not require credentials
          }
          val fileAndUrl =
            "-o" :: (root / artifactFilename).toJava.getPath :: (url + artifactFilename) :: Nil
          Process(curlWithBasicOptions ++ credentials ++ fileAndUrl) !! logger
        } match {
          case Success(str) =>
            if (str.nonEmpty)
              logger.output("OUTPUT: ")
          case Failure(t) =>
            throw new MessageOnlyException(
              s"Failed to download from ${url + artifactFilename}. Exception ${t.getMessage}"
            )
        }

        if (tarballPath.nonEmpty) {
          val tarball = root / artifactFilename
          logger.out(s"Downloaded damlc tarball to ${root.path}. Untarring ${tarball.pathAsString}")
          val result = Process(
            command = "tar" :: "xzf" :: tarball.pathAsString :: Nil,
            cwd = root.toJava,
          ) ! logger

          if (result == 0) {
            // best effort removal of tarball no longer needed to save space
            tarball.delete(swallowIOExceptions = true)
          } else {
            throw new MessageOnlyException(s"""
                                              |tar xzf ${tarball.pathAsString} has failed with exit code $result:
                                              |${logger.output("  ")}""".stripMargin.trim)
          }
        }
      }

      artifact.toJava
    }
  }

  /** Calls the Daml Codegen for the provided DAR file (hence, is suitable to use in a sourceGenerator task)
    */
  def generateCode(
      log: Logger,
      damlProjectDirectory: File,
      darFile: File,
      basePackageName: String,
      language: Codegen,
      managedSourceDir: File,
      damlVersion: String,
  ): Seq[File] = {
    require(
      damlProjectDirectory.exists,
      s"supplied daml project directory must exist [${damlProjectDirectory.absolutePath}]",
    )

    if (!darFile.exists())
      throw new MessageOnlyException(
        s"Codegen asked to generate code from nonexistent file: $darFile"
      )

    val (url, artifact, packageName, suffix, extraArgs) = language match {
      case Codegen.Java =>
        (
          s"https://repo.maven.apache.org/maven2/com/daml/codegen-jvm-main/$damlVersion/",
          s"codegen-jvm-main-$damlVersion.jar",
          basePackageName + ".java",
          "java",
          Seq("java"),
        )
      case Codegen.Scala =>
        (
          s"https://repo.maven.apache.org/maven2/com/daml/codegen-scala-main/$damlVersion/",
          s"codegen-scala-main-$damlVersion.jar",
          basePackageName,
          "scala",
          Seq(),
        )
    }

    val codegenJarPath = ensureArtifactAvailable(
      url = url,
      artifactFilename = artifact,
      damlVersion = damlVersion,
    ).getAbsolutePath

    log.debug(s"Running $language-codegen for $darFile into $managedSourceDir")

    val processLogger = new BufferedLogger

    // run the daml process using the working directory of the daml.yaml project file
    val result = Process(
      "java" +: "-jar" +: codegenJarPath +: (extraArgs ++ Seq(
        s"${darFile.getAbsolutePath}=$packageName",
        s"--output-directory=${managedSourceDir.getAbsolutePath}",
      )),
      damlProjectDirectory,
    ) ! processLogger

    if (result != 0) {
      throw new MessageOnlyException(s"""
           |java -jar $codegenJarPath failed [${darFile.getName}]:
           |${processLogger.output("  ")}
      """.stripMargin.trim)
    }

    // return all generated scala files
    (managedSourceDir ** s"*.$suffix").get
  }

  sealed trait ProjectVersionOverride extends Product with Serializable {
    def foreach(f: String => Unit): Unit
  }

  object ProjectVersionOverride {
    def apply(enableOverride: Boolean, overrideValue: => String): ProjectVersionOverride =
      if (enableOverride) Override(overrideValue) else DoNotOverride

    final case object DoNotOverride extends ProjectVersionOverride {
      override def foreach(f: String => Unit): Unit = ()
    }

    final case class Override(overrideVersion: String) extends ProjectVersionOverride {
      override def foreach(f: String => Unit): Unit = f(overrideVersion)
    }
  }
}
