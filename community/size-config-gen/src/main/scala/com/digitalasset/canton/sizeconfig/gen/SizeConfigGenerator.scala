// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sizeconfig.gen

import com.google.protobuf.Descriptors.FieldDescriptor.Type
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor, FileDescriptor}
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse
import protocgen.{CodeGenApp, CodeGenRequest, CodeGenResponse}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters.*
import scala.util.control.NonFatal

/** Protoc plugin generating size-limit config classes for the Ledger API messages.
  *
  * The plugin parameter `exclusions=<file>` names the file listing the fully-qualified names of the
  * messages for which no config class is generated.
  */
object SizeConfigGenerator extends CodeGenApp {

  final case class ConfigParam(protoField: String, scalaName: String, scaladoc: String)

  final case class MessageConfig(
      protoFullName: String,
      className: String,
      params: Seq[ConfigParam],
  )

  // aggregates the config classes of a proto package
  final case class RootConfig(
      protoPackage: String,
      scalaPackage: String,
      className: String,
      members: Seq[MessageConfig],
  )

  val scalaPackagePrefix: String = "com.digitalasset.canton.ledger.api.sizeconfig"

  // must match `optional bool size_limit = 51000;` in size_config_options.proto
  val sizeLimitFieldNumber: Int = 51000

  private val classSuffix = "SizeConfig"

  // the proto packages that are generated with a root class, and the name of that class; these are
  // the only generated packages: every other package of the files to generate must be listed in
  // `ignoredPackages`
  private val generatedPackages = Seq(
    "com.daml.ledger.api.v2" -> "PublicLapiSizeConfig",
    "com.daml.ledger.api.v2.admin" -> "AdminLapiSizeConfig",
    "com.daml.ledger.api.v2.interactive" -> "InteractiveLapiSizeConfig",
  )

  // proto packages that are deliberately not configured; listed explicitly so that a package added
  // to the Ledger API fails the generation instead of being silently left without size limits
  private val ignoredPackages = Set(
    "com.daml.ledger.api.v2.testing",
    // bounded as a whole by the size limit of `PreparedTransaction.transaction`
    "com.daml.ledger.api.v2.interactive.transaction.v1",
  )

  private val pluginParameter = s"""\\s*exclusions\\s*=\\s*(\\S.*?)\\s*""".r

  private val positiveIntImport = "import com.digitalasset.canton.config.RequireTypes.PositiveInt"
  private val pureconfigImports = Seq(
    "import pureconfig.generic.ProductHint",
    "import pureconfig.generic.semiauto.{deriveReader, deriveWriter}",
    "import pureconfig.{ConfigReader, ConfigWriter}",
  )

  override def process(request: CodeGenRequest): CodeGenResponse =
    generate(request) match {
      case Right(files) =>
        CodeGenResponse.succeed(
          files = files,
          // the Ledger API protos use proto3 `optional`
          supportedFeatures = Set(CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL),
        )
      case Left(error) => CodeGenResponse.fail(error)
    }

  private[gen] def generate(
      request: CodeGenRequest
  ): Either[String, Seq[CodeGeneratorResponse.File]] =
    for {
      exclusionsFile <- parsePluginParameter(request.parameter)
      exclusions <- readExclusionsFile(exclusionsFile)
      roots <- buildModel(filesToGenerate = request.filesToGenerate, exclusions = exclusions)
    } yield render(roots)

  private[gen] def parsePluginParameter(parameter: String): Either[String, Path] =
    parameter match {
      case pluginParameter(file) => Right(Paths.get(file))
      case _ => Left(s"Expected the plugin parameter 'exclusions=<file>', got '$parameter'")
    }

  private[gen] def parseExclusionsList(lines: Seq[String]): Either[String, Set[String]] = {
    val names = lines.map(_.takeWhile(_ != '#').trim).zipWithIndex.filter(_._1.nonEmpty)
    names
      .collectFirst {
        case (name, index)
            if !"""[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+""".r.pattern
              .matcher(name)
              .matches() =>
          s"Exclusions line ${index + 1}: '$name' is not a fully-qualified proto message name"
      }
      .toLeft(names.map(_._1).toSet)
  }

  private[gen] def readExclusionsFile(file: Path): Either[String, Set[String]] =
    (try Right(Files.readAllLines(file, StandardCharsets.UTF_8).asScala.toList)
    catch {
      case NonFatal(e) => Left(s"Cannot read exclusions file $file: ${e.getMessage}")
    }).flatMap(parseExclusionsList)

  // reads the `size_limit` custom field option declared in size_config_options.proto
  private[gen] def hasSizeLimit(field: FieldDescriptor): Boolean =
    field.getOptions.getUnknownFields
      .getField(sizeLimitFieldNumber)
      .getVarintList
      .asScala
      .lastOption
      .map(_.longValue())
      .contains(1L)

  // one root per generated package, holding the config classes of its messages
  private[gen] def buildModel(
      filesToGenerate: Seq[FileDescriptor],
      exclusions: Set[String],
  ): Either[String, Seq[RootConfig]] = {
    val generatedProtoPackages = generatedPackages.map(_._1).toSet
    val filesByPackage = filesToGenerate.groupBy(_.getPackage)

    // a package that is neither generated nor ignored would get no size limits at all
    val unknownPackages = filesByPackage.keys.toSeq
      .filterNot(generatedProtoPackages.contains)
      .filterNot(ignoredPackages.contains)
      .sorted
      .map(protoPackage =>
        s"Proto package $protoPackage is neither generated nor ignored: " +
          "add it to generatedPackages or ignoredPackages in SizeConfigGenerator"
      )

    val packageConfigs = generatedPackages.map { case (protoPackage, rootClassName) =>
      packageConfig(
        protoPackage = protoPackage,
        rootClassName = rootClassName,
        files = filesByPackage.getOrElse(protoPackage, Seq.empty),
        exclusions = exclusions,
      )
    }

    val allNames = packageConfigs.flatMap(_.messageNames).toSet
    val unknownExclusions = exclusions.diff(allNames).toSeq.sorted.map { name =>
      s"Exclusion '$name' does not name a message in packages ${generatedProtoPackages.mkString(", ")}"
    }

    (unknownPackages ++ unknownExclusions ++ packageConfigs.flatMap(_.errors)) match {
      case Seq() => Right(packageConfigs.map(_.root))
      case errors => Left(errors.mkString("\n"))
    }
  }

  // `messageNames` lists all messages of the package, including the excluded ones and those without
  // a config class, to validate the exclusions against
  private final case class PackageConfig(
      root: RootConfig,
      messageNames: Seq[String],
      errors: Seq[String],
  )

  private def packageConfig(
      protoPackage: String,
      rootClassName: String,
      files: Seq[FileDescriptor],
      exclusions: Set[String],
  ): PackageConfig = {
    val scalaPackage = toScalaPackage(protoPackage)
    val messages = files
      .flatMap(_.getMessageTypes.asScala.flatMap(withNested))
      // synthetic map entries: the map field itself is configured
      .filterNot(_.getOptions.getMapEntry)
      .sortBy(_.getFullName)

    val results = messages
      .filterNot(message => exclusions.contains(message.getFullName))
      .map(messageConfig)
    val messageErrors = results.collect { case Left(error) => error }
    val messageConfigs = results.collect { case Right(Some(config)) => config }

    val collisionErrors = messageConfigs
      .groupBy(_.className)
      .collect {
        case (memberClassName, clashing) if clashing.size > 1 =>
          s"Class name $scalaPackage.$memberClassName is generated for more than one message: ${clashing.map(_.protoFullName).mkString(", ")}"
      }
      .toSeq
      .sorted

    PackageConfig(
      root = RootConfig(
        protoPackage = protoPackage,
        scalaPackage = scalaPackage,
        className = rootClassName,
        members = messageConfigs,
      ),
      messageNames = messages.map(_.getFullName),
      errors = messageErrors ++ collisionErrors,
    )
  }

  // `None` if the message has no field that needs a size limit
  private def messageConfig(message: Descriptor): Either[String, Option[MessageConfig]] = {
    val params = message.getFields.asScala.toSeq.map(configParam)
    val errors = params.collect { case Left(error) => error }
    if (errors.nonEmpty) Left(errors.mkString("\n"))
    else {
      val found = params.collect { case Right(Some(param)) => param }
      val duplicateNames = found.groupBy(_.scalaName).collect {
        case (name, ps) if ps.size > 1 =>
          s"${message.getFullName}: fields ${ps.map(_.protoField).mkString(", ")} all map to parameter $name"
      }
      if (duplicateNames.nonEmpty) Left(duplicateNames.mkString("\n"))
      else
        Right(
          if (found.isEmpty) None
          else
            Some(
              MessageConfig(
                protoFullName = message.getFullName,
                className = className(message),
                params = found,
              )
            )
        )
    }
  }

  private def configParam(field: FieldDescriptor): Either[String, Option[ConfigParam]] = {
    val name = field.getName
    // maps are repeated fields too, so check them first
    val implicitScaladoc =
      if (field.isMapField) Some(s"max number of entries in map field `$name`")
      else if (field.isRepeated) Some(s"max number of elements in repeated field `$name`")
      else if (field.getType == Type.BYTES) Some(s"max size in bytes of bytes field `$name`")
      else None
    (implicitScaladoc, hasSizeLimit(field)) match {
      case (Some(_), true) =>
        Left(
          s"${field.getFullName}: `size_limit` is redundant, map, repeated and bytes fields always get a parameter"
        )
      case (Some(scaladoc), false) => Right(Some(param(field = field, scaladoc = scaladoc)))
      case (None, true) =>
        val scaladoc = s"size limit of field `$name` (marked with `size_limit`)"
        Right(Some(param(field = field, scaladoc = scaladoc)))
      case (None, false) => Right(None)
    }
  }

  private def param(field: FieldDescriptor, scaladoc: String): ConfigParam =
    ConfigParam(
      protoField = field.getName,
      scalaName = camelCase(field.getName) + "SizeLimit",
      scaladoc = scaladoc,
    )

  private def withNested(message: Descriptor): Seq[Descriptor] =
    message +: message.getNestedTypes.asScala.toSeq.flatMap(withNested)

  // `Outer.Inner` becomes `OuterInnerSizeConfig`
  private def className(message: Descriptor): String =
    message.getFullName
      .stripPrefix(message.getFile.getPackage + ".")
      .split('.')
      .mkString + classSuffix

  // `com.daml.ledger.api.v2.admin` becomes `<scalaPackagePrefix>.v2.admin`
  private def toScalaPackage(protoPackage: String): String =
    scalaPackagePrefix + protoPackage.stripPrefix("com.daml.ledger.api")

  // name of the member referring to a message config in its package root
  private[gen] def memberName(message: MessageConfig): String =
    lowerFirst(message.className.stripSuffix(classSuffix))

  private[gen] def camelCase(snake: String): String =
    snake.split('_').filter(_.nonEmpty).toList match {
      case Nil => snake
      case first :: rest => lowerFirst(first) + rest.map(upperFirst).mkString
    }

  private def lowerFirst(s: String): String =
    s.headOption.fold(s)(first => s"${first.toLower}${s.drop(1)}")

  private def upperFirst(s: String): String =
    s.headOption.fold(s)(first => s"${first.toUpper}${s.drop(1)}")

  // Rendering: one file per class. Only basic formatting is done here; the build runs scalafmt on the
  // generated files.

  private[gen] def render(roots: Seq[RootConfig]): Seq[CodeGeneratorResponse.File] =
    roots.flatMap(root => root.members.map(renderMessage(root.scalaPackage, _))) ++
      roots.map(renderRoot)

  private def renderMessage(
      scalaPackage: String,
      message: MessageConfig,
  ): CodeGeneratorResponse.File =
    createConfigFile(
      scalaPackage = scalaPackage,
      className = message.className,
      source = message.protoFullName,
      imports = positiveIntImport +: pureconfigImports,
      scaladoc = s"Size limits for proto message `${message.protoFullName}`." +:
        message.params.map(p => s"@param ${p.scalaName}\n  ${p.scaladoc}"),
      fields = message.params.map(p => s"${p.scalaName}: PositiveInt"),
    )

  private def renderRoot(root: RootConfig): CodeGeneratorResponse.File =
    createConfigFile(
      scalaPackage = root.scalaPackage,
      className = root.className,
      source = s"proto package ${root.protoPackage}",
      imports = pureconfigImports,
      scaladoc =
        Seq(s"Size limits for the Ledger API messages in proto package `${root.protoPackage}`."),
      fields = root.members.map(m => s"${memberName(m)}: ${m.className}"),
    )

  private def createConfigFile(
      scalaPackage: String,
      className: String,
      source: String,
      imports: Seq[String],
      scaladoc: Seq[String],
      fields: Seq[String],
  ): CodeGeneratorResponse.File = {
    val instance = lowerFirst(className)
    val scaladocLines = scaladoc.mkString("\n\n").split('\n').map(l => s"  * $l".stripTrailing)
    val content =
      s"""package $scalaPackage
         |
         |// Generated by ${getClass.getSimpleName.stripSuffix("$")}. DO NOT EDIT.
         |// Source: $source
         |
         |${imports.mkString("\n")}
         |
         |/**${scaladocLines.mkString("\n").drop(3)}
         |  */
         |final case class $className(
         |${fields.map(f => s"    $f,").mkString("\n")}
         |)
         |
         |object $className {
         |  private implicit val productHint: ProductHint[$className] =
         |    ProductHint[$className](allowUnknownKeys = false)
         |  implicit val ${instance}Reader: ConfigReader[$className] = deriveReader
         |  implicit val ${instance}Writer: ConfigWriter[$className] = deriveWriter
         |}
         |""".stripMargin
    CodeGeneratorResponse.File
      .newBuilder()
      .setName(s"${scalaPackage.replace('.', '/')}/$className.scala")
      .setContent(content)
      .build()
  }
}
