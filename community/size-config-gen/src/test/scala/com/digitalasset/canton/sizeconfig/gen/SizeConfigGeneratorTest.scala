// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sizeconfig.gen

import com.digitalasset.canton.sizeconfig.gen.SizeConfigGenerator.*
import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, OptionValues}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters.*

class SizeConfigGeneratorTest
    extends AnyWordSpec
    with Matchers
    with EitherValues
    with OptionValues {

  // Fixtures: the test protos (src/test/protobuf), compiled by sbt-protoc into a descriptor set

  private val prefix = "com/daml/ledger/api/v2/"
  private val v2 = "com.daml.ledger.api.v2"
  private val pkg = scalaPackagePrefix
  private val out = "com/digitalasset/canton/ledger/api/sizeconfig/v2"

  // fixtures that generate successfully, one per generated package plus an ignored package
  private val valid = Seq(
    "fixture_public.proto",
    "admin/fixture_admin.proto",
    "interactive/fixture_interactive.proto",
    "testing/fixture_ignored.proto",
  )

  private lazy val protos: Seq[FileDescriptorProto] = {
    val in = getClass.getResourceAsStream("/size-config-fixtures.desc")
    try FileDescriptorSet.parseFrom(in).getFileList.asScala.toList
    finally in.close()
  }

  // all files of the set, built in order (protoc emits dependencies first)
  private lazy val allFileDescriptors: Map[String, FileDescriptor] =
    protos.foldLeft(Map.empty[String, FileDescriptor]) { (built, proto) =>
      val dependencies = proto.getDependencyList.asScala.map(built).toArray
      built + (proto.getName -> FileDescriptor.buildFrom(proto, dependencies))
    }

  private def fileDescriptors(names: Seq[String]): Seq[FileDescriptor] =
    names.map(name => allFileDescriptors(prefix + name))

  private def messageDescriptor(fullName: String): Descriptor =
    allFileDescriptors.valuesIterator
      .flatMap(fd => Option(fd.findMessageTypeByName(fullName.stripPrefix(fd.getPackage + "."))))
      .find(_.getFullName == fullName)
      .getOrElse(throw new RuntimeException(s"No fixture message $fullName"))

  private def buildModelE(
      exclusions: Set[String] = Set.empty,
      fileNames: Seq[String] = valid,
  ): Either[String, Seq[RootConfig]] =
    buildModel(filesToGenerate = fileDescriptors(fileNames), exclusions = exclusions)

  private def model(exclusions: Set[String] = Set.empty): Seq[RootConfig] = buildModelE(
    exclusions
  ).value

  private def messageNames(roots: Seq[RootConfig]): Seq[String] =
    roots.flatMap(_.members).map(_.protoFullName)

  private def config(roots: Seq[RootConfig], protoFullName: String): MessageConfig =
    roots.flatMap(_.members).find(_.protoFullName == protoFullName).value

  private def withExclusionsFile[A](lines: String*)(test: Path => A): A = {
    val file = Files.createTempFile("exclusions", ".txt")
    try {
      Files.write(file, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
      test(file)
    } finally Files.delete(file)
  }

  // runs the plugin end-to-end through the entry point used by sbt-protoc
  private def run(parameter: String): CodeGeneratorResponse = {
    val request = CodeGeneratorRequest
      .newBuilder()
      .addAllProtoFile(protos.asJava)
      .addAllFileToGenerate(valid.map(prefix + _).asJava)
      .setParameter(parameter)
      .build()
    CodeGeneratorResponse.parseFrom(SizeConfigGenerator.run(request.toByteArray))
  }

  "SizeConfigGenerator" should {
    "generate the config classes and declare proto3 optional support" in {
      withExclusionsFile() { exclusionsFile =>
        val response = run(s"exclusions=$exclusionsFile")
        response.hasError shouldBe false
        (response.getSupportedFeatures & CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL.getNumber) should not be 0
        response.getFileList.asScala.map(_.getName) should contain allOf (
          s"$out/FixtureFormatSizeConfig.scala",
          s"$out/FixtureResponseSizeConfig.scala",
          s"$out/PublicLapiSizeConfig.scala",
          s"$out/admin/AdminLapiSizeConfig.scala",
          s"$out/interactive/InteractiveLapiSizeConfig.scala",
        )
      }
    }

    "not generate classes for imported files that are not among the files to generate" in {
      withExclusionsFile() { exclusions =>
        val response = run(s"exclusions=$exclusions")
        response.hasError shouldBe false
        // the imported file is in the request (as value.proto is), but not in file_to_generate
        protos.map(_.getName) should contain(prefix + "fixture_imported_only.proto")
        response.getFileList.asScala.map(_.getName) should not contain
          s"$out/FixtureImportedValueSizeConfig.scala"
      }
    }

    "not generate classes for messages in the exclusions list" in {
      withExclusionsFile("# responses", s"$v2.FixtureResponse") { exclusions =>
        val response = run(s"exclusions=$exclusions")
        response.hasError shouldBe false
        val names = response.getFileList.asScala.map(_.getName)
        names should not contain s"$out/FixtureResponseSizeConfig.scala"
        names should contain(s"$out/FixtureFormatSizeConfig.scala")
      }
    }

    "fail with an unknown exclusion" in {
      withExclusionsFile(s"$v2.DoesNotExist") { exclusions =>
        run(s"exclusions=$exclusions").getError should include(s"$v2.DoesNotExist")
      }
    }

    "fail with a missing exclusion file" in {
      val missing = Files.createTempFile("exclusions", ".txt")
      Files.delete(missing)
      run(s"exclusions=$missing").getError should include("Cannot read exclusions file")
    }

    "fail when missing parameters" in {
      run("").getError should include("Expected the plugin parameter 'exclusions=<file>'")
    }
  }

  "parsePluginParameter" should {
    "read the exclusions file, trimming whitespace" in {
      parsePluginParameter(" exclusions = /tmp/e.txt ").value shouldBe Paths.get("/tmp/e.txt")
    }

    "reject a missing, empty or unknown parameter" in {
      Seq("", "exclusions", "exclusions= ", "foo=/e").foreach { parameter =>
        parsePluginParameter(parameter).isLeft shouldBe true
      }
    }
  }

  "parseExclusionsList" should {
    "read one fully-qualified name per line" in {
      parseExclusionsList(Seq(s"$v2.A", s"$v2.admin.B")).value shouldBe Set(
        s"$v2.A",
        s"$v2.admin.B",
      )
    }

    "ignore comments, trailing comments, blank lines and surrounding whitespace" in {
      parseExclusionsList(
        Seq("# responses", "", "   ", s"  $v2.A  ", s"$v2.B # trailing comment")
      ).value shouldBe Set(s"$v2.A", s"$v2.B")
    }

    "accept nested message names" in {
      parseExclusionsList(Seq(s"$v2.Outer.Inner")).value shouldBe Set(s"$v2.Outer.Inner")
    }

    "reject lines that are not fully-qualified names, reporting the line number" in {
      parseExclusionsList(Seq(s"$v2.A", "NotQualified")).left.value should include("line 2")
      parseExclusionsList(Seq(s"$v2.A B")).left.value should include(
        "not a fully-qualified proto message name"
      )
      parseExclusionsList(Seq("com..daml")).isLeft shouldBe true
    }
  }

  "readExclusionsFile" should {
    "read the exclusions file" in {
      withExclusionsFile("# header", s"$v2.A") { file =>
        readExclusionsFile(file).value shouldBe Set(s"$v2.A")
      }
    }

    "fail for a missing file" in {
      val file = Files.createTempFile("exclusions", ".txt")
      Files.delete(file)
      readExclusionsFile(file).left.value should include("Cannot read exclusions file")
    }
  }

  "hasSizeLimit" should {
    val request = messageDescriptor(s"$v2.FixtureGetRequest")

    "be true for a field with [(size_limit) = true]" in {
      hasSizeLimit(request.findFieldByName("annotated_string")) shouldBe true
      hasSizeLimit(request.findFieldByName("payload")) shouldBe true
    }

    "be false for a field with [(size_limit) = false]" in {
      hasSizeLimit(request.findFieldByName("not_annotated")) shouldBe false
    }

    "be false for a field without the option" in {
      hasSizeLimit(request.findFieldByName("plain")) shouldBe false
    }
  }

  private val roots = model()
  private val generatedMessages = messageNames(roots)

  "buildModel" should {
    "generate a class for every message with a map, repeated, bytes or size_limit field" in {
      generatedMessages shouldBe Seq(
        s"$v2.FixtureFilters",
        s"$v2.FixtureFormat",
        s"$v2.FixtureGetRequest",
        s"$v2.FixtureGetRequest.Nested",
        s"$v2.FixtureOuter.Inner",
        s"$v2.FixtureResponse",
        s"$v2.admin.FixtureUpdateRequest",
        s"$v2.interactive.FixturePrepareRequest",
      )
    }

    "name every parameter <field>SizeLimit, in declaration order" in {
      config(roots = roots, protoFullName = s"$v2.FixtureGetRequest").params.map(p =>
        p.protoField -> p.scalaName
      ) shouldBe
        Seq(
          "act_as" -> "actAsSizeLimit",
          "page_token" -> "pageTokenSizeLimit",
          "transaction_hash" -> "transactionHashSizeLimit",
          "annotated_string" -> "annotatedStringSizeLimit",
          "payload" -> "payloadSizeLimit",
          "blob" -> "blobSizeLimit",
        )
    }

    "document each parameter" in {
      val docs =
        config(roots = roots, protoFullName = s"$v2.FixtureGetRequest").params
          .map(p => p.protoField -> p.scaladoc)
          .toMap
      docs("act_as") shouldBe "max number of elements in repeated field `act_as`"
      docs("page_token") shouldBe "max size in bytes of bytes field `page_token`"
      docs("annotated_string") shouldBe
        "size limit of field `annotated_string` (marked with `size_limit`)"
      docs("payload") shouldBe
        "size limit of field `payload` (marked with `size_limit`)"
    }

    "configure the map field itself and not emit the synthetic map entry" in {
      config(roots = roots, protoFullName = s"$v2.FixtureFormat").params shouldBe Seq(
        ConfigParam(
          protoField = "filters_by_party",
          scalaName = "filtersByPartySizeLimit",
          scaladoc = "max number of entries in map field `filters_by_party`",
        )
      )
      generatedMessages.filter(_.endsWith("Entry")) shouldBe empty
    }

    "skip messages without parameters, but not their nested messages" in {
      Seq(
        s"$v2.FixtureListRequest",
        s"$v2.FixtureNoParams",
        s"$v2.FixtureOuter",
        s"$v2.admin.FixtureUsesPublic",
      ).foreach(name => generatedMessages should not contain name)
      generatedMessages should contain(s"$v2.FixtureOuter.Inner")
    }

    "ignore packages other than v2, v2.admin and v2.interactive" in {
      generatedMessages.filter(_.contains(".testing.")) shouldBe empty
    }

    "ignore imported files that are not among the files to generate, even in a generated package" in {
      // like value.proto from ledger-api-value-proto: same package, imported by the Ledger API protos
      messageDescriptor(s"$v2.FixtureImportedValue").getFile.getPackage shouldBe v2
      generatedMessages should not contain s"$v2.FixtureImportedValue"
      config(roots = roots, protoFullName = s"$v2.FixtureGetRequest").params
        .map(_.protoField) should not contain
        "imported_value"
      // but if the user excludes it, we report it as unknown since it is not among the files to generate for
      buildModelE(exclusions = Set(s"$v2.FixtureImportedValue")).left.value should include(
        s"$v2.FixtureImportedValue"
      )
    }

    "name nested messages after their path and map proto packages to Scala packages" in {
      config(roots = roots, protoFullName = s"$v2.FixtureGetRequest.Nested").className shouldBe
        "FixtureGetRequestNestedSizeConfig"
      config(
        roots = roots,
        protoFullName = s"$v2.FixtureOuter.Inner",
      ).className shouldBe "FixtureOuterInnerSizeConfig"

      def scalaPackageOf(protoFullName: String): String =
        roots.find(_.members.exists(_.protoFullName == protoFullName)).value.scalaPackage

      scalaPackageOf(s"$v2.FixtureFormat") shouldBe s"$pkg.v2"
      scalaPackageOf(s"$v2.admin.FixtureUpdateRequest") shouldBe s"$pkg.v2.admin"
      scalaPackageOf(s"$v2.interactive.FixturePrepareRequest") shouldBe s"$pkg.v2.interactive"
    }

    "group the classes in one root per package, each message exactly once" in {
      roots.map(r => r.className -> r.scalaPackage) shouldBe Seq(
        "PublicLapiSizeConfig" -> s"$pkg.v2",
        "AdminLapiSizeConfig" -> s"$pkg.v2.admin",
        "InteractiveLapiSizeConfig" -> s"$pkg.v2.interactive",
      )
      generatedMessages.filter(_ == s"$v2.FixtureFormat").size shouldBe 1
      // referenced from the admin package, but stays in the public root
      roots.find(_.protoPackage == v2).value.members.map(memberName) should contain(
        "fixtureFormat"
      )
    }

    "be deterministic, independent of the input file order" in {
      buildModelE(fileNames = valid.reverse) shouldBe buildModelE()
    }

    "fail on size_limit on a repeated, map or bytes field" in {
      buildModelE(fileNames = Seq("fixture_invalid.proto")).left.value should include(
        "`size_limit` is redundant, map, repeated and bytes fields always get a parameter"
      )
    }

    "fail on class name collisions" in {
      buildModelE(fileNames = Seq("fixture_collision.proto")).left.value should include(
        "FixtureABCSizeConfig is generated for more than one message"
      )
    }

    "fail on a Ledger API package that is neither generated nor ignored" in {
      buildModelE(fileNames =
        valid :+ "interactive/transaction/v2/fixture_unknown_package.proto"
      ).left.value should include(
        s"$v2.interactive.transaction.v2 is neither generated nor ignored"
      )
    }
  }

  "The exclusion list" should {
    "remove an excluded message from the roots" in {
      messageNames(
        model(exclusions = Set(s"$v2.FixtureResponse"))
      ) should not contain s"$v2.FixtureResponse"
    }

    "keep the other messages of the same file" in {
      messageNames(
        model(exclusions = Set(s"$v2.FixtureResponse"))
      ) should contain allElementsOf Seq(
        s"$v2.FixtureFormat",
        s"$v2.FixtureGetRequest",
      )
    }

    "exclude a nested message without excluding its parent" in {
      val names = messageNames(model(exclusions = Set(s"$v2.FixtureGetRequest.Nested")))
      names should contain(s"$v2.FixtureGetRequest")
      names should not contain s"$v2.FixtureGetRequest.Nested"
    }

    "keep the root of a package whose messages are all excluded, with no members" in {
      val roots = model(exclusions = Set(s"$v2.interactive.FixturePrepareRequest"))
      roots.find(_.protoPackage == s"$v2.interactive").value.members shouldBe empty
    }

    "fail on entries that name no message of the generated packages" in {
      buildModelE(exclusions = Set(s"$v2.DoesNotExist")).left.value should include(
        s"$v2.DoesNotExist"
      )
      buildModelE(exclusions = Set(s"$v2.testing.FixtureIgnoredRequest")).left.value should include(
        s"$v2.testing.FixtureIgnoredRequest"
      )
    }
  }

  "camelCase" should {
    "convert snake_case field names" in {
      camelCase("filters_by_party") shouldBe "filtersByParty"
      camelCase("act_as") shouldBe "actAs"
      camelCase("type") shouldBe "type"
    }
  }

  "render" should {
    val foo = MessageConfig(
      protoFullName = s"$v2.Foo",
      className = "FooSizeConfig",
      params = Seq(
        ConfigParam(
          protoField = "act_as",
          scalaName = "actAsSizeLimit",
          scaladoc = "max number of elements in repeated field `act_as`",
        ),
        ConfigParam(
          protoField = "status",
          scalaName = "statusSizeLimit",
          scaladoc = "size limit of field `status` (marked with `size_limit`)",
        ),
      ),
    )
    val roots = Seq(
      RootConfig(
        protoPackage = v2,
        scalaPackage = "p.v2",
        className = "PublicLapiSizeConfig",
        members = Seq(foo),
      ),
      RootConfig(
        protoPackage = s"$v2.admin",
        scalaPackage = "p.v2.admin",
        className = "AdminLapiSizeConfig",
        members = Seq.empty,
      ),
    )
    val rendered = render(roots)
    def content(path: String): String = rendered.find(_.getName == path).value.getContent

    "write one file per class" in {
      rendered.map(_.getName) shouldBe Seq(
        "p/v2/FooSizeConfig.scala",
        "p/v2/PublicLapiSizeConfig.scala",
        "p/v2/admin/AdminLapiSizeConfig.scala",
      )
    }

    "render a message config as a case class without defaults, with pureconfig instances" in {
      content("p/v2/FooSizeConfig.scala") shouldBe
        """package p.v2
          |
          |// Generated by SizeConfigGenerator. DO NOT EDIT.
          |// Source: com.daml.ledger.api.v2.Foo
          |
          |import com.digitalasset.canton.config.RequireTypes.PositiveInt
          |import pureconfig.generic.ProductHint
          |import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
          |import pureconfig.{ConfigReader, ConfigWriter}
          |
          |/** Size limits for proto message `com.daml.ledger.api.v2.Foo`.
          |  *
          |  * @param actAsSizeLimit
          |  *   max number of elements in repeated field `act_as`
          |  *
          |  * @param statusSizeLimit
          |  *   size limit of field `status` (marked with `size_limit`)
          |  */
          |final case class FooSizeConfig(
          |    actAsSizeLimit: PositiveInt,
          |    statusSizeLimit: PositiveInt,
          |)
          |
          |object FooSizeConfig {
          |  private implicit val productHint: ProductHint[FooSizeConfig] =
          |    ProductHint[FooSizeConfig](allowUnknownKeys = false)
          |  implicit val fooSizeConfigReader: ConfigReader[FooSizeConfig] = deriveReader
          |  implicit val fooSizeConfigWriter: ConfigWriter[FooSizeConfig] = deriveWriter
          |}
          |""".stripMargin
    }

    "render a package root with one member per message config" in {
      content("p/v2/PublicLapiSizeConfig.scala") should include(
        """final case class PublicLapiSizeConfig(
          |    foo: FooSizeConfig,
          |)""".stripMargin
      )
    }
  }
}
