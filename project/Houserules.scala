import sbt.Keys._
import sbt._
import wartremover.WartRemover.autoImport._
import wartremover.contrib.ContribWart

/** Settings for all JVM projects in this build. Contains compiler flags,
  * settings for tests, etc.
  */
object JvmRulesPlugin extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = sbt.plugins.JvmPlugin

  import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.{
    HeaderLicense,
    HeaderPattern,
    headerLicense,
    headerMappings,
  }
  import de.heikoseeberger.sbtheader.{
    LineCommentCreator,
    CommentStyle => HeaderCommentStyle,
    FileType => HeaderFileType,
  }

  lazy val cantonRepoHeaderSettings = Seq(
    // Configure sbt-header to manage license notices in files
    headerLicense := Some(
      HeaderLicense
        .Custom( // When updating the year here, also update .circleci/enterpriseAppHeaderCheck.sh and damlRepoHeaderSettings below
          """|Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates
             |
             |Proprietary code. All rights reserved.
             |""".stripMargin
        )
    ),
    // Add license header to SQL files too
    headerMappings := headerMappings.value ++ Map(
      HeaderFileType("daml") -> dashCommentStyle,
      HeaderFileType("java") -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType("proto") -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType.sh -> HeaderCommentStyle.hashLineComment,
      HeaderFileType("sql") -> dashCommentStyle,
      HeaderFileType("rst") -> dotCommentStyle,
    ),
  )

  // In community-subprojects, we use the Daml repo's license header so that we can more easily share the same
  // files in the open-source Daml repo for consistency, and also to refer to the Apache 2.0 license in place of the
  // "Proprietary code." reference that we maintain in the enterprise edition and closed-source canton repo.
  lazy val damlRepoHeaderSettings = Seq(
    headerLicense := Some(
      HeaderLicense
        .Custom( // When updating the year here, also update .circleci/enterpriseAppHeaderCheck.sh and cantonRepoHeaderSettings above
          """|Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
             |SPDX-License-Identifier: Apache-2.0
             |""".stripMargin
        )
    )
  )

  lazy val dashCommentStyle = HeaderCommentStyle(
    new LineCommentCreator("--"),
    HeaderPattern.commentStartingWith("--"),
  )

  lazy val dotCommentStyle = HeaderCommentStyle(
    new LineCommentCreator(s"..${System.lineSeparator()}    "),
    HeaderPattern.commentStartingWith(s"..${System.lineSeparator()}    "),
  )

  override def projectSettings =
    Seq(
      javacOptions ++= Seq("-encoding", "UTF-8", "-Werror"),
      scalacOptions ++= Seq("-encoding", "UTF-8", "-language:postfixOps"),
      scalacOptions ++= {
        if (System.getProperty("canton-disable-warts") == "true") Seq()
        else
          Seq(
            "-feature",
            "-unchecked",
            "-deprecation",
            "-Xlint:_,-unused",
            "-Xmacro-settings:materialize-derivations",
            "-Xfatal-warnings",
            "-Wconf:cat=unused-imports:info", //reports unused-imports without counting them as warnings, and without causing -Werror to fail.
            "-Ywarn-dead-code",
            "-Ywarn-numeric-widen",
            "-Ywarn-value-discard", // Gives a warning for functions declared as returning Unit, but the body returns a value
            "-Ywarn-unused:imports",
            "-Ywarn-unused:implicits",
            "-Ywarn-unused:locals",
            "-Vimplicits",
            "-Vtype-diffs",
            "-Xsource:3",
          )
      },
      Test / scalacOptions --= Seq("-Ywarn-value-discard"), // disable value discard check on tests
      addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
      Compile / compile / wartremoverErrors ++= {
        if (System.getProperty("canton-disable-warts") == "true") Seq()
        else
          Seq(
            Wart.AsInstanceOf,
            Wart.EitherProjectionPartial,
            Wart.Enumeration,
            Wart.IsInstanceOf,
            Wart.JavaConversions,
            Wart.Null,
            Wart.Option2Iterable,
            Wart.OptionPartial,
            Wart.Product,
            Wart.Return,
            Wart.Serializable,
            Wart.TraversableOps,
            Wart.TryPartial,
            Wart.Var,
            Wart.While,
            ContribWart.UnintendedLaziness,
          )
      },
      Test / compile / wartremoverErrors := {
        if (System.getProperty("canton-disable-warts") == "true") Seq()
        else
          Seq(
            Wart.EitherProjectionPartial,
            Wart.Enumeration,
            Wart.JavaConversions,
            Wart.Option2Iterable,
            Wart.OptionPartial,
            Wart.Product,
            Wart.Return,
            Wart.Serializable,
            Wart.While,
            ContribWart.UnintendedLaziness,
          )
      },
      // Disable wart checks on generated code
      wartremoverExcluded += (Compile / sourceManaged).value,
      //licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
      //
      // allow sbt to pull scaladoc from managed dependencies if referenced in our ScalaDoc links
      autoAPIMappings := true,
      //
      // 'slowpoke'/notification message if tests run for more than 5mins, repeat at 30s intervals from there
      Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "120", "30"),
      //
      // CHP: Disable output for successful tests
      // G: But after finishing tests for a module, output summary of failed tests for that module, with full stack traces
      // F: Show full stack traces for exceptions in tests (at the time when they occur)
      Test / testOptions += Tests.Argument("-oCHPGF"),
      //
      // to allow notifications and alerts during test runs (like slowpokes^)
      Test / logBuffered := false,
    ) ++ cantonRepoHeaderSettings
}
