import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.*
import de.heikoseeberger.sbtheader.{
  LineCommentCreator,
  CommentStyle => HeaderCommentStyle,
  FileType => HeaderFileType,
}
import sbt.Keys.*
import sbt.*
import wartremover.Wart
import wartremover.WartRemover.autoImport.*
import wartremover.contrib.ContribWart
import Scala3Migration.onScalaVersion

/** Settings for all JVM projects in this build. Contains compiler flags, settings for tests, etc.
  */
object HouseRules extends AutoPlugin {
  private val enableUnusedSymbolsChecks = true // Can be turned to false during development

  override def trigger = allRequirements
  override def requires = sbt.plugins.JvmPlugin && de.heikoseeberger.sbtheader.HeaderPlugin

  lazy val cantonRepoHeaderSettings = Seq(
    // Configure sbt-header to manage license notices in files
    headerLicense := Some(
      HeaderLicense
        .Custom( // When updating the year here, also update damlRepoHeaderSettings below
          """|Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
             |Proprietary code. All rights reserved.
             |""".stripMargin
        )
    ),
    headerMappings := headerMappings.value ++ Map(
      HeaderFileType("daml") -> dashCommentStyle,
      HeaderFileType("java") -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType("proto") -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType.sh -> HeaderCommentStyle.hashLineComment,
      HeaderFileType("rst") -> dotCommentStyle,
      HeaderFileType("py") -> HeaderCommentStyle.hashLineComment,
    ),
  )

  // In community-subprojects, we use the Daml repo's license header so that we can more easily share the same
  // files in the open-source Daml repo for consistency, and also to refer to the Apache 2.0 license in place of the
  // "Proprietary code." reference that we maintain in the enterprise edition and closed-source canton repo.
  lazy val damlRepoHeaderSettings = Seq(
    headerLicense := Some(
      HeaderLicense
        .Custom( // When updating the year here, also update .circleci/enterpriseAppHeaderCheck.sh and cantonRepoHeaderSettings above
          """|Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    new LineCommentCreator(s"..${System.lineSeparator()}  "),
    HeaderPattern.commentStartingWith(s"..${System.lineSeparator()}  "),
  )

  lazy val wartsDisabledWithSystemProperty = System.getProperty("canton-disable-warts") == "true"

  def unlessWartsAreDisabledWithSystemProperty[A](a: Seq[A]): Seq[A] =
    if (wartsDisabledWithSystemProperty) Seq.empty else a

  def unlessWartsAreDisabledWithSystemProperty[A](a: A, as: A*): Seq[A] =
    if (wartsDisabledWithSystemProperty) Seq.empty else a +: as

  lazy val scalaOptionsForCompileScope: Def.Initialize[Seq[String]] =
    onScalaVersion(
      shared = unlessWartsAreDisabledWithSystemProperty(
        "-feature",
        "-unchecked",
        "-deprecation",
        "-Xmacro-settings:materialize-derivations",
        "-Xfatal-warnings",
        "-Wnonunit-statement", // Warns about any interesting expression whose value is ignored because it is followed by another expression
      ),
      scala213 = unlessWartsAreDisabledWithSystemProperty(
        "-Xlint:_,-unused",
        "-Wconf:cat=unused-imports:info", // reports unused-imports without counting them as warnings, and without causing -Werror to fail.
        "-Ywarn-numeric-widen",
        "-Vimplicits",
        "-Vtype-diffs",
        "-Xsource:3-cross",
      ),
      scala3 = Seq("-Ykind-projector"),
    )

  lazy val unusedSymbolsChecks: Def.Initialize[Seq[String]] =
    if (enableUnusedSymbolsChecks)
      onScalaVersion(
        shared = Seq(
          // Gives a warning for functions declared as returning Unit, but the body returns a value
          "-Wvalue-discard",
          "-Wunused:imports",
          // Not enabled patvars
          "-Wunused:privates",
          "-Wunused:locals",
          "-Wunused:params",
          "-Wunused:nowarn",
        ),
        scala213 = Seq("-Wdead-code"),
      )
    else Def.setting(Seq.empty)

  lazy val scalacOptionsToDisableForTests = Seq(
    "-Wvalue-discard",
    "-Wnonunit-statement",
  ) // disable value discard and non-unit statement checks on tests

  private val wartsCompileScopeExclusions = Seq(
    Wart.Any,
    Wart.ArrayEquals,
    Wart.AutoUnboxing,
    Wart.CaseClassPrivateApply,
    Wart.Equals,
    Wart.DefaultArguments,
    Wart.ExplicitImplicitTypes,
    Wart.FinalVal,
    Wart.ForeachEntry,
    Wart.ImplicitConversion,
    Wart.ImplicitParameter,
    Wart.JavaSerializable,
    Wart.LeakingSealed,
    Wart.ListAppend,
    Wart.ListUnapply,
    Wart.MutableDataStructures,
    Wart.NoNeedImport,
    Wart.NonUnitStatements,
    Wart.Nothing,
    Wart.Overloading,
    Wart.PlatformDefault,
    Wart.PublicInference,
    Wart.Recursion,
    Wart.RedundantConversions,
    Wart.ScalaApp,
    Wart.SeqApply,
    Wart.SeqUpdated,
    Wart.StringPlusAny,
    Wart.ThreadSleep,
    Wart.Throw,
    Wart.ToString,
    Wart.TripleQuestionMark,
  )

  lazy val wartremoverErrorsForCompileScope: Seq[Wart] = {
    val allWarts =
      Warts.allBut(wartsCompileScopeExclusions*) ++ Seq(
        ContribWart.UnintendedLaziness
      )

    unlessWartsAreDisabledWithSystemProperty(allWarts)
  }

  val wartremoverErrorsForTestScope: Seq[Wart] = {
    val exclusions = wartsCompileScopeExclusions ++ Seq(
      Wart.AnyVal,
      Wart.AsInstanceOf,
      Wart.GlobalExecutionContext,
      Wart.IsInstanceOf,
      Wart.IterableOps,
      Wart.ListUnapplySeq,
      Wart.ObjectThrowable,
      Wart.Null,
      Wart.Var,
    )

    val allWarts = Warts.allBut(exclusions*) ++ Seq(ContribWart.UnintendedLaziness)

    unlessWartsAreDisabledWithSystemProperty(allWarts)
  }

  override def projectSettings =
    Seq(
      javacOptions ++= Seq("-encoding", "UTF-8"),
      Compile / compile / javacOptions += "-Werror", // only for compile task, but not for doc task
      scalacOptions ++= Seq("-encoding", "UTF-8", "-language:postfixOps"),
      scalacOptions ++= scalaOptionsForCompileScope.value ++ unusedSymbolsChecks.value,
      Test / scalacOptions --= scalacOptionsToDisableForTests,
      libraryDependencies ++= onScalaVersion(
        scala213 = Seq(
          compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.3").cross(CrossVersion.full))
        )
      ).value,
      Compile / compile / wartremoverErrors ++= wartremoverErrorsForCompileScope,
      Test / compile / wartremoverErrors := wartremoverErrorsForTestScope,
      // Disable wart checks on generated code
      wartremoverExcluded ++= Seq((Compile / sourceManaged).value, (Test / sourceManaged).value),
      // licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
      //
      // allow sbt to pull scaladoc from managed dependencies if referenced in our ScalaDoc links
      autoAPIMappings := true,
      //
      // 'slowpoke'/notification message if tests run for more than 2mins, repeat at 30s intervals from there
      Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "120", "30"),
      //
      // CHP: Disable output for successful tests
      // G: But after finishing tests for a module, output summary of failed tests for that module, with full stack traces
      // F: Show full stack traces for exceptions in tests (at the time when they occur)
      // K: Do not print TestCanceledException at the end (in reminder) - they are still printed once due to F flag above F
      Test / testOptions += Tests.Argument("-oCHPGFK"),
      //
      // to allow notifications and alerts during test runs (like slowpokes^)
      Test / logBuffered := false,
    ) ++ cantonRepoHeaderSettings
}
