// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.annotations.NuckTest
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetup,
  SharedEnvironment,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion
import io.circe.*
import io.circe.parser.*
import monocle.macros.syntax.lens.*
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap}

import java.nio.file.*
import scala.collection.immutable.SortedMap

abstract class DamlScriptIT
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfterAllConfigMap {
  self: EnvironmentSetup =>

  // Resolve the conflict between BeforeAndAfterAll and BeforeAndAfterAllConfigMap
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = true

  import DamlScriptIT.*

  protected val debug = false

  protected def projectName: String

  protected var damlProjectDir: Path = _
  var env: Seq[(String, String)] = _

  final override def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap)
    def getEnv(name: String, default: String) =
      configMap.getOptional[String](name) match {
        case Some(value) =>
          // on CI we should get the value from the configMap
          value
        case None =>
          logger.warn(s"Using default value for $name: $default.")
          default
      }
    env = Seq(
      "DAML_VERSION" -> getEnv("damlVersion", BuildInfo.damlLibrariesVersion),
      "DPM_REGISTRY" -> getEnv("dpmRegistry", "europe-docker.pkg.dev/da-images/public-unstable"),
    )

    Option(getClass.getResource(s"/daml/$projectName")) match {
      case Some(in) =>
        damlProjectDir = Files.createTempDirectory(s"test_${getClass.getSimpleName}_")
        FileUtils.copyDirectory(Paths.get(in.toURI).toFile, damlProjectDir.toFile)
        // compile the project
        val _ = run(cmd = List("dpm", "build", "--output", projectName + ".dar"))
      case None =>
        throw new java.lang.Error("could not find daml project in resources: " + projectName)
    }
  }

  final override def afterAll(configMap: ConfigMap): Unit = {
    super.afterAll(configMap)
    if (!debug)
      FileUtils.deleteDirectory(damlProjectDir.toFile)
  }

  final def scriptError(
      cmd: List[String],
      stdout: String,
      stderr: String,
      cause: String,
  ): Nothing = {
    Console.err.println(
      s"""running command failed:
         |  command: ${cmd.mkString(" ")}
         |  cause: $cause
         |  cwd: $damlProjectDir (switch DamlScriptId.debug to true to keep this temporary directory)
         |  DAML_VERSION=${sys.env.getOrElse("DAML_VERSION", "<not set>")}
         |  DPM_REGISTRY=${sys.env.getOrElse("DPM_REGISTRY", "<not set>")}
         |  stdout: $stdout
         |  stderr: $stderr
         |""".stripMargin
    )
    throw new java.lang.Error(s"command failed: $cause")
  }

  private def run(
      cmd: List[String],
      ignoreExitCode: Boolean = false,
  ): (String, String) = {
    val stderr = new StringBuilder
    val stdout = new StringBuilder
    val logger =
      sys.process.ProcessLogger(stdout.append(_).append("\n"), stderr.append(_).append("\n"))

    val exitCode =
      try
        sys.process.Process(
          cmd,
          cwd = Some(damlProjectDir.toFile),
          env*
        ) ! logger
      catch {
        case scala.util.control.NonFatal(cause) =>
          scriptError(cmd, stdout.result(), stderr.result(), cause.getMessage)
      }
    if (ignoreExitCode || exitCode == 0)
      (stdout.result(), stderr.result())
    else
      scriptError(cmd, stdout.result(), stderr.result(), s"exitCode = $exitCode")
  }

  def runScript(
      host: String,
      port: Port,
      skippedTests: List[String],
  ): Map[String, Either[String, Json]] = {
    val outputFile = Files.createTempFile(damlProjectDir, projectName, ".json")
    val cmd = List(
      List("dpm", "script"),
      List("--dar", projectName + ".dar"),
      List("--all"),
      skippedTests.flatMap(List("--skip-script-name", _)),
      List("--ledger-host", host),
      List("--ledger-port", port.unwrap.toString),
      List("--static-time"),
      List("--max-inbound-message-size", Int.MaxValue.toString),
      List("--upload-dar", "true"),
      List("--json-test-summary", outputFile.toString),
    ).flatten
    val (stdout, stderr) = run(cmd, ignoreExitCode = true)
    val resultOrErr = for {
      output <- scala.util.Try(Files.readString(outputFile)).toEither
      json <- parse(output)
      result <- json.as[Map[String, Either[String, Json]]]
    } yield result
    resultOrErr match {
      case Right(value) =>
        value
      case Left(err) =>
        scriptError(
          cmd,
          stdout,
          stderr,
          s"failed to parse script output: ${err.getMessage}",
        )
    }
  }

  protected def uckMode: Boolean

  protected def langVersion: LanguageVersion

  protected def enableLfDev: Boolean =
    langVersion == com.digitalasset.daml.lf.language.LanguageVersion.devLfVersion

  // Skip test if protocol version is smaller
  protected lazy val minimumProtocolVersion: ProtocolVersion = ProtocolVersion.minimum

  private lazy val lowerCommandTrackerDuration: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_ {
      _.focus(_.ledgerApi.commandService.defaultTrackingTimeout)
        .replace(config.NonNegativeFiniteDuration.ofSeconds(15))
    }

  private lazy val useTestingTimeService: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService)

  private lazy val maybeEnableLfDev: Seq[ConfigTransform] =
    if (enableLfDev)
      ConfigTransforms.enableAlphaVersionSupport
    else Nil

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(lowerCommandTrackerDuration)
      .addConfigTransforms(useTestingTimeService)
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransforms(maybeEnableLfDev*)
      .withSetup { env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  protected def expectedResults: Map[String, ExpectedResult] = Map.empty

  def checkForSuccess(
      scriptId: String,
      expectedValueOpt: Option[Json],
      actualResultOpt: Option[Either[String, Json]],
  ): Option[String] =
    actualResultOpt match {
      case Some(actualValue) =>
        expectedValueOpt match {
          case Some(expectedValue) if expectedValue != actualValue =>
            Some(
              s"script $scriptId was expected to succeed with value $expectedValue but it succeeded with a different value: $actualValue"
            )
          case _ =>
            None
        }
      case None =>
        Some(s"script $scriptId was expected to succeed but it was not found in the results")
    }

  def checkForFailure(
      scriptId: String,
      errorMsgPattern: String,
      actualResultOpt: Option[Either[String, Json]],
  ): Option[String] =
    actualResultOpt match {
      case Some(Left(actualErrorMsg)) =>
        if (errorMsgPattern.r.findFirstIn(actualErrorMsg).isEmpty)
          Some(
            s"script $scriptId was expected to fail with message $errorMsgPattern but it failed with a different message: $actualErrorMsg"
          )
        else
          None
      case Some(Right(_)) =>
        Some(
          s"script $scriptId was expected to fail with message $errorMsgPattern but it succeeded"
        )
      case None =>
        Some(s"script $scriptId was expected to fail but it was not found in the results")
    }

  s"daml-script should produce expected results" onlyRunWithOrGreaterThan minimumProtocolVersion in {
    env =>
      import env.participant1

      val actualResults = runScript(
        host = participant1.config.ledgerApi.address,
        port = participant1.config.ledgerApi.port,
        skippedTests = expectedResults.collect { case (id, ExpectedResult.Ignored) => id }.toList,
      )

      //  In case you are missing expected results when adding new test file, you can uncomment the following code to
      //  print the missing expected results based on the actual results.
      //  Make sure to replace the placeholder string with the proper ExpectedResult (Success, Failure or Broken) and
      //  its parameters.
      //
      //    println(
      //      actualResults
      //        .collect {
      //          case (id, Right(_)) if !expectedResults.isDefinedAt(id) =>
      //            s"\"$id\" -> Failure("replace me with proper ExpectedResult")"
      //        }
      //        .mkString("Map(\n", ",\n", ")")
      //    )

      val unexpected = actualResults.keys.flatMap(scriptId =>
        expectedResults.get(scriptId) match {
          case Some(
                _: ExpectedResult.Success | _: ExpectedResult.Failure | _: ExpectedResult.Broken
              ) =>
            List.empty
          case _ =>
            List(scriptId)
        }
      )

      if (unexpected.nonEmpty)
        fail(s"got results for unexpected script ids: ${unexpected.mkString("\n  ", ",\n  ", ",")}")

      forEvery(expectedResults) { case (scriptId, expectedResult) =>
        val actualResult = actualResults.get(scriptId)
        expectedResult match {
          case ExpectedResult.Ignored =>
            actualResult match {
              case None =>
                if (debug) println("scriptId is ignored")
                succeed
              case Some(_) =>
                fail(s"script $scriptId was expected to be skipped but it was executed")
            }
          case ExpectedResult.Success(expectedValueOpt) =>
            checkForSuccess(scriptId, expectedValueOpt, actualResult) match {
              case Some(error) => fail(error)
              case None =>
                if (debug) println(s"script $scriptId succeeds")
                succeed
            }
          case ExpectedResult.Failure(errorMsgPattern) =>
            checkForFailure(scriptId, errorMsgPattern, actualResult) match {
              case Some(error) => fail(error)
              case None =>
                if (debug) println(s"script $scriptId fails")
                succeed
            }
          case ExpectedResult.Broken(expected) =>
            val assessment = expected match {
              case Left(ExpectedResult.Failure(errorMsgPattern)) =>
                checkForFailure(scriptId, errorMsgPattern, actualResult)
              case Right(ExpectedResult.Success(expectedValueOpt)) =>
                checkForSuccess(scriptId, expectedValueOpt, actualResult)
            }
            assessment match {
              case Some(error) =>
                if (debug) println(s"script $scriptId is broken")
                succeed
              case None =>
                fail(s"script $scriptId was expected to be broken but it succeeded")
            }
        }
      }
  }
}

// TODO(#16458) This should be a stable protocol version
//  Split the tests into a stable and a dev suite and run the dev suite only with the dev protocol version
abstract class DamlScriptDevIT extends DamlScriptIT {

  import DamlScriptIT.ExpectedResult.*

  override lazy val uckMode = true
  override lazy val langVersion = LanguageVersion.v2_dev
  override lazy val projectName = "ScriptDevTests"
  override lazy val minimumProtocolVersion = ProtocolVersion.dev

  override val expectedResults = SortedMap(
    "ActionTest:testFilterA" -> Success(),
    "AuthEvalOrder:t1_create_success" -> Failure("t1 finished with no authorization failure"),
    "AuthEvalOrder:t2_create_badlyAuthorized" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthEvalOrder:t3_createViaExerice_success" -> Failure(
      "t3 finished with no authorization failure"
    ),
    "AuthEvalOrder:t4_createViaExerice_badlyAuthorized" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthFailure:t1_CreateMissingAuthorization" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthFailure:t2_MaintainersNotSubsetOfSignatories" -> Failure(
      "has maintainers .* which are not a subset of the signatories"
    ),
    "AuthFailure:t3_FetchMissingAuthorization" -> Failure(
      "requires one of the stakeholders .* of the fetched contract to be an authorizer"
    ),
    "AuthFailure:t4_LookupByKeyMissingAuthorization" -> Failure(
      "requires authorizers .* for lookup by key"
    ),
    "AuthFailure:t5_ExerciseMissingAuthorization" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthorizedDivulgence:test_authorizedFetch" -> Success(),
    "AuthorizedDivulgence:test_divulgeChoiceTargetContractId" -> Success(),
    "AuthorizedDivulgence:test_noDivulgenceForFetch" -> Success(),
    "AuthorizedDivulgence:test_noDivulgenceOfCreateArguments" -> Success(),
    "BasicTests:test_createAndFetch" -> Success(),
    "BasicTests:test_doubleLetTest" -> Success(),
    "BasicTests:test_exponentiation" -> Success(),
    "BasicTests:test_failedAuths" -> Success(),
    "BasicTests:test_getTimeTest" -> Success(),
    "BasicTests:test_letTest" -> Success(),
    "BasicTests:test_listMatchTest" -> Success(),
    "BasicTests:test_mustFails" -> Success(),
    "BasicTests:test_payoutTest" -> Success(),
    "BasicTests:test_screateAndExercise" -> Success(),
    "BasicTests:test_screateAndExerciseComposit" -> Success(),
    "BasicTests:test_sgetTimeTest" -> Success(),
    "BasicTests:test_testXyzTest" -> Success(),
    "BasicTests:test_typeWithParameters" -> Success(),
    "ChoiceShadowing:test1" -> Success(),
    "CoerceContractId:test" -> Success(),
    "Conjunction:main" -> Success(),
    "ConjunctionChoices:demo" -> Success(),
    "ConsumedContractKey:testFetchFromConsumingChoice" -> Failure(
      "Update failed due to fetch of an inactive contract"
    ),
    "ConsumedContractKey:testFetchKeyFromConsumingChoice" -> Failure(
      "dependency error: couldn't find key"
    ),
    "ConsumedContractKey:testLookupKeyFromConsumingChoice" -> Success(),
    "ConsumingTests:main" -> Success(),
    "ContractIdInContractKeySkipCheck:createCmdCrashes" -> Failure(
      "Contract IDs are not supported"
    ),
    "ContractIdInContractKeySkipCheck:createCrashes" -> Failure("Contract IDs are not supported"),
    "ContractIdInContractKeySkipCheck:exerciseCmdCrashes" -> Failure(
      "Contract IDs are not supported"
    ),
    "ContractIdInContractKeySkipCheck:exerciseCrashes" -> Failure("Contract IDs are not supported"),
    "ContractIdInContractKeySkipCheck:fetchCrashes" -> Failure("Contract IDs are not supported"),
    "ContractIdInContractKeySkipCheck:lookupCrashes" -> Failure("Contract IDs are not supported"),
    "ContractIdInContractKeySkipCheck:queryCrashes" ->
      // should have failed but it succeeds (tracked by https://github.com/digital-asset/daml/issues/17554)
      Broken(Failure("Contract IDs are not supported")),
    "ContractKeyNotEffective:fetchByKeyMustFail" -> Failure(
      "Setting time backwards is not allowed"
    ),
    "ContractKeyNotVisible:aScript" -> Failure("Couldn't see contract with key .*"),
    "ContractKeyNotVisible:blindLookup" -> Broken(
      Failure("expected unassigned key, which already exists")
    ),
    "ContractKeyNotVisible:divulgeeLookup" -> Broken(
      Failure("expected unassigned key, which already exists")
    ),
    "ContractKeyNotVisible:localFetch" -> Success(),
    "ContractKeyNotVisible:localLookup" -> Success(),
    "ContractKeys:test" -> Success(),
    "CreateAndExercise:main" -> Success(),
    "DamlScriptTrySubmit:authorizationError" -> Success(),
    "DamlScriptTrySubmit:contractKeyNotFound" -> Success(),
    "DamlScriptTrySubmit:contractNotActive" -> Failure("contractNotActive no additional info"),
    "DamlScriptTrySubmit:createEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmit:devError" -> Success(),
    "DamlScriptTrySubmit:duplicateContractKey" -> Success(),
    "DamlScriptTrySubmit:failureStatusError" -> Success(),
    "DamlScriptTrySubmit:fetchEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmit:truncatedError" -> Failure("EXPECTED_TRUNCATED_ERROR"),
    "DamlScriptTrySubmit:wronglyTypedContract" -> Success(),
    "EmptyContractKeyMaintainers:createCmdNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:createNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:fetchNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:lookupNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:queryNoMaintainer" -> Failure("Couldn't see contract with key"),
    "EqContractId:main" -> Success(),
    "ExceptionAndContractKey:testCreate" -> Success(),
    "ExceptionAndContractKey:testLookup" -> Success(),
    "ExceptionSemantics:divulgence" -> Success(),
    "ExceptionSemantics:duplicateKey" -> Ignored,
    "ExceptionSemantics:handledArithmeticError" -> Success(),
    "ExceptionSemantics:handledUserException" -> Success(),
    "ExceptionSemantics:rollbackArchive" -> Success(),
    "ExceptionSemantics:tryContext" -> Failure("Contract could not be found"),
    "ExceptionSemantics:uncaughtArithmeticError" -> Success(),
    "ExceptionSemantics:uncaughtUserException" -> Success(),
    "ExceptionSemantics:unhandledArithmeticError" -> Failure(
      "UNHANDLED_EXCEPTION/DA.Exception.ArithmeticError:ArithmeticError"
    ),
    "ExceptionSemantics:unhandledUserException" -> Failure(
      "UNHANDLED_EXCEPTION/ExceptionSemantics:E"
    ),
    "FailedFetch:fetchNonStakeholder" -> Failure("CONTRACT_NOT_FOUND"),
    "FetchByKey:failLedger" -> Failure("couldn't find key"),
    "FetchByKey:failSpeedy" -> Failure("couldn't find key"),
    "FetchByKey:mustFail" -> Success(),
    "Interface:main" -> Success(),
    "InterfaceArchive:main" -> Success(),
    "Iou12:main" -> Success(),
    "KeyNotVisibleStakeholders:blindFetch" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "KeyNotVisibleStakeholders:blindLookup" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "KeyNotVisibleStakeholders:divulgeeFetch" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "KeyNotVisibleStakeholders:divulgeeLookup" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "LFContractKeys:lookupTest" -> Ignored,
    "LargeTransaction:largeListAsAChoiceArgTest" -> Success(),
    "LargeTransaction:largeTransactionWithManyContractsTest" -> Success(),
    "LargeTransaction:largeTransactionWithOneContractTest" -> Success(),
    "LargeTransaction:listSizeTest" -> Success(),
    "LargeTransaction:rangeOfIntsToListContainerTest" -> Success(),
    "LargeTransaction:rangeOfIntsToListTest" -> Success(),
    "LargeTransaction:rangeTest" -> Success(),
    "LedgerTestException:test" -> Failure("ohno"),
    "LfInterfaces:run" -> Success(),
    "LfStableContractKeyThroughExercises:run" -> Ignored,
    "LfStableContractKeys:run" -> Ignored,
    "MoreChoiceObserverDivulgence:test" -> Success(),
    "Self2:main" -> Success(),
    "Self:main" -> Success(),
    "TransientFailure:testBio" -> Failure("FAILED_PRECONDITION"),
  )
}

@NuckTest
class DamlScriptDevReferenceIT extends DamlScriptDevIT {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

object DamlScriptIT {

  sealed abstract class ExpectedResult extends Product with Serializable

  object ExpectedResult {
    final case class Success(value: Option[Json] = None) extends ExpectedResult

    final case class Failure(errorMsgPattern: String) extends ExpectedResult

    final case class Broken(result: Either[Failure, Success]) extends ExpectedResult

    object Broken {
      def apply(success: Success): Broken = Broken(Right(success))
      def apply(failure: Failure): Broken = Broken(Left(failure))
    }

    final case object Ignored extends ExpectedResult
  }

  private implicit val decodeResult: Decoder[Either[String, Json]] = Decoder.instance { c =>
    c.downField("error").as[String].map(Left(_)) orElse c.downField("result").as[Json].map(Right(_))
  }
}
