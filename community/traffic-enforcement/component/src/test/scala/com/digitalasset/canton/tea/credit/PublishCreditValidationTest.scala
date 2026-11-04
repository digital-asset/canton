// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.credit

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tea.daml.java.canton.traffic.credit.TrafficCreditIssuer
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  Identifier,
  PackageId,
  ParticipantId,
  Party,
  QualifiedName,
}
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig, Result, Error as EngineError}
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value.{
  ValueInt64,
  ValueParty,
  ValueRecord,
  ValueText,
  ValueVariant,
}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalacheck.{Gen, Shrink}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.regex.Pattern
import java.util.zip.ZipInputStream

/** Checks the validation of `Canton.Traffic.Credit`: the `AccountId` spec, both in `mkAccountId`
  * and when exercising `PublishCredit` with an unvalidated account id, and the positive
  * `trafficAmount` of `PublishCredit`.
  *
  * Runs against the pinned canton-traffic-credit DAR in `src/main/resources/dar`, not the current
  * Daml source.
  */
class PublishCreditValidationTest extends AnyWordSpec with BaseTest with ScalaCheckPropertyChecks {

  import AccountIdGenerators.*

  /** The spec from the `AccountId` doc comment. `^` and `$` are dropped as `matches()` anchors the
    * whole input, and Java's `$` also matches before a trailing newline.
    */
  private val specPattern =
    Pattern.compile("[A-Za-z0-9@^$.!`\\-#+'~_|:() ]+(?:/[A-Za-z0-9@^$.!`\\-#+'~_|:() ]+)*")

  private def isValidPerSpec(s: String): Boolean =
    s.codePointCount(0, s.length) <= MaxLength && specPattern.matcher(s).matches()

  private val testDarResource = "canton-traffic-credit-test-1.0.0.dar"
  private val testDar = DarDecoder.assertReadArchive(
    testDarResource,
    new ZipInputStream(getClass.getClassLoader.getResourceAsStream(testDarResource)),
  )
  private val testPackages = testDar.all.toMap
  private val (testPackageId, testPackage) = testDar.main
  private val issuerTemplate = Identifier(
    testPackageId,
    QualifiedName.assertFromString("Canton.Traffic.CreditTest:TestCreditIssuer"),
  )
  private val engine =
    new Engine(EngineConfig(allowedLanguageVersions = LanguageVersion.allLfVersions), loggerFactory)
  private val paymaster = Party.assertFromString("paymaster")

  /** Creates a `TestCreditIssuer` and exercises `choice` on it, returning the exercise result. */
  private def createAndExercise(choice: String, argument: Value): Either[EngineError, Value] =
    engine
      .submit(
        // Only the test package: the pinned DAR comes with an older daml-stdlib of the same name.
        packageMap =
          Map(testPackageId -> (testPackage.metadata.name -> testPackage.metadata.version)),
        packagePreference = Set(testPackageId),
        submitters = Set(paymaster),
        cmds = ApiCommands(
          ImmArray(
            ApiCommand.CreateAndExercise(
              issuerTemplate.toRef,
              ValueRecord(None, ImmArray(None -> ValueParty(paymaster))),
              ChoiceName.assertFromString(choice),
              argument,
            )
          ),
          Time.Timestamp.Epoch,
          choice,
        ),
        participantId = ParticipantId.assertFromString("participant"),
        submissionSeed = Hash.hashPrivateKey(argument.toString),
        contractIdVersion = ContractIdVersion.V1,
        interpretationConfig = InterpretationConfig.DefaultForTesting,
        prefetchKeys = Seq.empty,
      )
      .consume(Result.lookupHandler(pkgs = testPackages))
      .map { case (tx, _) =>
        tx.roots.toSeq
          .map(tx.nodes)
          .collectFirst { case exercise: Node.Exercise => exercise.exerciseResult }
          .flatten
          .getOrElse(fail(s"No exercise result in $tx"))
      }

  /** Evaluates `mkAccountId value`, returning the error message or the account id's value. */
  private def mkAccountId(value: String): Either[String, String] =
    createAndExercise(
      "TestCreditIssuer_MkAccountId",
      ValueRecord(None, ImmArray(None -> ValueText(value))),
    ) match {
      case Right(ValueVariant(_, constructor, ValueText(error))) if constructor == "Left" =>
        Left(error)
      case Right(ValueVariant(_, constructor, ValueRecord(_, ImmArray((_, ValueText(accountId))))))
          if constructor == "Right" =>
        Right(accountId)
      case other => fail(s"Unexpected result of mkAccountId: $other")
    }

  /** Exercises `PublishCredit` with `accountId` as given, bypassing `mkAccountId`. */
  private def publishCredit(accountId: String, trafficAmount: Long): Either[EngineError, Unit] =
    createAndExercise(
      "TestCreditIssuer_PublishCredit",
      ValueRecord(
        None,
        ImmArray(
          None -> ValueRecord(None, ImmArray(None -> ValueText(accountId))),
          None -> ValueInt64(trafficAmount),
        ),
      ),
    ).map(_ => ())

  private val trafficAmount: Gen[Long] = Gen.posNum[Long]
  // Daml `Int` is 64 bit, so any `Long` can be submitted
  private val anyTrafficAmount: Gen[Long] = Gen.frequency(
    1 -> Gen.oneOf(0L, 1L, -1L, Long.MaxValue, Long.MinValue),
    3 -> Gen.choose(Long.MinValue, Long.MaxValue),
  )
  // Shrinking towards 0 could turn a valid amount into an invalid one
  private implicit val trafficAmountShrink: Shrink[Long] = Shrink.shrinkAny
  private implicit val stringShrink: Shrink[String] = shrinkByRemovingCodePoints

  // Tests of a single class of account ids (including valid ones) disable shrinking by shadowing
  // `stringShrink`, as shrinking could turn a generated invalid id into a valid one and vice versa.

  "the test DAR" should {
    "contain the pinned canton-traffic-credit package" in {
      testPackages.keySet should contain(PackageId.assertFromString(TrafficCreditIssuer.PACKAGE_ID))
    }
  }

  "mkAccountId" should {
    "accept exactly the account ids allowed by the spec" in {
      forAll(anyAccountId, minSuccessful(500)) { s =>
        mkAccountId(s).isRight shouldBe isValidPerSpec(s)
      }
    }

    "keep the value of valid account ids" in {
      implicit val stringShrink: Shrink[String] = Shrink.shrinkAny
      forAll(validAccountId, minSuccessful(200))(s => mkAccountId(s) shouldBe Right(s))
    }

    "reject account ids with a forbidden character" in {
      implicit val stringShrink: Shrink[String] = Shrink.shrinkAny
      forAll(accountIdWithForbiddenChar, minSuccessful(200)) { s =>
        mkAccountId(s).left.value should include("must only contain")
      }
    }

    "reject account ids with an empty segment" in {
      implicit val stringShrink: Shrink[String] = Shrink.shrinkAny
      forAll(accountIdWithEmptySegment, minSuccessful(200)) { s =>
        mkAccountId(s).left.value should include("must not be empty")
      }
    }

    "reject account ids longer than 255 characters" in {
      implicit val stringShrink: Shrink[String] = Shrink.shrinkAny
      forAll(tooLongAccountId, minSuccessful(200)) { s =>
        mkAccountId(s).left.value should include("must be at most 255 characters")
      }
    }
  }

  "PublishCredit" should {
    "accept exactly the account ids allowed by the spec" in {
      forAll(anyAccountId, trafficAmount, minSuccessful(500)) { (s, amount) =>
        publishCredit(s, amount).isRight shouldBe isValidPerSpec(s)
      }
    }

    "fail with the error of mkAccountId" in {
      forAll(anyAccountId, trafficAmount, minSuccessful(200)) { (s, amount) =>
        whenever(!isValidPerSpec(s)) {
          publishCredit(s, amount).left.value.message should include(mkAccountId(s).left.value)
        }
      }
    }

    "accept exactly the positive amounts" in {
      implicit val stringShrink: Shrink[String] = Shrink.shrinkAny
      forAll(validAccountId, anyTrafficAmount, minSuccessful(500)) { (s, amount) =>
        publishCredit(s, amount).isRight shouldBe amount > 0
      }
    }

    "reject non-positive amounts with the amount error" in {
      implicit val stringShrink: Shrink[String] = Shrink.shrinkAny
      forAll(validAccountId, anyTrafficAmount, minSuccessful(200)) { (s, amount) =>
        whenever(amount <= 0) {
          publishCredit(s, amount).left.value.message should include(
            "trafficAmount must be positive"
          )
        }
      }
    }

    "check the account id before the amount" in {
      forAll(anyAccountId, anyTrafficAmount, minSuccessful(200)) { (s, amount) =>
        whenever(!isValidPerSpec(s) && amount <= 0) {
          publishCredit(s, amount).left.value.message should include(mkAccountId(s).left.value)
        }
      }
    }
  }
}
