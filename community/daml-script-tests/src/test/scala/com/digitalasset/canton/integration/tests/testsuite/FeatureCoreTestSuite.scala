// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests
package testsuite

import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

/** These are tests that we always expect to be true, regardless of the language version or protocol
  * version specified.
  */
object FeatureCoreTestSuite extends DamlScriptTestSuite {
  import DamlScriptTestSuiteFramework.ExpectedResult.*

  override val projectName = "FeatureCoreTests"

  override def isEnabledFor(
      protocolVersion: ProtocolVersion,
      languageVersion: LanguageVersion,
  ): Boolean =
    true

  override val expectedResults = Map(
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
    "AuthFailure:t3_FetchMissingAuthorization" -> Failure(
      "requires one of the stakeholders .* of the fetched contract to be an authorizer"
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
    "ConsumingTests:main" -> Success(),
    "CreateAndExercise:main" -> Success(),
    "DamlScriptTrySubmit:authorizationError" -> Success(),
    "DamlScriptTrySubmit:failureStatusError" -> Success(),
    "DamlScriptTrySubmit:wronglyTypedContract" -> Success(),
    "EqContractId:main" -> Success(),
    "ExceptionSemantics:handledArithmeticError" -> Success(),
    "ExceptionSemantics:handledUserException" -> Success(),
    "ExceptionSemantics:uncaughtArithmeticError" -> Success(),
    "ExceptionSemantics:uncaughtUserException" -> Success(),
    "ExceptionSemantics:unhandledArithmeticError" -> Failure(
      "UNHANDLED_EXCEPTION/DA.Exception.ArithmeticError:ArithmeticError"
    ),
    "ExceptionSemantics:unhandledUserException" -> Failure(
      "UNHANDLED_EXCEPTION/ExceptionSemantics:E"
    ),
    "FailedFetch:fetchNonStakeholder" -> Failure("CONTRACT_NOT_FOUND"),
    "Interface:main" -> Success(),
    "InterfaceArchive:main" -> Success(),
    "Iou12:main" -> Success(),
    "LargeTransaction:largeListAsAChoiceArgTest" -> Success(),
    "LargeTransaction:largeTransactionWithManyContractsTest" -> Success(),
    "LargeTransaction:largeTransactionWithOneContractTest" -> Success(),
    "LargeTransaction:listSizeTest" -> Success(),
    "LargeTransaction:rangeOfIntsToListContainerTest" -> Success(),
    "LargeTransaction:rangeOfIntsToListTest" -> Success(),
    "LargeTransaction:rangeTest" -> Success(),
    "LedgerTestException:test" -> Failure("ohno"),
    "LfInterfaces:run" -> Success(),
    "MoreChoiceObserverDivulgence:test" -> Success(),
    "Self2:main" -> Success(),
    "Self:main" -> Success(),
    "TransientFailure:testBio" -> Failure("FAILED_PRECONDITION"),
  )
}
