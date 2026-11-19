// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests
package testsuite

import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

object FeatureContractKeyTestSuite extends DamlScriptTestSuite {
  import DamlScriptTestSuiteFramework.contractIDsNotSupported
  import DamlScriptTestSuiteFramework.ExpectedResult.*

  override val projectName = "FeatureContractKeyTests"

  override def isEnabledFor(
      protocolVersion: ProtocolVersion,
      languageVersion: LanguageVersion,
  ): Boolean =
    LanguageFeatures.featureContractKeys.enabledIn(languageVersion)

  override val expectedResults = Map(
    "AuthFailureWithKeys:t2_MaintainersNotSubsetOfSignatories" -> Failure(
      "has maintainers .* which are not a subset of the signatories"
    ),
    "AuthFailureWithKeys:t4_LookupByKeyMissingAuthorization" -> Failure(
      "requires authorizers .* for lookup by key"
    ),
    "ConsumedContractKey:testFetchFromConsumingChoice" -> Failure(
      "Update failed due to fetch of an inactive contract"
    ),
    "ConsumedContractKey:testFetchKeyFromConsumingChoice" -> Failure(
      "dependency error: couldn't find key"
    ),
    "ConsumedContractKey:testLookupKeyFromConsumingChoice" -> Success(),
    "ContractIdInContractKeySkipCheck:createCmdCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:createCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:exerciseCmdCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:exerciseCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:fetchCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:lookupCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:queryCrashes" -> contractIDsNotSupported,
    "ContractKeyNotEffective:fetchByKeyMustFail" -> Failure(
      "Setting time backwards is not allowed"
    ),
    "ContractKeyNotVisible:aScript" -> Failure("Couldn't see contract with key .*"),
    "ContractKeyNotVisible:blindLookup" -> Success(),
    "ContractKeyNotVisible:divulgeeLookup" -> Success(),
    "ContractKeyNotVisible:localFetch" -> Success(),
    "ContractKeyNotVisible:localLookup" -> Success(),
    "ContractKeys:test" -> Success(),
    "DamlScriptTrySubmitWithKeys:authorizationError" -> Success(),
    "DamlScriptTrySubmitWithKeys:contractKeyNotFound" -> Success(),
    "DamlScriptTrySubmitWithKeys:contractNotActive" -> Failure(
      "contractNotActive no additional info"
    ),
    "DamlScriptTrySubmitWithKeys:createEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmitWithKeys:duplicateContractKey" -> Failure("incorrectly succeeded"),
    "DamlScriptTrySubmitWithKeys:fetchEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmitWithKeys:truncatedError" -> Failure("EXPECTED_TRUNCATED_ERROR"),
    "DamlScriptTrySubmitWithKeys:wronglyTypedContract" -> Success(),
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
    "ExceptionAndContractKey:testCreate" -> Success(),
    "ExceptionAndContractKey:testLookup" -> Success(),
    "ExceptionSemanticsWithKeys:divulgence" -> Success(),
    "ExceptionSemanticsWithKeys:duplicateKey" -> Ignored,
    "ExceptionSemanticsWithKeys:rollbackArchive" -> Failure("DAML_EFFECTFUL_ROLLBACK_ERROR"),
    "ExceptionSemanticsWithKeys:rollbackConsumingExercise" -> Failure(
      "DAML_EFFECTFUL_ROLLBACK_ERROR"
    ),
    "ExceptionSemanticsWithKeys:rollbackCreate" -> Failure("DAML_EFFECTFUL_ROLLBACK_ERROR"),
    "ExceptionSemanticsWithKeys:tryContext" -> Failure("Contract could not be found"),
    "FetchByKey:failLedger" -> Failure("couldn't find key"),
    "FetchByKey:failSpeedy" -> Failure("couldn't find key"),
    "FetchByKey:mustFail" -> Success(),
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
    "LfStableContractKeyThroughExercises:run" -> Ignored,
    "LfStableContractKeys:run" -> Ignored,
    "NUCKTests:createMultiple" -> Success(),
    "NUCKTests:exerciseByKeyWithInvisibleButDisclosedKey" -> Success(),
    "NUCKTests:exerciseByMultiple" -> Success(),
    "NUCKTests:lookupNByKeyMultiple" -> Success(),
    "NUCKTests:queryNByKeyMultiple" -> Success(),
    "NUCKTests:queryNByKeyUnauthorized" -> Success(),
    "NUCKTests:useAllOperations" -> Success(),
  )
}
