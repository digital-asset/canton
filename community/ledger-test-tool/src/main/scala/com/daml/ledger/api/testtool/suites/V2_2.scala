// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.TestDars
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.runner.AvailableTests
import com.daml.ledger.api.testtool.suites.v2_2.*
import com.daml.ledger.api.testtool.suites.v2_2.objectmeta.{
  PartyManagementServiceObjectMetaIT,
  UserManagementServiceObjectMetaIT,
}
import com.daml.tls.TlsClientConfig

class V2_2(override val testDars: TestDars) extends AvailableTests {
  override def defaultTests(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT(testDars),
      new CheckpointInTailingStreamsIT(testDars),
      new CommandDeduplicationIT(timeoutScaleFactor),
      new CommandDeduplicationParallelIT,
      new CommandDeduplicationPeriodValidationIT(testDars),
      new CommandServiceIT(testDars),
      new GetCompletionsIT,
      new CommandSubmissionCompletionIT(testDars),
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandService),
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandSubmissionService),
      new ContractIdIT,
      new DamlValuesIT,
      new DeeplyNestedValueIT,
      new DivulgenceIT(testDars),
      new EventQueryServiceIT(testDars),
      new ExplicitDisclosureIT(testDars),
      new HealthServiceIT,
      new IdentityProviderConfigServiceIT,
      new InteractiveSubmissionServiceIT(testDars),
      new InterfaceIT(testDars),
      new InterfaceSubscriptionsIT(testDars),
      new InterfaceSubscriptionsWithEventBlobsIT(testDars),
      new LimitsIT,
      new MultiPartySubmissionIT,
      new PackageManagementServiceIT(testDars),
      new PackageServiceIT,
      new ParticipantPruningIT(testDars),
      new PartyManagementServiceIT(testDars),
      new ExternalPartyManagementServiceIT,
      new PartyManagementServiceObjectMetaIT,
      new PartyManagementServiceUpdateRpcIT,
      new SemanticTests(testDars),
      new StateServiceIT,
      new TimeServiceIT,
      new TransactionServiceArgumentsIT(testDars),
      new TransactionServiceAuthorizationIT(testDars),
      new TransactionServiceCorrectnessIT(testDars),
      new TransactionServiceExerciseIT(testDars),
      new TransactionServiceFiltersIT(testDars),
      new TransactionServiceOutputsIT(testDars),
      new UpdateServiceQueryIT(testDars),
      new TransactionServiceStakeholdersIT(testDars),
      new TransactionServiceValidationIT(testDars),
      new TransactionServiceVisibilityIT(testDars),
      new UpdateServiceStreamsIT(testDars),
      new UpdateServiceTopologyEventsIT,
      new UpgradingIT(testDars),
      new UserManagementServiceIT,
      new UserManagementServiceObjectMetaIT,
      new UserManagementServiceUpdateRpcIT,
      new ValueLimitsIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT(testDars),
      new VettingIT(testDars),
      new ContractServiceIT(testDars),
      new StateServiceGetLedgerEndIT,
    )

  override def optionalTests(tlsConfiguration: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    Vector(
      new TLSOnePointThreeIT(tlsConfiguration),
      new TLSAtLeastOnePointTwoIT(tlsConfiguration),
    )
}
