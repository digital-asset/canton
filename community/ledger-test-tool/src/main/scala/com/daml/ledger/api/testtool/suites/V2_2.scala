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
      new ActiveContractsServiceIT,
      new CheckpointInTailingStreamsIT,
      new CommandDeduplicationIT(timeoutScaleFactor),
      new CommandDeduplicationParallelIT,
      new CommandDeduplicationPeriodValidationIT,
      new CommandServiceIT,
      new CommandSubmissionCompletionIT,
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandService),
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandSubmissionService),
      new ContractIdIT,
      new DamlValuesIT,
      new DeeplyNestedValueIT,
      new DivulgenceIT,
      new EventQueryServiceIT,
      new ExplicitDisclosureIT,
      new HealthServiceIT,
      new IdentityProviderConfigServiceIT,
      new InteractiveSubmissionServiceIT,
      new InterfaceIT,
      new InterfaceSubscriptionsIT(testDars),
      new InterfaceSubscriptionsWithEventBlobsIT(testDars),
      new LimitsIT,
      new MultiPartySubmissionIT,
      new PackageManagementServiceIT(testDars),
      new PackageServiceIT,
      new ParticipantPruningIT,
      new PartyManagementServiceIT,
      new ExternalPartyManagementServiceIT,
      new PartyManagementServiceObjectMetaIT,
      new PartyManagementServiceUpdateRpcIT,
      new SemanticTests,
      new StateServiceIT,
      new TimeServiceIT,
      new TransactionServiceArgumentsIT,
      new TransactionServiceAuthorizationIT,
      new TransactionServiceCorrectnessIT,
      new TransactionServiceExerciseIT,
      new TransactionServiceFiltersIT,
      new TransactionServiceOutputsIT,
      new UpdateServiceQueryIT,
      new TransactionServiceStakeholdersIT,
      new TransactionServiceValidationIT,
      new TransactionServiceVisibilityIT,
      new UpdateServiceStreamsIT(testDars),
      new UpdateServiceTopologyEventsIT,
      new UpgradingIT(testDars),
      new UserManagementServiceIT,
      new UserManagementServiceObjectMetaIT,
      new UserManagementServiceUpdateRpcIT,
      new ValueLimitsIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT,
      new VettingIT(testDars),
      new ContractServiceIT,
    )

  override def optionalTests(tlsConfiguration: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    Vector(
      new TLSOnePointThreeIT(tlsConfiguration),
      new TLSAtLeastOnePointTwoIT(tlsConfiguration),
    )
}
