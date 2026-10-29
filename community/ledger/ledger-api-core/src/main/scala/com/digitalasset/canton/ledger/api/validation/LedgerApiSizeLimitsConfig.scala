// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.{
  CommandsSizeConfig,
  CompletionStreamRequestSizeConfig,
  CreateAndExerciseCommandSizeConfig,
  CreateCommandSizeConfig,
  DisclosedContractSizeConfig,
  EventFormatSizeConfig,
  ExerciseByKeyCommandSizeConfig,
  ExerciseCommandSizeConfig,
  FiltersSizeConfig,
  GetActiveContractsPageRequestSizeConfig,
  GetActiveContractsRequestSizeConfig,
  GetCompletionByHashRequestSizeConfig,
  GetCompletionsRequestSizeConfig,
  GetContractRequestSizeConfig,
  GetLedgerEndRequestSizeConfig,
  GetUpdateByHashRequestSizeConfig,
  GetUpdatesPageRequestSizeConfig,
  PackageMetadataFilterSizeConfig,
  ParticipantAuthorizationTopologyFormatSizeConfig,
  PrefetchContractKeySizeConfig,
  PublicLapiSizeConfig,
  ReassignmentCommandsSizeConfig,
  SignatureSizeConfig,
  SigningPublicKeySizeConfig,
  TopologyStateFilterSizeConfig,
}

/** Size limits for the Ledger API requests.
  *
  * @param publicLapiSizeConfig
  *   message-specific limits for the public Ledger API
  * @param sharedSizeConfig
  *   limits shared by several message-specific limits. A message-specific limit that is set
  *   explicitly takes precedence.
  */
final case class LedgerApiSizeLimitsConfig(
    publicLapiSizeConfig: PublicLapiSizeConfig,
    private val sharedSizeConfig: SharedSizeConfig,
)

/** Limits shared by several message-specific limits of the Ledger API
  *
  * @param completionsRequestPartiesSizeLimit
  *   default of the parties limit of all completion requests
  * @param transactionHashBytesSizeLimit
  *   default of the transaction hash limit of all by-hash requests
  */
final case class SharedSizeConfig(
    completionsRequestPartiesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    transactionHashBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
)

object LedgerApiSizeLimitsConfig {

  def apply(): LedgerApiSizeLimitsConfig = defaultWithShared(SharedSizeConfig())

  def defaultWithShared(sharedSizeConfig: SharedSizeConfig): LedgerApiSizeLimitsConfig =
    LedgerApiSizeLimitsConfig(
      publicLapiSizeConfig = publicLapiDefaultsWithShared(sharedSizeConfig),
      sharedSizeConfig = sharedSizeConfig,
    )

  private def publicLapiDefaultsWithShared(shared: SharedSizeConfig): PublicLapiSizeConfig =
    PublicLapiSizeConfig(
      completionStreamRequest = CompletionStreamRequestSizeConfig(
        partiesSizeLimit = shared.completionsRequestPartiesSizeLimit
      ),
      getCompletionsRequest = GetCompletionsRequestSizeConfig(
        partiesSizeLimit = shared.completionsRequestPartiesSizeLimit
      ),
      getCompletionByHashRequest = GetCompletionByHashRequestSizeConfig(
        transactionHashSizeLimit = shared.transactionHashBytesSizeLimit
      ),
      commands = CommandsSizeConfig(
        commandsSizeLimit = PositiveInt.MaxValue,
        actAsSizeLimit = PositiveInt.MaxValue,
        readAsSizeLimit = PositiveInt.MaxValue,
        disclosedContractsSizeLimit = PositiveInt.MaxValue,
        packageIdSelectionPreferenceSizeLimit = PositiveInt.MaxValue,
        prefetchContractKeysSizeLimit = PositiveInt.MaxValue,
      ),
      createAndExerciseCommand = CreateAndExerciseCommandSizeConfig(
        createArgumentsSizeLimit = PositiveInt.MaxValue,
        choiceArgumentSizeLimit = PositiveInt.MaxValue,
      ),
      createCommand = CreateCommandSizeConfig(createArgumentsSizeLimit = PositiveInt.MaxValue),
      disclosedContract = DisclosedContractSizeConfig(
        createdEventBlobSizeLimit = PositiveInt.MaxValue
      ),
      exerciseByKeyCommand = ExerciseByKeyCommandSizeConfig(
        contractKeySizeLimit = PositiveInt.MaxValue,
        choiceArgumentSizeLimit = PositiveInt.MaxValue,
      ),
      exerciseCommand = ExerciseCommandSizeConfig(choiceArgumentSizeLimit = PositiveInt.MaxValue),
      getContractRequest = GetContractRequestSizeConfig(
        queryingPartiesSizeLimit = PositiveInt.MaxValue
      ),
      getActiveContractsPageRequest = GetActiveContractsPageRequestSizeConfig(
        pageTokenSizeLimit = PositiveInt.MaxValue
      ),
      getActiveContractsRequest = GetActiveContractsRequestSizeConfig(
        streamContinuationTokenSizeLimit = PositiveInt.MaxValue
      ),
      getLedgerEndRequest = GetLedgerEndRequestSizeConfig(
        synchronizerIdSizeLimit = PositiveInt.MaxValue
      ),
      eventFormat = EventFormatSizeConfig(filtersByPartySizeLimit = PositiveInt.MaxValue),
      filters = FiltersSizeConfig(cumulativeSizeLimit = PositiveInt.MaxValue),
      participantAuthorizationTopologyFormat = ParticipantAuthorizationTopologyFormatSizeConfig(
        partiesSizeLimit = PositiveInt.MaxValue
      ),
      getUpdateByHashRequest = GetUpdateByHashRequestSizeConfig(
        transactionHashSizeLimit = shared.transactionHashBytesSizeLimit
      ),
      getUpdatesPageRequest = GetUpdatesPageRequestSizeConfig(
        pageTokenSizeLimit = PositiveInt.MaxValue
      ),
      reassignmentCommands = ReassignmentCommandsSizeConfig(
        commandsSizeLimit = PositiveInt.MaxValue
      ),
      packageMetadataFilter = PackageMetadataFilterSizeConfig(
        packageIdsSizeLimit = PositiveInt.MaxValue,
        packageNamePrefixesSizeLimit = PositiveInt.MaxValue,
      ),
      topologyStateFilter = TopologyStateFilterSizeConfig(
        participantIdsSizeLimit = PositiveInt.MaxValue,
        synchronizerIdsSizeLimit = PositiveInt.MaxValue,
      ),
      prefetchContractKey =
        PrefetchContractKeySizeConfig(contractKeySizeLimit = PositiveInt.MaxValue),
      signature = SignatureSizeConfig(signatureSizeLimit = PositiveInt.MaxValue),
      signingPublicKey = SigningPublicKeySizeConfig(keyDataSizeLimit = PositiveInt.MaxValue),
    )
}
