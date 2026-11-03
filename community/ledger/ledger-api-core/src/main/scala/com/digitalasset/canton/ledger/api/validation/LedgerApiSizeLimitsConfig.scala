// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.{
  AdminLapiSizeConfig,
  AllocateExternalPartyRequestSignedTransactionSizeConfig,
  AllocateExternalPartyRequestSizeConfig,
  AuthorizePartyUpdateRequestSizeConfig,
  CreateUserRequestSizeConfig,
  GenerateExternalPartyTopologyRequestSizeConfig,
  GetPartiesRequestSizeConfig,
  GrantUserRightsRequestSizeConfig,
  ObjectMetaSizeConfig,
  RevokeUserRightsRequestSizeConfig,
  UpdateIdentityProviderConfigRequestSizeConfig,
  UpdatePartyDetailsRequestSizeConfig,
  UpdateUserRequestSizeConfig,
  UpdateVettedPackagesRequestSizeConfig,
  UploadDarFileRequestSizeConfig,
  ValidateDarFileRequestSizeConfig,
  VettedPackagesChangeUnvetSizeConfig,
  VettedPackagesChangeVetSizeConfig,
}
import com.digitalasset.canton.ledger.api.sizeconfig.v2.interactive.{
  CostEstimationHintsSizeConfig,
  GetPreferredPackagesRequestSizeConfig,
  InteractiveLapiSizeConfig,
  MetadataSizeConfig,
  MetadataSubmitterInfoSizeConfig,
  PackageVettingRequirementSizeConfig,
  PartySignaturesSizeConfig,
  PrepareSubmissionRequestSizeConfig,
  PreparedTransactionSizeConfig,
  SinglePartySignaturesSizeConfig,
}
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
  * @param adminLapiSizeConfig
  *   message-specific limits for the admin Ledger API
  * @param interactiveLapiSizeConfig
  *   message-specific limits for the interactive submission service
  * @param sharedSizeConfig
  *   limits shared by several message-specific limits. A message-specific limit that is set
  *   explicitly takes precedence.
  * @param eventFormatTotalFiltersSizeLimit
  *   max number of filters of the event formats of a request, after decomposing the template and
  *   interface filters across the requested parties. A template or wildcard filter counts as 1, an
  *   interface filter as 5.
  * @param metadataInputContractBytesSizeLimit
  *   max serialized size in bytes of each `PreparedTransaction.Metadata.InputContract`. Defaults to
  *   [[SharedSizeConfig.contractBytesSizeLimit]].
  * @param metadataGlobalKeyMappingEntryBytesSizeLimit
  *   max serialized size in bytes of each `PreparedTransaction.Metadata.GlobalKeyMappingEntry`
  */
final case class LedgerApiSizeLimitsConfig(
    publicLapiSizeConfig: PublicLapiSizeConfig,
    adminLapiSizeConfig: AdminLapiSizeConfig,
    interactiveLapiSizeConfig: InteractiveLapiSizeConfig,
    private val sharedSizeConfig: SharedSizeConfig,
    eventFormatTotalFiltersSizeLimit: PositiveInt,
    metadataInputContractBytesSizeLimit: PositiveInt,
    metadataGlobalKeyMappingEntryBytesSizeLimit: PositiveInt,
)

/** Limits shared by several message-specific limits of the Ledger API
  *
  * @param completionsRequestPartiesSizeLimit
  *   shared limit of max number of parties limit of all completion requests
  * @param transactionHashBytesSizeLimit
  *   shared limit of max transaction hash size of all by-hash requests
  * @param continuationTokenBytesSizeLimit
  *   shared limit of the continuation token and page tokens of the active contracts and updates
  * @param synchronizerIdsSizeLimit
  *   shared limit of synchronizer ids of `GetLedgerEndRequest` and `TopologyStateFilter`
  * @param submissionCommandsSizeLimit
  *   max number of the commands of `Commands` and `PrepareSubmissionRequest`
  * @param submissionActAsSizeLimit
  *   shared limit of the act_as of `Commands`, `PrepareSubmissionRequest` and
  *   `PreparedTransaction.Metadata.SubmitterInfo`
  * @param submissionReadAsSizeLimit
  *   shared limit of the read_as of `Commands` and `PrepareSubmissionRequest`
  * @param submissionDisclosedContractsSizeLimit
  *   shared limit of the disclosed contracts of `Commands` and `PrepareSubmissionRequest`
  * @param submissionPackageIdSelectionPreferenceSizeLimit
  *   shared limit of the package id selection preference of `Commands` and
  *   `PrepareSubmissionRequest`
  * @param submissionPrefetchContractKeysSizeLimit
  *   shared limit of the prefetch contract keys of `Commands` and `PrepareSubmissionRequest`
  * @param argumentsBytesSizeLimit
  *   shared limit of all Daml values (arguments) of the commands: create arguments, choice
  *   arguments and contract keys
  * @param contractBytesSizeLimit
  *   shared limit of the contract provided by the client: created_event_blob of `DisclosedContract`
  *   and the serialized size of `PreparedTransaction.Metadata.InputContract`
  * @param darFileBytesSizeLimit
  *   shared limit of the DAR file of `UploadDarFileRequest` and `ValidateDarFileRequest`
  * @param vettedPackagesChangePackagesSizeLimit
  *   shared limit of the max number of packages of the vet and unvet `VettedPackagesChange`
  * @param participantUidsSizeLimit
  *   shared limit of the confirming and observing participant uids of
  *   `GenerateExternalPartyTopologyRequest`
  * @param userRightsSizeLimit
  *   shared limit of the rights of `CreateUserRequest`, `GrantUserRightsRequest` and
  *   `RevokeUserRightsRequest`
  * @param updateMaskBytesSizeLimit
  *   shared limit of the update_mask of `UpdateUserRequest`, `UpdatePartyDetailsRequest` and
  *   `UpdateIdentityProviderConfigRequest`
  * @param signaturesSizeLimit
  *   shared limit of the signatures limit of `AllocateExternalPartyRequest.SignedTransaction`,
  *   `AllocateExternalPartyRequest` (multi hash signatures), `AuthorizePartyUpdateRequest` and
  *   `SinglePartySignatures`
  * @param topologyTransactionBytesSizeLimit
  *   shared limit of the serialized topology transaction limit of
  *   `AllocateExternalPartyRequest.SignedTransaction` and `AuthorizePartyUpdateRequest`
  */
final case class SharedSizeConfig(
    completionsRequestPartiesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    transactionHashBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    continuationTokenBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    synchronizerIdsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    submissionCommandsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    submissionActAsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    submissionReadAsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    submissionDisclosedContractsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    submissionPackageIdSelectionPreferenceSizeLimit: PositiveInt = PositiveInt.MaxValue,
    submissionPrefetchContractKeysSizeLimit: PositiveInt = PositiveInt.MaxValue,
    argumentsBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    contractBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    darFileBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    vettedPackagesChangePackagesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    participantUidsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    userRightsSizeLimit: PositiveInt = PositiveInt.MaxValue,
    updateMaskBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    signaturesSizeLimit: PositiveInt = PositiveInt.MaxValue,
    topologyTransactionBytesSizeLimit: PositiveInt = PositiveInt.MaxValue,
)

object LedgerApiSizeLimitsConfig {

  def apply(): LedgerApiSizeLimitsConfig = defaultWithShared(SharedSizeConfig())

  def defaultWithShared(sharedSizeConfig: SharedSizeConfig): LedgerApiSizeLimitsConfig =
    LedgerApiSizeLimitsConfig(
      publicLapiSizeConfig = publicLapiDefaultsWithShared(sharedSizeConfig),
      adminLapiSizeConfig = adminLapiDefaultsWithShared(sharedSizeConfig),
      interactiveLapiSizeConfig = interactiveLapiDefaultsWithShared(sharedSizeConfig),
      sharedSizeConfig = sharedSizeConfig,
      eventFormatTotalFiltersSizeLimit = PositiveInt.MaxValue,
      metadataInputContractBytesSizeLimit = sharedSizeConfig.contractBytesSizeLimit,
      metadataGlobalKeyMappingEntryBytesSizeLimit = PositiveInt.MaxValue,
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
        commandsSizeLimit = shared.submissionCommandsSizeLimit,
        actAsSizeLimit = shared.submissionActAsSizeLimit,
        readAsSizeLimit = shared.submissionReadAsSizeLimit,
        disclosedContractsSizeLimit = shared.submissionDisclosedContractsSizeLimit,
        packageIdSelectionPreferenceSizeLimit =
          shared.submissionPackageIdSelectionPreferenceSizeLimit,
        prefetchContractKeysSizeLimit = shared.submissionPrefetchContractKeysSizeLimit,
      ),
      createAndExerciseCommand = CreateAndExerciseCommandSizeConfig(
        createArgumentsSizeLimit = shared.argumentsBytesSizeLimit,
        choiceArgumentSizeLimit = shared.argumentsBytesSizeLimit,
      ),
      createCommand = CreateCommandSizeConfig(
        createArgumentsSizeLimit = shared.argumentsBytesSizeLimit
      ),
      disclosedContract = DisclosedContractSizeConfig(
        createdEventBlobSizeLimit = shared.contractBytesSizeLimit
      ),
      exerciseByKeyCommand = ExerciseByKeyCommandSizeConfig(
        contractKeySizeLimit = shared.argumentsBytesSizeLimit,
        choiceArgumentSizeLimit = shared.argumentsBytesSizeLimit,
      ),
      exerciseCommand = ExerciseCommandSizeConfig(
        choiceArgumentSizeLimit = shared.argumentsBytesSizeLimit
      ),
      getContractRequest = GetContractRequestSizeConfig(
        queryingPartiesSizeLimit = PositiveInt.MaxValue
      ),
      getActiveContractsPageRequest = GetActiveContractsPageRequestSizeConfig(
        pageTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      getActiveContractsRequest = GetActiveContractsRequestSizeConfig(
        streamContinuationTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      getLedgerEndRequest = GetLedgerEndRequestSizeConfig(
        synchronizerIdSizeLimit = shared.synchronizerIdsSizeLimit
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
        pageTokenSizeLimit = shared.continuationTokenBytesSizeLimit
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
        synchronizerIdsSizeLimit = shared.synchronizerIdsSizeLimit,
      ),
      prefetchContractKey = PrefetchContractKeySizeConfig(
        contractKeySizeLimit = shared.argumentsBytesSizeLimit
      ),
      signature = SignatureSizeConfig(signatureSizeLimit = PositiveInt.MaxValue),
      signingPublicKey = SigningPublicKeySizeConfig(keyDataSizeLimit = PositiveInt.MaxValue),
    )

  private def adminLapiDefaultsWithShared(shared: SharedSizeConfig): AdminLapiSizeConfig =
    AdminLapiSizeConfig(
      allocateExternalPartyRequest = AllocateExternalPartyRequestSizeConfig(
        onboardingTransactionsSizeLimit = PositiveInt.MaxValue,
        multiHashSignaturesSizeLimit = shared.signaturesSizeLimit,
      ),
      allocateExternalPartyRequestSignedTransaction =
        AllocateExternalPartyRequestSignedTransactionSizeConfig(
          transactionSizeLimit = shared.topologyTransactionBytesSizeLimit,
          signaturesSizeLimit = shared.signaturesSizeLimit,
        ),
      authorizePartyUpdateRequest = AuthorizePartyUpdateRequestSizeConfig(
        transactionSizeLimit = shared.topologyTransactionBytesSizeLimit,
        signaturesSizeLimit = shared.signaturesSizeLimit,
      ),
      createUserRequest = CreateUserRequestSizeConfig(rightsSizeLimit = shared.userRightsSizeLimit),
      generateExternalPartyTopologyRequest = GenerateExternalPartyTopologyRequestSizeConfig(
        otherConfirmingParticipantUidsSizeLimit = shared.participantUidsSizeLimit,
        observingParticipantUidsSizeLimit = shared.participantUidsSizeLimit,
      ),
      getPartiesRequest = GetPartiesRequestSizeConfig(partiesSizeLimit = PositiveInt.MaxValue),
      grantUserRightsRequest = GrantUserRightsRequestSizeConfig(
        rightsSizeLimit = shared.userRightsSizeLimit
      ),
      objectMeta = ObjectMetaSizeConfig(annotationsSizeLimit = PositiveInt.MaxValue),
      revokeUserRightsRequest = RevokeUserRightsRequestSizeConfig(
        rightsSizeLimit = shared.userRightsSizeLimit
      ),
      updateIdentityProviderConfigRequest = UpdateIdentityProviderConfigRequestSizeConfig(
        updateMaskSizeLimit = shared.updateMaskBytesSizeLimit
      ),
      updatePartyDetailsRequest = UpdatePartyDetailsRequestSizeConfig(
        updateMaskSizeLimit = shared.updateMaskBytesSizeLimit
      ),
      updateUserRequest = UpdateUserRequestSizeConfig(
        updateMaskSizeLimit = shared.updateMaskBytesSizeLimit
      ),
      updateVettedPackagesRequest = UpdateVettedPackagesRequestSizeConfig(
        changesSizeLimit = PositiveInt.MaxValue,
        updateVettedPackagesForceFlagsSizeLimit = PositiveInt.MaxValue,
      ),
      uploadDarFileRequest = UploadDarFileRequestSizeConfig(
        darFileSizeLimit = shared.darFileBytesSizeLimit
      ),
      validateDarFileRequest = ValidateDarFileRequestSizeConfig(
        darFileSizeLimit = shared.darFileBytesSizeLimit
      ),
      vettedPackagesChangeUnvet = VettedPackagesChangeUnvetSizeConfig(
        packagesSizeLimit = shared.vettedPackagesChangePackagesSizeLimit
      ),
      vettedPackagesChangeVet = VettedPackagesChangeVetSizeConfig(
        packagesSizeLimit = shared.vettedPackagesChangePackagesSizeLimit
      ),
    )

  private def interactiveLapiDefaultsWithShared(
      shared: SharedSizeConfig
  ): InteractiveLapiSizeConfig =
    InteractiveLapiSizeConfig(
      costEstimationHints = CostEstimationHintsSizeConfig(
        expectedSignaturesSizeLimit = PositiveInt.MaxValue
      ),
      getPreferredPackagesRequest = GetPreferredPackagesRequestSizeConfig(
        packageVettingRequirementsSizeLimit = PositiveInt.MaxValue
      ),
      metadata = MetadataSizeConfig(
        inputContractsSizeLimit = PositiveInt.MaxValue,
        globalKeyMappingSizeLimit = PositiveInt.MaxValue,
      ),
      metadataSubmitterInfo = MetadataSubmitterInfoSizeConfig(
        actAsSizeLimit = shared.submissionActAsSizeLimit
      ),
      packageVettingRequirement = PackageVettingRequirementSizeConfig(
        partiesSizeLimit = PositiveInt.MaxValue
      ),
      partySignatures = PartySignaturesSizeConfig(signaturesSizeLimit = PositiveInt.MaxValue),
      prepareSubmissionRequest = PrepareSubmissionRequestSizeConfig(
        commandsSizeLimit = shared.submissionCommandsSizeLimit,
        actAsSizeLimit = shared.submissionActAsSizeLimit,
        readAsSizeLimit = shared.submissionReadAsSizeLimit,
        disclosedContractsSizeLimit = shared.submissionDisclosedContractsSizeLimit,
        packageIdSelectionPreferenceSizeLimit =
          shared.submissionPackageIdSelectionPreferenceSizeLimit,
        prefetchContractKeysSizeLimit = shared.submissionPrefetchContractKeysSizeLimit,
      ),
      preparedTransaction = PreparedTransactionSizeConfig(
        transactionSizeLimit = PositiveInt.MaxValue
      ),
      singlePartySignatures = SinglePartySignaturesSizeConfig(
        signaturesSizeLimit = shared.signaturesSizeLimit
      ),
    )
}
