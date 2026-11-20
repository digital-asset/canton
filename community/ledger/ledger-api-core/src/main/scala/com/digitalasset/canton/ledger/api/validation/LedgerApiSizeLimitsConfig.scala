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
  ContractIdFilterSizeConfig,
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
  GetHostedPartiesPageRequestSizeConfig,
  GetHostedPartiesRequestSizeConfig,
  GetLedgerEndRequestSizeConfig,
  GetUpdateByHashRequestSizeConfig,
  GetUpdatesPageRequestSizeConfig,
  HostedPartiesFilterSizeConfig,
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
  */
final case class LedgerApiSizeLimitsConfig(
    publicLapiSizeConfig: PublicLapiSizeConfig,
    adminLapiSizeConfig: AdminLapiSizeConfig,
    interactiveLapiSizeConfig: InteractiveLapiSizeConfig,
    private val sharedSizeConfig: SharedSizeConfig,
    eventFormatTotalFiltersSizeLimit: PositiveInt,
    metadataInputContractBytesSizeLimit: PositiveInt,
)

/** Limits shared by several message-specific limits of the Ledger API
  *
  * @param completionsRequestPartiesSizeLimit
  *   shared limit of max number of parties limit of all completion requests
  * @param transactionHashBytesSizeLimit
  *   shared limit of max transaction hash size of all by-hash requests
  * @param continuationTokenBytesSizeLimit
  *   shared limit of the continuation token and page tokens of the active contracts, updates and
  *   hosted parties
  * @param synchronizerIdsSizeLimit
  *   shared limit of synchronizer ids of `GetLedgerEndRequest`, `TopologyStateFilter` and
  *   `HostedPartiesFilter`
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
  *   `AllocateExternalPartyRequest` (multi hash signatures), `AuthorizePartyUpdateRequest`,
  *   `SinglePartySignatures` and the expected signatures of `CostEstimationHints`
  * @param topologyTransactionBytesSizeLimit
  *   shared limit of the serialized topology transaction limit of
  *   `AllocateExternalPartyRequest.SignedTransaction` and `AuthorizePartyUpdateRequest`
  */
final case class SharedSizeConfig(
    completionsRequestPartiesSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    // hashes are 32-byte raw SHA-256 digests
    transactionHashBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(100),
    continuationTokenBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(1024),
    synchronizerIdsSizeLimit: PositiveInt = PositiveInt.tryCreate(100),
    submissionCommandsSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    submissionActAsSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    submissionReadAsSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    // disclosed contracts are input contracts, so keep it <= metadata.inputContractsSizeLimit
    submissionDisclosedContractsSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    submissionPackageIdSelectionPreferenceSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    submissionPrefetchContractKeysSizeLimit: PositiveInt = PositiveInt.tryCreate(100),
    argumentsBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(10 * 1024 * 1024),
    contractBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(10 * 1024 * 1024),
    darFileBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(20 * 1024 * 1024),
    vettedPackagesChangePackagesSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    participantUidsSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    userRightsSizeLimit: PositiveInt = PositiveInt.tryCreate(10_000),
    updateMaskBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(1024),
    signaturesSizeLimit: PositiveInt = PositiveInt.tryCreate(1_000),
    topologyTransactionBytesSizeLimit: PositiveInt = PositiveInt.tryCreate(1024 * 1024),
)

object LedgerApiSizeLimitsConfig {

  def apply(): LedgerApiSizeLimitsConfig = defaultWithShared(SharedSizeConfig())

  def defaultWithShared(sharedSizeConfig: SharedSizeConfig): LedgerApiSizeLimitsConfig =
    LedgerApiSizeLimitsConfig(
      publicLapiSizeConfig = publicLapiDefaultsWithShared(sharedSizeConfig),
      adminLapiSizeConfig = adminLapiDefaultsWithShared(sharedSizeConfig),
      interactiveLapiSizeConfig = interactiveLapiDefaultsWithShared(sharedSizeConfig),
      sharedSizeConfig = sharedSizeConfig,
      eventFormatTotalFiltersSizeLimit = PositiveInt.tryCreate(1_000),
      metadataInputContractBytesSizeLimit = sharedSizeConfig.contractBytesSizeLimit,
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
      contractIdFilter =
        ContractIdFilterSizeConfig(includeInterfaceViewsSizeLimit = PositiveInt.MaxValue),
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
        queryingPartiesSizeLimit = PositiveInt.tryCreate(1_000)
      ),
      getActiveContractsPageRequest = GetActiveContractsPageRequestSizeConfig(
        pageTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      getActiveContractsRequest = GetActiveContractsRequestSizeConfig(
        streamContinuationTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      getHostedPartiesPageRequest = GetHostedPartiesPageRequestSizeConfig(
        pageTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      getHostedPartiesRequest = GetHostedPartiesRequestSizeConfig(
        streamContinuationTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      hostedPartiesFilter = HostedPartiesFilterSizeConfig(
        partyFilterSizeLimit = PositiveInt.tryCreate(10_000),
        synchronizerIdFilterSizeLimit = shared.synchronizerIdsSizeLimit,
      ),
      getLedgerEndRequest = GetLedgerEndRequestSizeConfig(
        synchronizerIdSizeLimit = shared.synchronizerIdsSizeLimit
      ),
      eventFormat = EventFormatSizeConfig(
        filtersByPartySizeLimit = PositiveInt.tryCreate(10_000),
        filtersByContractIdSizeLimit = PositiveInt.tryCreate(1_000),
      ),
      filters = FiltersSizeConfig(cumulativeSizeLimit = PositiveInt.tryCreate(100)),
      participantAuthorizationTopologyFormat = ParticipantAuthorizationTopologyFormatSizeConfig(
        partiesSizeLimit = PositiveInt.tryCreate(1_000)
      ),
      getUpdateByHashRequest = GetUpdateByHashRequestSizeConfig(
        transactionHashSizeLimit = shared.transactionHashBytesSizeLimit
      ),
      getUpdatesPageRequest = GetUpdatesPageRequestSizeConfig(
        pageTokenSizeLimit = shared.continuationTokenBytesSizeLimit
      ),
      reassignmentCommands = ReassignmentCommandsSizeConfig(
        commandsSizeLimit = PositiveInt.tryCreate(1_000)
      ),
      packageMetadataFilter = PackageMetadataFilterSizeConfig(
        packageIdsSizeLimit = PositiveInt.tryCreate(1_000),
        packageNamePrefixesSizeLimit = PositiveInt.tryCreate(1_000),
      ),
      topologyStateFilter = TopologyStateFilterSizeConfig(
        participantIdsSizeLimit = PositiveInt.tryCreate(1_000),
        synchronizerIdsSizeLimit = shared.synchronizerIdsSizeLimit,
      ),
      prefetchContractKey = PrefetchContractKeySizeConfig(
        contractKeySizeLimit = shared.argumentsBytesSizeLimit
      ),
      signature = SignatureSizeConfig(signatureSizeLimit = PositiveInt.tryCreate(8 * 1024)),
      signingPublicKey =
        SigningPublicKeySizeConfig(keyDataSizeLimit = PositiveInt.tryCreate(4 * 1024)),
    )

  private def adminLapiDefaultsWithShared(shared: SharedSizeConfig): AdminLapiSizeConfig =
    AdminLapiSizeConfig(
      allocateExternalPartyRequest = AllocateExternalPartyRequestSizeConfig(
        onboardingTransactionsSizeLimit = PositiveInt.tryCreate(100),
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
      getPartiesRequest =
        GetPartiesRequestSizeConfig(partiesSizeLimit = PositiveInt.tryCreate(1_000)),
      grantUserRightsRequest = GrantUserRightsRequestSizeConfig(
        rightsSizeLimit = shared.userRightsSizeLimit
      ),
      objectMeta = ObjectMetaSizeConfig(annotationsSizeLimit = PositiveInt.tryCreate(100)),
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
        changesSizeLimit = PositiveInt.tryCreate(1_000),
        // the UpdateVettedPackagesForceFlag enum has 2 flags
        updateVettedPackagesForceFlagsSizeLimit = PositiveInt.tryCreate(10),
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
        expectedSignaturesSizeLimit = shared.signaturesSizeLimit
      ),
      getPreferredPackagesRequest = GetPreferredPackagesRequestSizeConfig(
        packageVettingRequirementsSizeLimit = PositiveInt.tryCreate(1_000)
      ),
      metadata = MetadataSizeConfig(
        inputContractsSizeLimit = PositiveInt.tryCreate(1_000),
        // deprecated and always empty: the decoder never reads it
        globalKeyMappingSizeLimit = PositiveInt.MaxValue,
      ),
      metadataSubmitterInfo = MetadataSubmitterInfoSizeConfig(
        actAsSizeLimit = shared.submissionActAsSizeLimit
      ),
      packageVettingRequirement = PackageVettingRequirementSizeConfig(
        partiesSizeLimit = PositiveInt.tryCreate(1_000)
      ),
      partySignatures = PartySignaturesSizeConfig(signaturesSizeLimit =
        // should match with act_as
        PositiveInt.tryCreate(1_000)
      ),
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
        transactionSizeLimit = PositiveInt.tryCreate(20 * 1024 * 1024)
      ),
      singlePartySignatures = SinglePartySignaturesSizeConfig(
        signaturesSizeLimit = shared.signaturesSizeLimit
      ),
    )
}
