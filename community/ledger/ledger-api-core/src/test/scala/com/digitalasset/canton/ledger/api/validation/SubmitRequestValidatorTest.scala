// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForTransactionRequest,
}
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod as DeduplicationPeriodProto
import com.daml.ledger.api.v2.commands.{
  Command,
  Commands,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
  PrefetchContractKey,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.ledger.api.v2.crypto as lapiCrypto
import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  PrepareSubmissionRequest,
  HashingSchemeVersion as ApiHashingSchemeVersion,
}
import com.daml.ledger.api.v2.reassignment_commands.{
  ReassignmentCommand,
  ReassignmentCommands,
  UnassignCommand,
}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.api.v2.value.{
  List as ApiList,
  Optional as ApiOptional,
  TextMap as ApiTextMap,
  *,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{DeduplicationPeriod, Offset}
import com.digitalasset.canton.ledger.api.ApiMocks.{commandId, submissionId, userId, workflowId}
import com.digitalasset.canton.ledger.api.sizeconfig.v2.interactive.{
  CostEstimationHintsSizeConfig,
  MetadataSubmitterInfoSizeConfig,
  PartySignaturesSizeConfig,
  PrepareSubmissionRequestSizeConfig,
  PreparedTransactionSizeConfig,
  SinglePartySignaturesSizeConfig,
}
import com.digitalasset.canton.ledger.api.sizeconfig.v2.{
  CommandsSizeConfig,
  CreateCommandSizeConfig,
  DisclosedContractSizeConfig,
  EventFormatSizeConfig,
  ExerciseCommandSizeConfig,
  PrefetchContractKeySizeConfig,
  PublicLapiSizeConfig,
  ReassignmentCommandsSizeConfig,
  SignatureSizeConfig,
}
import com.digitalasset.canton.ledger.api.util.{DurationConversion, TimestampConversion}
import com.digitalasset.canton.ledger.api.{
  ApiMocks,
  DisclosedContract,
  SubmissionIdGenerator,
  Commands as ApiCommands,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.canton.tea.AccountId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.command.{
  ApiContractKey,
  ApiCommand as LfCommand,
  ApiCommands as LfCommands,
}
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.data.Ref.TypeConRef
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  Node as LfNode,
  SerializationVersion as LfSerializationVersion,
}
import com.digitalasset.daml.lf.value.Value as Lf
import com.digitalasset.daml.lf.value.Value.ValueRecord
import com.google.protobuf.duration.Duration
import com.google.protobuf.empty.Empty
import io.grpc.Status.Code.INVALID_ARGUMENT
import io.grpc.StatusRuntimeException
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Instant, Duration as JDuration}

class SubmitRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with SizeLimitTestUtils
    with TableDrivenPropertyChecks
    with MockitoSugar
    with ArgumentMatchersSugar {
  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  private object api {
    private val packageId = "package"
    private val moduleName = "module"
    private val entityName = "entity"
    val identifier = Identifier(packageId, moduleName = moduleName, entityName = entityName)
    val int64 = Sum.Int64(1)
    val label = "label"
    val constructor = "constructor"
    val submitter = "party"
    val deduplicationDuration = new Duration().withSeconds(10)
    val synchronizerId = "x::synchronizerId"

    private def commandDef(createPackageId: String, moduleName: String = moduleName) =
      Command.of(
        Command.Command.Create(
          CreateCommand.of(
            Some(Identifier(createPackageId, moduleName = moduleName, entityName = entityName)),
            Some(
              Record(
                Some(Identifier(packageId, moduleName = moduleName, entityName = entityName)),
                Seq(RecordField("something", Some(Value(Value.Sum.Bool(true))))),
              )
            ),
          )
        )
      )

    val command = commandDef(packageId)
    val packageNameEncoded = Ref.PackageRef.Name(packageName).toString
    val commandWithPackageNameScoping = commandDef(packageNameEncoded)
    val prefetchKey =
      PrefetchContractKey(Some(identifier), Some(ApiMocks.values.validApiParty), Some(1))
    val prefetchKeyWithPackageNameScoping =
      prefetchKey.copy(templateId = Some(Identifier(packageNameEncoded, moduleName, entityName)))

    val commands = Commands(
      workflowId = workflowId.unwrap,
      userId = userId,
      submissionId = submissionId.unwrap,
      commandId = commandId.unwrap,
      readAs = Nil,
      actAs = Seq(submitter),
      commands = Seq(command),
      deduplicationPeriod = DeduplicationPeriodProto.DeduplicationDuration(deduplicationDuration),
      minLedgerTimeAbs = None,
      minLedgerTimeRel = None,
      packageIdSelectionPreference = Seq.empty,
      synchronizerId = synchronizerId,
      prefetchContractKeys = Seq.empty,
      disclosedContracts = Nil,
      tapsMaxPasses = None,
      accountId = "",
    )
  }

  private object internal {
    val ledgerTime = Instant.EPOCH.plusSeconds(10)
    val submittedAt = Instant.now
    val timeDelta = java.time.Duration.ofSeconds(1)
    val maxDeduplicationDuration = java.time.Duration.ofDays(1)
    val deduplicationDuration = java.time.Duration.ofSeconds(
      api.deduplicationDuration.seconds,
      api.deduplicationDuration.nanos.toLong,
    )

    val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString(api.identifier.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("module"),
        Ref.DottedName.assertFromString("entity"),
      ),
    )
    val templateRef: TypeConRef = TypeConRef(
      Ref.PackageRef.Id(templateId.packageId),
      templateId.qualifiedName,
    )
    val templateRefByName: TypeConRef = TypeConRef(
      Ref.PackageRef.Name(packageName),
      templateId.qualifiedName,
    )

    val disclosedContracts: ImmArray[DisclosedContract] = ImmArray(
      DisclosedContract(
        fatContractInstance = FatContractInstance.fromCreateNode(
          create = LfNode.Create(
            coid = Lf.ContractId.V1.assertFromString("00" + "00" * 32),
            packageName = Ref.PackageName.assertFromString("package"),
            templateId = templateId,
            arg = ValueRecord(
              Some(templateId),
              ImmArray.empty,
            ),
            signatories = Set(Ref.Party.assertFromString("party")),
            stakeholders = Set(Ref.Party.assertFromString("party")),
            keyOpt = None,
            version = LfSerializationVersion.VDev,
          ),
          createTime = CreationTime.CreatedAt(Time.Timestamp.now()),
          authenticationData = Bytes.Empty,
        ),
        synchronizerIdO = Some(SynchronizerId.tryFromString(api.synchronizerId)),
      )
    )

    val emptyCommands: ApiCommands = emptyCommandsBuilder(Ref.PackageRef.Id(templateId.packageId))
    def emptyCommandsBuilder(
        packageRef: Ref.PackageRef,
        packagePreferenceSet: Set[Ref.PackageId] = Set.empty,
        packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
        prefetchKeys: Seq[ApiContractKey] = Seq.empty,
    ) = ApiCommands(
      workflowId = Some(workflowId),
      userId = userId,
      commandId = commandId,
      submissionId = Some(submissionId),
      actAs = Set(ApiMocks.party),
      readAs = Set.empty,
      submittedAt = Time.Timestamp.assertFromInstant(submittedAt),
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(deduplicationDuration),
      commands = LfCommands(
        ImmArray(
          LfCommand.Create(
            TypeConRef(packageRef, templateId.qualifiedName),
            Lf.ValueRecord(
              Option(
                templateId
              ),
              ImmArray((Option(Ref.Name.assertFromString("something")), Lf.ValueTrue)),
            ),
          )
        ),
        Time.Timestamp.assertFromInstant(ledgerTime),
        workflowId.unwrap,
      ),
      disclosedContracts,
      packagePreferenceSet = packagePreferenceSet,
      synchronizerId = Some(SynchronizerId.tryFromString(api.synchronizerId)),
      packageMap = packageMap,
      prefetchKeys = prefetchKeys,
      tapsMaxPasses = None,
      accountId = None,
    )
  }

  private[this] def withLedgerTime(commands: ApiCommands, let: Instant): ApiCommands =
    commands.copy(
      commands = commands.commands.copy(
        ledgerEffectiveTime = Time.Timestamp.assertFromInstant(let)
      )
    )

  private def unexpectedError = sys.error("unexpected error")

  private val testedCommandValidator = {
    val validateDisclosedContractsMock = mock[ValidateDisclosedContracts]

    when(validateDisclosedContractsMock.validateCommands(any[Commands])(any[ErrorLoggingContext]))
      .thenReturn(Right(internal.disclosedContracts))
    when(
      validateDisclosedContractsMock.validateDisclosedContracts(
        any[Seq[com.daml.ledger.api.v2.commands.DisclosedContract]]
      )(any[ErrorLoggingContext])
    )
      .thenReturn(Right(ImmArray.Empty))

    new CommandsValidator(
      validateUpgradingPackageResolutions = ValidateUpgradingPackageResolutions.Empty,
      validateDisclosedContracts = validateDisclosedContractsMock,
    )
  }

  private val testedSubmitAndWaitRequestValidator = new SubmitAndWaitRequestValidator(
    testedCommandValidator
  )

  private val testedSubmitRequestValidator = new SubmitRequestValidator(testedCommandValidator)

  private val testedValueValidator = ValueValidator

  "CommandSubmissionRequestValidator" when {
    "validating SubmitAndWaitRequestValidator" should {
      "validate a request with transaction format" in {
        testedSubmitAndWaitRequestValidator.validate(
          req = SubmitAndWaitForTransactionRequest(
            commands = Some(api.commands),
            transactionFormat = Some(
              TransactionFormat(
                eventFormat = Some(EventFormat(Map.empty, Some(Filters(Nil)), verbose = false)),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
          ),
          currentLedgerTime = internal.ledgerTime,
          currentUtcTime = internal.submittedAt,
          maxDeduplicationDuration = internal.maxDeduplicationDuration,
        ) shouldEqual Right(())
      }

      "validate a request with empty by party and any party filters" in {
        requestMustFailWith(
          testedSubmitAndWaitRequestValidator.validate(
            req = SubmitAndWaitForTransactionRequest(
              commands = Some(api.commands),
              transactionFormat = Some(
                TransactionFormat(
                  eventFormat = Some(EventFormat(Map.empty, None, verbose = false)),
                  transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
                )
              ),
            ),
            currentLedgerTime = internal.ledgerTime,
            currentUtcTime = internal.submittedAt,
            maxDeduplicationDuration = internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: filtersByParty and filtersForAnyParty " +
              "cannot be empty simultaneously",
          metadata = Map.empty,
        )
      }

      "reject a request with transaction format exceeding the total filters limit" in {
        val maxTotalFilters = 1
        requestMustFailWith(
          new SubmitAndWaitRequestValidator(testedCommandValidator)(
            sizeLimitsConfig = sizeLimitsConfig.copy(eventFormatTotalFiltersSizeLimit =
              PositiveInt.tryCreate(maxTotalFilters)
            )
          ).validate(
            req = SubmitAndWaitForTransactionRequest(
              commands = Some(api.commands),
              transactionFormat = Some(
                TransactionFormat(
                  // a template wildcard for party and one for any party
                  eventFormat = Some(
                    EventFormat(Map(party -> Filters(Nil)), Some(Filters(Nil)), verbose = false)
                  ),
                  transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
                )
              ),
            ),
            currentLedgerTime = internal.ledgerTime,
            currentUtcTime = internal.submittedAt,
            maxDeduplicationDuration = internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            totalFiltersLimitExceeded(actual = maxTotalFilters + 1, limit = maxTotalFilters),
          metadata = Map.empty,
        )
      }

      "reject a request without transaction format" in {
        requestMustFailWith(
          testedSubmitAndWaitRequestValidator.validate(
            req = SubmitAndWaitForTransactionRequest(
              commands = Some(api.commands),
              transactionFormat = None,
            ),
            currentLedgerTime = internal.ledgerTime,
            currentUtcTime = internal.submittedAt,
            maxDeduplicationDuration = internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: transaction_format",
          metadata = Map.empty,
        )
      }

      "reject a reassignment request with an event format without filters" in {
        mustFailWithoutFilters(
          testedSubmitAndWaitRequestValidator.validate(
            SubmitAndWaitForReassignmentRequest(
              // not validated: the event format is rejected first
              reassignmentCommands = Some(ReassignmentCommands.defaultInstance),
              eventFormat = Some(EventFormat(Map.empty, None, verbose = false)),
            )
          )
        )
      }
    }

    "validating interactive submission requests" should {
      "accept HASHING_SCHEME_VERSION_V4 for prepare requests" in {
        val result = testedSubmitRequestValidator.validatePrepare(
          req = PrepareSubmissionRequest(
            userId = userId,
            commandId = commandId.unwrap,
            commands = Seq(api.command),
            minLedgerTime = None,
            actAs = Seq(api.submitter),
            readAs = Seq.empty,
            disclosedContracts = Seq.empty,
            synchronizerId = api.synchronizerId,
            packageIdSelectionPreference = Seq.empty,
            verboseHashing = false,
            prefetchContractKeys = Seq.empty,
            maxRecordTime = None,
            estimateTrafficCost = None,
            tapsMaxPasses = None,
            hashingSchemeVersion = Some(ApiHashingSchemeVersion.HASHING_SCHEME_VERSION_V4),
            accountId = "",
          ),
          currentLedgerTime = internal.ledgerTime,
          currentUtcTime = internal.submittedAt,
        )

        inside(result) { case Right(validated) =>
          validated.hashingSchemeVersion shouldBe HashingSchemeVersion.V4
        }
      }
    }

    "validating command submission requests" should {
      "validate a complete request" in {
        testedCommandValidator.validateCommands(
          api.commands,
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(internal.emptyCommands)
      }

      "tolerate a missing submissionId" in {
        testedCommandValidator.validateCommands(
          api.commands.withSubmissionId(""),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(internal.emptyCommands.copy(submissionId = None))
      }

      "reject requests with empty commands" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withCommands(Seq.empty),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: commands",
          metadata = Map.empty,
        )
      }

      "tolerate a missing workflowId" in {
        testedCommandValidator.validateCommands(
          api.commands.withWorkflowId(""),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(
          internal.emptyCommands.copy(
            workflowId = None,
            commands = internal.emptyCommands.commands.copy(commandsReference = ""),
          )
        )
      }

      "tolerate a missing synchronizerId" in {
        testedCommandValidator.validateCommands(
          api.commands.withSynchronizerId(""),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(
          internal.emptyCommands.copy(synchronizerId = None)
        )
      }

      "validate the accountId" in {
        testedCommandValidator.validateCommands(
          api.commands.withAccountId("acme/billing"),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(
          internal.emptyCommands.copy(accountId = Some(AccountId.tryCreate("acme/billing")))
        )

        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withAccountId("acme//billing"),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field account_id: " +
              "Invalid AccountId 'acme//billing': expected non-empty segments separated by '/', " +
              "made of ASCII letters, digits, spaces, and @^$.!`-#+'~_|:()",
          metadata = Map.empty,
        )
      }

      "not allow missing userId" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withUserId(""),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: user_id",
          metadata = Map.empty,
        )
      }

      "not allow missing commandId" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withCommandId(""),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: command_id",
          metadata = Map.empty,
        )
      }

      "not allow missing submitter" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withActAs(Seq.empty),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: party or act_as",
          metadata = Map.empty,
        )
      }

      "correctly read and deduplicate multiple submitters" in {

        val result = testedCommandValidator
          .validateCommands(
            api.commands
              .withActAs(Seq("alice"))
              .addActAs("bob")
              .addReadAs("alice")
              .addReadAs("charlie")
              .addReadAs("bob"),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          )
        inside(result) { case Right(cmd) =>
          // actAs parties are gathered from "party" and "readAs" fields
          cmd.actAs shouldEqual Set("alice", "bob")
          // readAs should exclude all parties that are already actAs parties
          cmd.readAs shouldEqual Set("charlie")
        }
      }

      "tolerate a single submitter specified in the actAs fields" in {

        testedCommandValidator
          .validateCommands(
            api.commands.withActAs(Seq.empty).addActAs(api.submitter),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ) shouldEqual Right(internal.emptyCommands)
      }

      "tolerate a single submitter specified in party, actAs, and readAs fields" in {

        testedCommandValidator
          .validateCommands(
            api.commands
              .withActAs(Seq(api.submitter))
              .addActAs(api.submitter)
              .addReadAs(api.submitter),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ) shouldEqual Right(internal.emptyCommands)
      }

      "advance ledger time if minLedgerTimeAbs is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)

        testedCommandValidator.validateCommands(
          api.commands.copy(
            minLedgerTimeAbs = Some(TimestampConversion.fromInstant(minLedgerTimeAbs))
          ),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(withLedgerTime(internal.emptyCommands, minLedgerTimeAbs))
      }

      "advance ledger time if minLedgerTimeRel is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)

        testedCommandValidator.validateCommands(
          api.commands.copy(
            minLedgerTimeRel = Some(DurationConversion.toProto(internal.timeDelta))
          ),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(withLedgerTime(internal.emptyCommands, minLedgerTimeAbs))
      }

      "transform valid deduplication into correct internal structure" in {
        val deduplicationDuration = Duration.of(10, 0)
        forAll(
          Table[DeduplicationPeriodProto, DeduplicationPeriod](
            ("input proto deduplication", "valid model deduplication"),
            DeduplicationPeriodProto.DeduplicationOffset(12345678L) -> DeduplicationPeriod
              .DeduplicationOffset(Some(Offset.tryFromLong(12345678L))),
            DeduplicationPeriodProto.DeduplicationDuration(
              deduplicationDuration
            ) -> DeduplicationPeriod
              .DeduplicationDuration(JDuration.ofSeconds(10)),
            DeduplicationPeriodProto.Empty -> DeduplicationPeriod.DeduplicationDuration(
              internal.maxDeduplicationDuration
            ),
          )
        ) {
          case (
                sentDeduplication: DeduplicationPeriodProto,
                expectedDeduplication: DeduplicationPeriod,
              ) =>
            val result = testedCommandValidator.validateCommands(
              api.commands.copy(deduplicationPeriod = sentDeduplication),
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationDuration,
            )
            inside(result) { case Right(valid) =>
              valid.deduplicationPeriod shouldBe expectedDeduplication
            }
        }
      }

      "not allow negative deduplication duration" in {
        forAll(
          Table(
            "deduplication period",
            DeduplicationPeriodProto.DeduplicationDuration(Duration.of(-1, 0)),
          )
        ) { deduplication =>
          requestMustFailWith(
            testedCommandValidator.validateCommands(
              api.commands.copy(deduplicationPeriod = deduplication),
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationDuration,
            ),
            code = INVALID_ARGUMENT,
            description =
              "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field deduplication_period: Duration must be positive",
            metadata = Map.empty,
          )
        }
      }

      "allow deduplication duration exceeding maximum deduplication duration" in {
        val durationSecondsExceedingMax =
          internal.maxDeduplicationDuration.plusSeconds(1)
        forAll(
          Table(
            "deduplication period",
            DeduplicationPeriodProto.DeduplicationDuration(
              Duration.of(durationSecondsExceedingMax.getSeconds, 0)
            ),
          )
        ) { deduplicationPeriod =>
          val commandsWithDeduplicationDuration = api.commands
            .copy(deduplicationPeriod = deduplicationPeriod)
          testedCommandValidator.validateCommands(
            commandsWithDeduplicationDuration,
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ) shouldBe Right(
            internal.emptyCommands.copy(
              deduplicationPeriod =
                DeduplicationPeriod.DeduplicationDuration(durationSecondsExceedingMax)
            )
          )
        }
      }

      "default to maximum deduplication duration if deduplication is missing" in {
        testedCommandValidator.validateCommands(
          api.commands.copy(deduplicationPeriod = DeduplicationPeriodProto.Empty),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(
          internal.emptyCommands.copy(
            deduplicationPeriod =
              DeduplicationPeriod.DeduplicationDuration(internal.maxDeduplicationDuration)
          )
        )
      }

      "fail when disclosed contracts validation fails" in {
        val validateDisclosedContractsMock = mock[ValidateDisclosedContracts]

        when(
          validateDisclosedContractsMock.validateCommands(any[Commands])(any[ErrorLoggingContext])
        )
          .thenReturn(
            Left(
              RequestValidationErrors.InvalidField
                .Reject("some failed", "some message")
                .asGrpcError
            )
          )

        val failingDisclosedContractsValidator = new CommandsValidator(
          validateUpgradingPackageResolutions = ValidateUpgradingPackageResolutions.Empty,
          validateDisclosedContracts = validateDisclosedContractsMock,
        )

        requestMustFailWith(
          request = failingDisclosedContractsValidator
            .validateCommands(
              api.commands,
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationDuration,
            ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field some failed: some message",
          metadata = Map.empty,
        )
      }

      "when upgrading" should {
        val validateDisclosedContractsMock = mock[ValidateDisclosedContracts]

        when(
          validateDisclosedContractsMock.validateCommands(any[Commands])(any[ErrorLoggingContext])
        )
          .thenReturn(Right(internal.disclosedContracts))

        val packageMap =
          Map(packageId -> (packageName, Ref.PackageVersion.assertFromString("1.0.0")))
        val validateUpgradingPackageResolutions = new ValidateUpgradingPackageResolutions {
          override def apply(userPackageIdPreferences: Seq[String])(implicit
              errorLoggingContext: ErrorLoggingContext
          ): Either[
            StatusRuntimeException,
            ValidateUpgradingPackageResolutions.ValidatedCommandPackageResolutionsSnapshot,
          ] =
            Right(
              ValidateUpgradingPackageResolutions.ValidatedCommandPackageResolutionsSnapshot(
                packagePreferenceSet =
                  userPackageIdPreferences.toSet.map(Ref.PackageId.assertFromString),
                packageMap = packageMap,
              )
            )
        }

        val commandsValidatorForUpgrading = new CommandsValidator(
          validateUpgradingPackageResolutions = validateUpgradingPackageResolutions,
          validateDisclosedContracts = validateDisclosedContractsMock,
        )

        "allow package name reference instead of package id" in {
          commandsValidatorForUpgrading
            .validateCommands(
              api.commands.copy(
                commands = Seq(api.commandWithPackageNameScoping),
                prefetchContractKeys = Seq(api.prefetchKeyWithPackageNameScoping),
              ),
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationDuration,
            ) shouldBe Right(
            internal.emptyCommandsBuilder(
              Ref.PackageRef.Name(packageName),
              packageMap = packageMap,
              prefetchKeys =
                Seq(ApiContractKey(internal.templateRefByName, ApiMocks.values.validLfParty, 1)),
            )
          )
        }

        "allow correctly specifying the package_id_selection_preference" in {
          val userPackageIdPreference = Seq("validPackageId", "anotherPackageId")
          commandsValidatorForUpgrading
            .validateCommands(
              api.commands.copy(packageIdSelectionPreference = userPackageIdPreference),
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationDuration,
            ) shouldBe Right(
            internal.emptyCommandsBuilder(
              Ref.PackageRef.Id(internal.templateId.packageId),
              packageMap = packageMap,
              packagePreferenceSet =
                userPackageIdPreference.map(Ref.PackageId.assertFromString).toSet,
            )
          )
        }
      }
    }

    "validating prefetched contract keys" should {
      "allow complete keys" in {
        testedCommandValidator.validateCommands(
          api.commands.copy(prefetchContractKeys = Seq(api.prefetchKey)),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        ) shouldEqual Right(
          internal.emptyCommands.copy(
            prefetchKeys =
              Seq(ApiContractKey(internal.templateRef, ApiMocks.values.validLfParty, 1))
          )
        )
      }

      "reject keys with missing template ID" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.copy(prefetchContractKeys = Seq(api.prefetchKey.copy(templateId = None))),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: template_id",
          metadata = Map.empty,
        )
      }

      "reject keys with missing key value" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.copy(prefetchContractKeys = Seq(api.prefetchKey.copy(contractKey = None))),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationDuration,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_key",
          metadata = Map.empty,
        )
      }
    }

    "validating reassignment commands" should {
      val maxReassignmentCommands = 2
      val commandsValidatorWithReassignmentCommandsLimit = new CommandsValidator(
        validateUpgradingPackageResolutions = ValidateUpgradingPackageResolutions.Empty
      )(
        sizeLimitsConfig = sizeLimitsConfig.copy(
          publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
            reassignmentCommands = ReassignmentCommandsSizeConfig(
              commandsSizeLimit = PositiveInt.tryCreate(maxReassignmentCommands)
            )
          )
        )
      )
      val unassignCommand = ReassignmentCommand(
        ReassignmentCommand.Command.UnassignCommand(
          UnassignCommand(
            contractId = contractId.coid,
            source = api.synchronizerId,
            target = "y::synchronizerId",
            overrideSourceValidationPackageId = None,
            overrideTargetValidationPackageId = None,
          )
        )
      )
      def reassignmentCommands(numberOfCommands: Int): ReassignmentCommands =
        ReassignmentCommands(
          workflowId = "workflowId",
          userId = "userId",
          commandId = "commandId",
          submitter = api.submitter,
          submissionId = "submissionId",
          commands = Seq.fill(numberOfCommands)(unassignCommand),
          accountId = "",
        )

      cardinalityLimit("commands", limit = maxReassignmentCommands)(numberOfCommands =>
        commandsValidatorWithReassignmentCommandsLimit.validateReassignmentCommands(
          reassignmentCommands(numberOfCommands)
        )
      )
    }

    "validating the size limits of commands" should {
      val maxElements = 2
      val maxCreatedEventBlobBytes = 4
      val maxArgumentBytes = 20

      val publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig

      def validateCommands(commands: Commands, publicLapiSizeConfig: PublicLapiSizeConfig) =
        new CommandsValidator(ValidateUpgradingPackageResolutions.Empty)(
          sizeLimitsConfig.copy(publicLapiSizeConfig = publicLapiSizeConfig)
        ).validateCommands(
          commands,
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationDuration,
        )

      val commandsLimits = publicLapiSizeConfig.copy(
        commands = CommandsSizeConfig(
          commandsSizeLimit = PositiveInt.tryCreate(maxElements),
          actAsSizeLimit = PositiveInt.tryCreate(maxElements),
          readAsSizeLimit = PositiveInt.tryCreate(maxElements),
          disclosedContractsSizeLimit = PositiveInt.tryCreate(maxElements),
          packageIdSelectionPreferenceSizeLimit = PositiveInt.tryCreate(maxElements),
          prefetchContractKeysSizeLimit = PositiveInt.tryCreate(maxElements),
        ),
        disclosedContract = DisclosedContractSizeConfig(
          createdEventBlobSizeLimit = PositiveInt.tryCreate(maxCreatedEventBlobBytes)
        ),
      )

      def validateWithCommandsLimits(commands: Commands) =
        CommandsValidator.validateCommandsSizeLimits(commands)(
          sizeLimitsConfig.copy(publicLapiSizeConfig = commandsLimits),
          errorLoggingContext,
        )

      def disclosedContract(createdEventBlobBytes: Int) = ProtoDisclosedContract(
        templateId = Some(api.identifier),
        contractId = contractId.coid,
        createdEventBlob = bytes(createdEventBlobBytes),
        synchronizerId = api.synchronizerId,
      )

      "validate the size limits of the commands when validating commands" in {
        requestMustFailWith(
          request = validateCommands(
            api.commands.copy(actAs = parties(maxElements + 1)),
            commandsLimits,
          ),
          code = INVALID_ARGUMENT,
          description =
            cardinalityLimitExceeded("act_as", actual = maxElements + 1, limit = maxElements),
        )
      }

      cardinalityLimit("commands", limit = maxElements)(numberOfCommands =>
        validateWithCommandsLimits(
          api.commands.copy(commands = Seq.fill(numberOfCommands)(api.command))
        )
      )

      cardinalityLimit("act_as", limit = maxElements)(numberOfParties =>
        validateWithCommandsLimits(api.commands.copy(actAs = parties(numberOfParties)))
      )

      cardinalityLimit("read_as", limit = maxElements)(numberOfParties =>
        validateWithCommandsLimits(api.commands.copy(readAs = parties(numberOfParties)))
      )

      cardinalityLimit("disclosed_contracts", limit = maxElements)(numberOfContracts =>
        validateWithCommandsLimits(
          api.commands.copy(disclosedContracts = Seq.fill(numberOfContracts)(disclosedContract(1)))
        )
      )

      cardinalityLimit("package_id_selection_preference", limit = maxElements)(numberOfPackageIds =>
        validateWithCommandsLimits(
          api.commands.copy(packageIdSelectionPreference =
            (1 to numberOfPackageIds).map(i => s"packageId$i")
          )
        )
      )

      cardinalityLimit("prefetch_contract_keys", limit = maxElements)(numberOfKeys =>
        validateWithCommandsLimits(
          api.commands.copy(prefetchContractKeys = Seq.fill(numberOfKeys)(api.prefetchKey))
        )
      )

      byteSizeLimit("created_event_blob", limit = maxCreatedEventBlobBytes)(numberOfBytes =>
        validateWithCommandsLimits(
          api.commands.copy(disclosedContracts = Seq(disclosedContract(numberOfBytes)))
        )
      )

      val argumentsLimit = PositiveInt.tryCreate(maxArgumentBytes)
      val smallValue = Value(api.int64)
      val smallRecord = Record(None, Seq(RecordField("", Some(smallValue))))

      def value(numberOfBytes: Int): Value = {
        val textValue = Value(Value.Sum.Text("a" * (numberOfBytes - 2)))
        assert(textValue.serializedSize == numberOfBytes)
        textValue
      }

      def record(numberOfBytes: Int): Record = {
        val record = Record(None, Seq(RecordField("", Some(value(numberOfBytes - 4)))))
        assert(record.serializedSize == numberOfBytes)
        record
      }

      def validateCommand(command: Command.Command, publicLapiSizeConfig: PublicLapiSizeConfig) =
        validateCommands(
          api.commands.copy(commands = Seq(Command.of(command))),
          publicLapiSizeConfig,
        )

      def exerciseCommand(choiceArgument: Value) =
        Command.Command.Exercise(
          ExerciseCommand(
            templateId = Some(api.identifier),
            contractId = contractId.coid,
            choice = "choice",
            choiceArgument = Some(choiceArgument),
          )
        )

      def exerciseByKeyCommand(contractKey: Value, choiceArgument: Value) =
        Command.Command.ExerciseByKey(
          ExerciseByKeyCommand(
            templateId = Some(api.identifier),
            contractKey = Some(contractKey),
            choice = "choice",
            choiceArgument = Some(choiceArgument),
          )
        )

      def createAndExerciseCommand(createArguments: Record, choiceArgument: Value) =
        Command.Command.CreateAndExercise(
          CreateAndExerciseCommand(
            templateId = Some(api.identifier),
            createArguments = Some(createArguments),
            choice = "choice",
            choiceArgument = Some(choiceArgument),
          )
        )

      serializedSizeLimit(
        "create_arguments",
        limit = maxArgumentBytes,
        limitName = "limit of the create command",
      )(numberOfBytes =>
        validateCommand(
          Command.Command.Create(
            CreateCommand(
              templateId = Some(api.identifier),
              createArguments = Some(record(numberOfBytes)),
            )
          ),
          publicLapiSizeConfig.copy(createCommand = CreateCommandSizeConfig(argumentsLimit)),
        )
      )

      serializedSizeLimit(
        "choice_argument",
        limit = maxArgumentBytes,
        limitName = "limit of the exercise command",
      )(numberOfBytes =>
        validateCommand(
          exerciseCommand(value(numberOfBytes)),
          publicLapiSizeConfig.copy(exerciseCommand = ExerciseCommandSizeConfig(argumentsLimit)),
        )
      )

      serializedSizeLimit(
        "contract_key",
        limit = maxArgumentBytes,
        limitName = "limit of the exercise by key command",
      )(numberOfBytes =>
        validateCommand(
          exerciseByKeyCommand(contractKey = value(numberOfBytes), choiceArgument = smallValue),
          publicLapiSizeConfig.copy(exerciseByKeyCommand =
            publicLapiSizeConfig.exerciseByKeyCommand.copy(contractKeySizeLimit = argumentsLimit)
          ),
        )
      )

      serializedSizeLimit(
        "choice_argument",
        limit = maxArgumentBytes,
        limitName = "limit of the exercise by key command",
      )(numberOfBytes =>
        validateCommand(
          exerciseByKeyCommand(
            contractKey = ApiMocks.values.validApiParty,
            choiceArgument = value(numberOfBytes),
          ),
          publicLapiSizeConfig.copy(exerciseByKeyCommand =
            publicLapiSizeConfig.exerciseByKeyCommand.copy(choiceArgumentSizeLimit = argumentsLimit)
          ),
        )
      )

      serializedSizeLimit(
        "create_arguments",
        limit = maxArgumentBytes,
        limitName = "limit of the create and exercise command",
      )(numberOfBytes =>
        validateCommand(
          createAndExerciseCommand(
            createArguments = record(numberOfBytes),
            choiceArgument = smallValue,
          ),
          publicLapiSizeConfig.copy(createAndExerciseCommand =
            publicLapiSizeConfig.createAndExerciseCommand.copy(createArgumentsSizeLimit =
              argumentsLimit
            )
          ),
        )
      )

      serializedSizeLimit(
        "choice_argument",
        limit = maxArgumentBytes,
        limitName = "limit of the create and exercise command",
      )(numberOfBytes =>
        validateCommand(
          createAndExerciseCommand(
            createArguments = smallRecord,
            choiceArgument = value(numberOfBytes),
          ),
          publicLapiSizeConfig.copy(createAndExerciseCommand =
            publicLapiSizeConfig.createAndExerciseCommand.copy(choiceArgumentSizeLimit =
              argumentsLimit
            )
          ),
        )
      )

      serializedSizeLimit(
        "contract_key",
        limit = maxArgumentBytes,
        limitName = "limit of the prefetch contract key",
      )(numberOfBytes =>
        validateCommands(
          api.commands.copy(prefetchContractKeys =
            Seq(api.prefetchKey.copy(contractKey = Some(value(numberOfBytes))))
          ),
          publicLapiSizeConfig.copy(prefetchContractKey =
            PrefetchContractKeySizeConfig(argumentsLimit)
          ),
        )
      )
    }

    "validating the size limits of prepare submission requests" should {
      val maxCommands = 2
      val maxActAs = 3
      val maxReadAs = 4
      val maxDisclosedContracts = 5
      val maxPackageIdSelectionPreference = 6
      val maxPrefetchContractKeys = 7
      val maxExpectedSignatures = 8
      val maxCreatedEventBlobBytes = 9

      val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
        publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
          disclosedContract = DisclosedContractSizeConfig(
            createdEventBlobSizeLimit = PositiveInt.tryCreate(maxCreatedEventBlobBytes)
          )
        ),
        interactiveLapiSizeConfig = sizeLimitsConfig.interactiveLapiSizeConfig.copy(
          prepareSubmissionRequest = PrepareSubmissionRequestSizeConfig(
            commandsSizeLimit = PositiveInt.tryCreate(maxCommands),
            actAsSizeLimit = PositiveInt.tryCreate(maxActAs),
            readAsSizeLimit = PositiveInt.tryCreate(maxReadAs),
            disclosedContractsSizeLimit = PositiveInt.tryCreate(maxDisclosedContracts),
            packageIdSelectionPreferenceSizeLimit =
              PositiveInt.tryCreate(maxPackageIdSelectionPreference),
            prefetchContractKeysSizeLimit = PositiveInt.tryCreate(maxPrefetchContractKeys),
          ),
          costEstimationHints = CostEstimationHintsSizeConfig(
            expectedSignaturesSizeLimit = PositiveInt.tryCreate(maxExpectedSignatures)
          ),
        ),
      )

      val prepareRequest = PrepareSubmissionRequest.defaultInstance

      def validate(prepareRequest: PrepareSubmissionRequest) =
        CommandsValidator.validatePrepareSubmissionRequestSizeLimits(prepareRequest)(
          sizeLimitsConfigWithLimits,
          errorLoggingContext,
        )

      def disclosedContract(createdEventBlobBytes: Int) = ProtoDisclosedContract(
        templateId = Some(api.identifier),
        contractId = contractId.coid,
        createdEventBlob = bytes(createdEventBlobBytes),
        synchronizerId = api.synchronizerId,
      )

      "validate the size limits of the prepare request when validating a prepare request" in {
        requestMustFailWith(
          request = new CommandsValidator(ValidateUpgradingPackageResolutions.Empty)(
            sizeLimitsConfigWithLimits
          ).validatePrepareRequest(
            prepareRequest.copy(actAs = parties(maxActAs + 1)),
            internal.ledgerTime,
            internal.submittedAt,
          ),
          code = INVALID_ARGUMENT,
          description = cardinalityLimitExceeded("act_as", actual = maxActAs + 1, limit = maxActAs),
        )
      }

      cardinalityLimit("commands", limit = maxCommands)(numberOfCommands =>
        validate(prepareRequest.copy(commands = Seq.fill(numberOfCommands)(api.command)))
      )

      cardinalityLimit("act_as", limit = maxActAs)(numberOfParties =>
        validate(prepareRequest.copy(actAs = parties(numberOfParties)))
      )

      cardinalityLimit("read_as", limit = maxReadAs)(numberOfParties =>
        validate(prepareRequest.copy(readAs = parties(numberOfParties)))
      )

      cardinalityLimit("disclosed_contracts", limit = maxDisclosedContracts)(numberOfContracts =>
        validate(
          prepareRequest.copy(disclosedContracts =
            Seq.fill(numberOfContracts)(disclosedContract(1))
          )
        )
      )

      cardinalityLimit("package_id_selection_preference", limit = maxPackageIdSelectionPreference)(
        numberOfPackageIds =>
          validate(
            prepareRequest.copy(packageIdSelectionPreference =
              (1 to numberOfPackageIds).map(i => s"packageId$i")
            )
          )
      )

      cardinalityLimit("prefetch_contract_keys", limit = maxPrefetchContractKeys)(numberOfKeys =>
        validate(
          prepareRequest.copy(prefetchContractKeys = Seq.fill(numberOfKeys)(api.prefetchKey))
        )
      )

      cardinalityLimit("expected_signatures", limit = maxExpectedSignatures)(numberOfSignatures =>
        validate(
          prepareRequest.copy(estimateTrafficCost =
            Some(
              iss.CostEstimationHints(
                disabled = false,
                expectedSignatures = Seq.fill(numberOfSignatures)(
                  lapiCrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519
                ),
              )
            )
          )
        )
      )

      byteSizeLimit("created_event_blob", limit = maxCreatedEventBlobBytes)(numberOfBytes =>
        validate(prepareRequest.copy(disclosedContracts = Seq(disclosedContract(numberOfBytes))))
      )
    }

    "validating the size limits of execute submission requests" should {
      // distinct limits, so that a check reading the limit of another field fails
      val maxActAs = 2
      val maxInputContracts = 3
      val maxPartySignatures = 5
      val maxSinglePartySignatures = 6
      val maxSignatureBytes = 7
      val maxFiltersByParty = 8
      val maxTransactionBytes = 20
      val maxInputContractBytes = 30

      val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
        metadataInputContractBytesSizeLimit = PositiveInt.tryCreate(maxInputContractBytes),
        publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
          signature =
            SignatureSizeConfig(signatureSizeLimit = PositiveInt.tryCreate(maxSignatureBytes)),
          eventFormat = EventFormatSizeConfig(
            filtersByPartySizeLimit = PositiveInt.tryCreate(maxFiltersByParty)
          ),
        ),
        interactiveLapiSizeConfig = sizeLimitsConfig.interactiveLapiSizeConfig.copy(
          preparedTransaction = PreparedTransactionSizeConfig(
            transactionSizeLimit = PositiveInt.tryCreate(maxTransactionBytes)
          ),
          metadata = sizeLimitsConfig.interactiveLapiSizeConfig.metadata.copy(
            inputContractsSizeLimit = PositiveInt.tryCreate(maxInputContracts)
          ),
          metadataSubmitterInfo = MetadataSubmitterInfoSizeConfig(
            actAsSizeLimit = PositiveInt.tryCreate(maxActAs)
          ),
          partySignatures = PartySignaturesSizeConfig(
            signaturesSizeLimit = PositiveInt.tryCreate(maxPartySignatures)
          ),
          singlePartySignatures = SinglePartySignaturesSizeConfig(
            signaturesSizeLimit = PositiveInt.tryCreate(maxSinglePartySignatures)
          ),
        ),
      )

      val submitRequestValidatorWithLimits =
        new SubmitRequestValidator(testedCommandValidator)(sizeLimitsConfigWithLimits)

      def validate(executeRequest: iss.ExecuteSubmissionRequest) =
        SubmitRequestValidator.validateExecuteSubmissionRequestSizeLimits(executeRequest)(
          errorLoggingContext,
          sizeLimitsConfigWithLimits,
        )

      def withMetadata(metadata: iss.Metadata) =
        iss.ExecuteSubmissionRequest.defaultInstance
          .withPreparedTransaction(
            iss.PreparedTransaction(transaction = None, metadata = Some(metadata))
          )

      def withPartySignatures(signatures: Seq[iss.SinglePartySignatures]) =
        iss.ExecuteSubmissionRequest.defaultInstance.withPartySignatures(
          iss.PartySignatures(signatures)
        )

      def signature(numberOfBytes: Int) = lapiCrypto.Signature(
        format = lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
        signature = bytes(numberOfBytes),
        signedBy = "fingerprint",
        signingAlgorithmSpec = lapiCrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
      )

      def singlePartySignatures(signatures: Seq[lapiCrypto.Signature]) =
        iss.SinglePartySignatures(party = "alice::fingerprint", signatures = signatures)

      def damlTransaction(numberOfBytes: Int) = {
        val transaction = iss.DamlTransaction.defaultInstance.withVersion("a" * (numberOfBytes - 2))
        assert(transaction.serializedSize == numberOfBytes)
        transaction
      }

      def inputContract(numberOfBytes: Int) = {
        // 3 bytes of field tag and length of event_blob
        val inputContract =
          iss.Metadata.InputContract.defaultInstance.withEventBlob(bytes(numberOfBytes - 3))
        assert(inputContract.serializedSize == numberOfBytes)
        inputContract
      }

      // passes the non-size validation of an execute request
      val executeRequest = iss.ExecuteSubmissionRequest(
        preparedTransaction = Some(
          iss.PreparedTransaction(
            transaction = None,
            metadata = Some(
              iss.Metadata.defaultInstance
                .withSubmitterInfo(
                  iss.Metadata
                    .SubmitterInfo(actAs = Seq(api.submitter), commandId = commandId.unwrap)
                )
                .withSynchronizerId(api.synchronizerId)
            ),
          )
        ),
        partySignatures = Some(iss.PartySignatures(Seq(singlePartySignatures(Seq(signature(1)))))),
        deduplicationPeriod = iss.ExecuteSubmissionRequest.DeduplicationPeriod.Empty,
        submissionId = "",
        userId = userId,
        hashingSchemeVersion = ApiHashingSchemeVersion.HASHING_SCHEME_VERSION_V2,
        minLedgerTime = None,
        accountId = "",
      )

      "validate the size limits of the execute request when validating an execute request" in {
        requestMustFailWith(
          request = submitRequestValidatorWithLimits.validateExecute(
            withPartySignatures(Seq.fill(maxPartySignatures + 1)(singlePartySignatures(Nil))),
            internal.ledgerTime,
            SubmissionIdGenerator.Random,
            internal.maxDeduplicationDuration,
          )(errorLoggingContext),
          code = INVALID_ARGUMENT,
          description = cardinalityLimitExceeded(
            "signatures",
            actual = maxPartySignatures + 1,
            limit = maxPartySignatures,
          ),
        )
      }

      "validate the transaction format when validating an execute-and-wait request" in {
        requestMustFailWith(
          request = submitRequestValidatorWithLimits.validateExecuteAndWait(
            executeRequest,
            Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = parties(maxFiltersByParty + 1).map(_ -> Filters(Nil)).toMap,
                    filtersForAnyParty = None,
                    verbose = false,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            internal.ledgerTime,
            SubmissionIdGenerator.Random,
            internal.maxDeduplicationDuration,
          )(errorLoggingContext),
          code = INVALID_ARGUMENT,
          description = cardinalityLimitExceeded(
            "filters_by_party",
            actual = maxFiltersByParty + 1,
            limit = maxFiltersByParty,
          ),
        )
      }

      serializedSizeLimit("transaction", limit = maxTransactionBytes)(numberOfBytes =>
        validate(
          iss.ExecuteSubmissionRequest.defaultInstance.withPreparedTransaction(
            iss.PreparedTransaction(
              transaction = Some(damlTransaction(numberOfBytes)),
              metadata = None,
            )
          )
        )
      )

      cardinalityLimit("act_as", limit = maxActAs)(numberOfParties =>
        validate(
          withMetadata(
            iss.Metadata.defaultInstance.withSubmitterInfo(
              iss.Metadata.SubmitterInfo(actAs = parties(numberOfParties), commandId = "")
            )
          )
        )
      )

      cardinalityLimit("input_contracts", limit = maxInputContracts)(numberOfContracts =>
        validate(
          withMetadata(
            iss.Metadata.defaultInstance.withInputContracts(
              Seq.fill(numberOfContracts)(iss.Metadata.InputContract.defaultInstance)
            )
          )
        )
      )

      serializedSizeLimit("input_contracts", limit = maxInputContractBytes)(numberOfBytes =>
        validate(
          withMetadata(
            iss.Metadata.defaultInstance.withInputContracts(Seq(inputContract(numberOfBytes)))
          )
        )
      )

      cardinalityLimit(
        "signatures",
        limit = maxPartySignatures,
        limitName = "limit of the party signatures",
      )(numberOfParties =>
        validate(withPartySignatures(Seq.fill(numberOfParties)(singlePartySignatures(Nil))))
      )

      cardinalityLimit(
        "signatures",
        limit = maxSinglePartySignatures,
        limitName = "limit of the signatures of a party",
      )(numberOfSignatures =>
        validate(
          withPartySignatures(
            Seq(singlePartySignatures(Seq.fill(numberOfSignatures)(signature(1))))
          )
        )
      )

      byteSizeLimit("signature", limit = maxSignatureBytes)(numberOfBytes =>
        validate(withPartySignatures(Seq(singlePartySignatures(Seq(signature(numberOfBytes))))))
      )
    }

    "validating contractId values" should {
      "succeed" in {

        val coid = Lf.ContractId.V1.assertFromString("00" + "00" * 32)

        val input = Value(Sum.ContractId(coid.coid))
        val expected = Lf.ValueContractId(coid)

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }
    }

    "validating party values" should {
      "convert valid party" in {
        testedValueValidator.validateValue(ApiMocks.values.validApiParty) shouldEqual Right(
          ApiMocks.values.validLfParty
        )
      }

      "reject non valid party" in {
        requestMustFailWith(
          request = testedValueValidator.validateValue(ApiMocks.values.invalidApiParty),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
        )
      }
    }

    "validating decimal values" should {
      "convert valid decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "absolute values" -> "scale",
            "0" -> 0,
            "0.0" -> 0,
            "1.0000" -> 0,
            "3.1415926536" -> 10,
            "1" + "0" * 27 -> 0,
            "1" + "0" * 27 + "." + "0" * 9 + "1" -> 10,
            "0." + "0" * 9 + "1" -> 10,
            "0." -> 0,
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { (absoluteValue, expectedScale) =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            val expected =
              Lf.ValueNumeric(
                Numeric
                  .assertFromBigDecimal(Numeric.Scale.assertFromInt(expectedScale), BigDecimal(s))
              )
            testedValueValidator.validateValue(input) shouldEqual Right(expected)
          }
        }

      }

      "reject out-of-bound decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "absolute values" -> "scale",
            "1" + "0" * 38 -> 0,
            "1" + "0" * 28 + "." + "0" * 10 + "1" -> 11,
            "1" + "0" * 27 + "." + "0" * 11 + "1" -> 12,
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { (absoluteValue, _) =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            requestMustFailWith(
              request = testedValueValidator.validateValue(input),
              code = INVALID_ARGUMENT,
              description =
                s"""INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Could not read Numeric string "$s"""",
              metadata = Map.empty,
            )
          }
        }
      }

      "reject invalid decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "invalid absolute values",
            "zero",
            "1E10",
            "+",
            "-",
            "0x01",
            ".",
            "",
            ".0",
            "0." + "0" * 37 + "1",
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { absoluteValue =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            requestMustFailWith(
              request = testedValueValidator.validateValue(input),
              code = INVALID_ARGUMENT,
              description =
                s"""INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Could not read Numeric string "$s"""",
              metadata = Map.empty,
            )
          }
        }
      }

    }

    "validating text values" should {
      "accept any of them" in {
        val strings =
          Table("string", "", "a¶‱😂", "a¶‱😃")

        forEvery(strings) { s =>
          val input = Value(Sum.Text(s))
          val expected = Lf.ValueText(s)
          testedValueValidator.validateValue(input) shouldEqual Right(expected)
        }
      }
    }

    "validating timestamp values" should {
      "accept valid timestamp" in {
        val testCases = Table(
          "long/timestamp",
          Time.Timestamp.MinValue.micros -> Time.Timestamp.MinValue,
          0L -> Time.Timestamp.Epoch,
          Time.Timestamp.MaxValue.micros -> Time.Timestamp.MaxValue,
        )

        forEvery(testCases) { case (long, timestamp) =>
          val input = Value(Sum.Timestamp(long))
          val expected = Lf.ValueTimestamp(timestamp)
          testedValueValidator.validateValue(input) shouldEqual Right(expected)
        }
      }

      "reject out-of-bound timestamp" in {
        val testCases = Table(
          "long/timestamp",
          Long.MinValue,
          Time.Timestamp.MinValue.micros - 1,
          Time.Timestamp.MaxValue.micros + 1,
          Long.MaxValue,
        )

        forEvery(testCases) { long =>
          val input = Value(Sum.Timestamp(long))
          requestMustFailWith(
            request = testedValueValidator.validateValue(input),
            code = INVALID_ARGUMENT,
            description =
              s"INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: cannot convert long $long into Timestamp:out of bound Timestamp $long",
            metadata = Map.empty,
          )
        }
      }
    }

    "validating date values" should {
      "accept valid date" in {
        val testCases = Table(
          "int/date",
          Time.Date.MinValue.days -> Time.Date.MinValue,
          0 -> Time.Date.Epoch,
          Time.Date.MaxValue.days -> Time.Date.MaxValue,
        )

        forEvery(testCases) { case (int, date) =>
          val input = Value(Sum.Date(int))
          val expected = Lf.ValueDate(date)
          testedValueValidator.validateValue(input) shouldEqual Right(expected)
        }
      }

      "reject out-of-bound date" in {
        val testCases = Table(
          "int/date",
          Int.MinValue,
          Time.Date.MinValue.days - 1,
          Time.Date.MaxValue.days + 1,
          Int.MaxValue,
        )

        forEvery(testCases) { int =>
          val input = Value(Sum.Date(int))
          requestMustFailWith(
            request = testedValueValidator.validateValue(input),
            code = INVALID_ARGUMENT,
            description =
              s"INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: out of bound Date $int",
            metadata = Map.empty,
          )
        }
      }
    }

    "validating boolean values" should {
      "accept any of them" in {
        testedValueValidator.validateValue(Value(Sum.Bool(true))) shouldEqual Right(Lf.ValueTrue)
        testedValueValidator.validateValue(Value(Sum.Bool(false))) shouldEqual Right(Lf.ValueFalse)
      }
    }

    "validating unit values" should {
      "succeed" in {
        testedValueValidator.validateValue(Value(Sum.Unit(Empty()))) shouldEqual Right(Lf.ValueUnit)
      }
    }

    "validating record values" should {
      "convert valid records" in {
        val record =
          Value(
            Sum.Record(
              Record(Some(api.identifier), Seq(RecordField(api.label, Some(Value(api.int64)))))
            )
          )
        val expected =
          Lf.ValueRecord(
            Some(ApiMocks.identifier),
            ImmArray(Some(ApiMocks.label) -> ApiMocks.values.int64),
          )
        testedValueValidator.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing identifiers in records" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          Lf.ValueRecord(None, ImmArray(Some(ApiMocks.label) -> ApiMocks.values.int64))
        testedValueValidator.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing labels in record fields" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField("", Some(Value(api.int64)))))))
        val expected =
          ValueRecord(None, ImmArray(None -> ApiMocks.values.int64))
        testedValueValidator.validateValue(record) shouldEqual Right(expected)
      }

      "not allow missing record values" in {
        val record =
          Value(Sum.Record(Record(Some(api.identifier), Seq(RecordField(api.label, None)))))
        requestMustFailWith(
          request = testedValueValidator.validateValue(record),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
          metadata = Map.empty,
        )
      }

    }

    "validating variant values" should {

      "convert valid variants" in {
        val variant =
          Value(Sum.Variant(Variant(Some(api.identifier), api.constructor, Some(Value(api.int64)))))
        val expected = Lf.ValueVariant(
          Some(ApiMocks.identifier),
          ApiMocks.values.constructor,
          ApiMocks.values.int64,
        )
        testedValueValidator.validateValue(variant) shouldEqual Right(expected)
      }

      "tolerate missing identifiers" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, Some(Value(api.int64)))))
        val expected =
          Lf.ValueVariant(None, ApiMocks.values.constructor, ApiMocks.values.int64)

        testedValueValidator.validateValue(variant) shouldEqual Right(expected)
      }

      "not allow missing constructor" in {
        val variant = Value(Sum.Variant(Variant(None, "", Some(Value(api.int64)))))
        requestMustFailWith(
          request = testedValueValidator.validateValue(variant),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: constructor",
          metadata = Map.empty,
        )
      }

      "not allow missing values" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, None)))
        requestMustFailWith(
          request = testedValueValidator.validateValue(variant),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
          metadata = Map.empty,
        )
      }

    }

    "validating list values" should {
      "convert empty lists" in {
        val input = Value(Sum.List(ApiList(List.empty)))
        val expected =
          Lf.ValueNil

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid lists" in {
        val list = Value(Sum.List(ApiList(Seq(Value(api.int64), Value(api.int64)))))
        val expected =
          Lf.ValueList(FrontStack(ApiMocks.values.int64, ApiMocks.values.int64))
        testedValueValidator.validateValue(list) shouldEqual Right(expected)
      }

      "reject lists containing invalid values" in {
        val input = Value(
          Sum.List(
            ApiList(Seq(ApiMocks.values.validApiParty, ApiMocks.values.invalidApiParty))
          )
        )
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
        )
      }
    }

    "validating optional values" should {
      "convert empty optionals" in {
        val input = Value(Sum.Optional(ApiOptional(None)))
        val expected = Lf.ValueNone

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid non-empty optionals" in {
        val list = Value(Sum.Optional(ApiOptional(Some(ApiMocks.values.validApiParty))))
        val expected = Lf.ValueOptional(Some(ApiMocks.values.validLfParty))
        testedValueValidator.validateValue(list) shouldEqual Right(expected)
      }

      "reject optional containing invalid values" in {
        val input = Value(Sum.Optional(ApiOptional(Some(ApiMocks.values.invalidApiParty))))
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
        )
      }
    }

    "validating map values" should {
      "convert empty maps" in {
        val input = Value(Sum.TextMap(ApiTextMap(List.empty)))
        val expected = Lf.ValueTextMap(SortedLookupList.Empty)
        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid maps" in {
        val entries = (1 until 5)
          .map { x =>
            Utf8.sha256(Utf8.getBytes(x.toString)) -> x.toLong
          }
          .to(ImmArray)
        val apiEntries = entries.map { case (k, v) =>
          ApiTextMap.Entry(k, Some(Value(Sum.Int64(v))))
        }
        val input = Value(Sum.TextMap(ApiTextMap(apiEntries.toSeq)))
        val lfEntries = entries.map { case (k, v) => k -> Lf.ValueInt64(v) }
        val expected =
          Lf.ValueTextMap(SortedLookupList.fromImmArray(lfEntries).getOrElse(unexpectedError))

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }

      "reject maps with repeated keys" in {
        val entries = (1 +: (1 until 5))
          .map { x =>
            Utf8.sha256(Utf8.getBytes(x.toString)) -> x.toLong
          }
          .to(ImmArray)
        val apiEntries = entries.map { case (k, v) =>
          ApiTextMap.Entry(k, Some(Value(Sum.Int64(v))))
        }
        val input = Value(Sum.TextMap(ApiTextMap(apiEntries.toSeq)))
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: key 6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b duplicated when trying to build map",
          metadata = Map.empty,
        )
      }

      "reject maps containing invalid value" in {
        val apiEntries =
          List(
            ApiTextMap.Entry("1", Some(ApiMocks.values.validApiParty)),
            ApiTextMap.Entry("2", Some(ApiMocks.values.invalidApiParty)),
          )
        val input = Value(Sum.TextMap(ApiTextMap(apiEntries)))
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
        )
      }
    }
  }
}
