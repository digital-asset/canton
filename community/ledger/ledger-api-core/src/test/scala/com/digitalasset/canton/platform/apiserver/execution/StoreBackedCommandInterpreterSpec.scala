// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.participant.state.index.{
  ContractKeyPage,
  ContractState,
  ContractStore,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandInterpreter.StoreNeedKeyContinuationToken
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.InterpretationTimeExceeded
import com.digitalasset.canton.platform.config.CommandServiceConfig
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator.ContractAuthenticatorFn
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.TestEngine
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfPartyId, LfValue}
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  GlobalKey,
  NeedKeyProgression,
  NextGenContractStateMachine as ContractStateMachine,
  Node as LfNode,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.{crypto, engine}
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Future

class StoreBackedCommandInterpreterSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with HasExecutionContext
    with FailOnShutdown
    with BaseTest {

  private val testEngine =
    new TestEngine(
      packagePaths = Seq(CantonExamplesPath),
      iterationsBetweenInterruptions = 10,
      loggerFactory = loggerFactory,
    )
  private val alice = LfPartyId.assertFromString("Alice")

  private val createCycleApiCommand: Commands =
    testEngine.validateCommand(new Cycle("id", alice).create().commands.loneElement, alice)

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-ext-pkg-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  private val externalCallPkgId = parserParameters.defaultPackageId
  private val externalCallPkg = p"""
    metadata ( '-ext-pkg-' : '1.0.0' )

    module M {
      record @serializable T = { party: Party };

      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {party} this] (Nil @Party);
        observers Nil @Party;

        choice Call (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff";
      };
    }
  """

  private val externalCallTemplateId =
    Ref.Identifier(externalCallPkgId, Ref.QualifiedName.assertFromString("M:T"))

  private def mkExternalCallEngine(): Engine = {
    val engine = new Engine(
      EngineConfig(allowedLanguageVersions = LanguageVersion.allLfVersionsRange),
      loggerFactory,
    )
    testEngine.consume(engine.preloadPackage(externalCallPkgId, externalCallPkg)) shouldBe ()
    engine
  }

  private val externalCallPackageResolver: PackageResolver = new PackageResolver {
    override protected def resolveInternal(packageId: Ref.PackageId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[com.digitalasset.daml.lf.language.Ast.Package]] =
      FutureUnlessShutdown.pure(Option.when(packageId == externalCallPkgId)(externalCallPkg))
  }

  private val externalCallCommands: Commands =
    Commands(
      workflowId = None,
      userId = Ref.UserId.assertFromString("app"),
      commandId = com.digitalasset.canton.ledger.api.CommandId(
        Ref.CommandId.assertFromString("external-call-cmd")
      ),
      submissionId = None,
      actAs = Set(alice),
      readAs = Set.empty,
      submittedAt = Time.Timestamp.Epoch,
      deduplicationPeriod = com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration(
        Duration.ZERO
      ),
      commands = ApiCommands(
        commands = ImmArray(
          ApiCommand.CreateAndExercise(
            externalCallTemplateId.toRef,
            Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
            Ref.ChoiceName.assertFromString("Call"),
            Value.ValueUnit,
          )
        ),
        ledgerEffectiveTime = Time.Timestamp.Epoch,
        commandsReference = "external-call-store-backed-test",
      ),
      disclosedContracts = ImmArray.empty,
      synchronizerId = None,
      packagePreferenceSet = Set(externalCallPkgId),
      packageMap = Map(
        externalCallPkgId -> (externalCallPkg.metadata.name, externalCallPkg.metadata.version)
      ),
      prefetchKeys = Seq.empty,
      tapsMaxPasses = None,
    )

  private def repeatCycleApiCommand(
      cid: ContractId,
      disclosedContracts: Seq[FatContractInstance] = Seq.empty,
  ): Commands =
    testEngine.validateCommand(
      new Cycle.ContractId(cid.coid).exerciseRepeat().commands().loneElement,
      alice,
      disclosedContracts,
    )

  private def createCycleContract(id: String = "id") = {
    val (createTx, createMeta) =
      testEngine.submitAndConsume(new Cycle(id, alice).create().commands.loneElement, alice)
    val createNode = createTx.nodes.values.collect { case c: LfNodeCreate => c }.loneElement
    val (_, createSeed) = createMeta.nodeSeeds.toList.loneElement
    val contract = ExampleContractFactory.fromCreate(createNode)
    (createNode, createSeed, contract)
  }

  private val salt: Bytes = ContractAuthenticationDataV1(TestSalt.generateSalt(36))(
    CantonContractIdVersion.maxV1
  ).toLfBytes
  private val identifier: Identifier =
    Ref.Identifier(Ref.PackageId.assertFromString("p"), Ref.QualifiedName.assertFromString("m:n"))
  private val packageName: PackageName = PackageName.assertFromString("pkg-name")
  private val disclosedContractId: LfContractId = TransactionBuilder.newCid
  private def mkCreateNode(contractId: Value.ContractId = disclosedContractId) =
    LfNode.Create(
      coid = contractId,
      packageName = packageName,
      templateId = identifier,
      arg = Value.ValueTrue,
      signatories = Set(Ref.Party.assertFromString("unexpectedSig")),
      stakeholders = Set(
        Ref.Party.assertFromString("unexpectedSig"),
        Ref.Party.assertFromString("unexpectedObs"),
      ),
      keyOpt = Some(
        KeyWithMaintainers.assertBuild(
          templateId = identifier,
          LfValue.ValueTrue,
          crypto.Hash.hashPrivateKey("dummy-key-hash"),
          Set(Ref.Party.assertFromString("unexpectedSig")),
          packageName,
        )
      ),
      version = LfSerializationVersion.StableVersions.max,
    )
  private val disclosedCreateNode = mkCreateNode()
  private val disclosedContractCreateTime = Time.Timestamp.now()

  private val processedDisclosedContracts = ImmArray(
    FatContract.fromCreateNode(
      create = disclosedCreateNode,
      createTime = CreationTime.CreatedAt(disclosedContractCreateTime),
      authenticationData = salt,
    )
  )

  private val submissionSeed = Hash.hashPrivateKey("a key")

  private def mkSut(
      engine: Engine,
      contractStore: ContractStore = mock[ContractStore],
      contractAuthenticator: ContractAuthenticatorFn = (_, _) => Left("Not authorized"),
      tolerance: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(60),
      packageResolver: PackageResolver = testEngine.packageResolver,
  ) =
    new StoreBackedCommandInterpreter(
      engine = engine,
      contractStateMode = ContractStateMachine.Mode.default,
      participant = Ref.ParticipantId.assertFromString("anId"),
      packageResolver = packageResolver,
      contractStore = contractStore,
      contractAuthenticator = contractAuthenticator,
      metrics = LedgerApiServerMetrics.ForTesting,
      prefetchingRecursionLevel = CommandServiceConfig.DefaultContractPrefetchingDepth,
      loggerFactory = loggerFactory,
      dynParamGetter = new TestDynamicSynchronizerParameterGetter(tolerance),
      timeProvider = TimeProvider.UTC,
    )

  "StoreBackedCommandExecutor" should {
    "add interpretation time and used disclosed contracts to result" in {

      val sut = mkSut(testEngine.engine, tolerance = NonNegativeFiniteDuration.Zero)

      sut
        .interpret(createCycleApiCommand, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map { actual =>
          actual.foreach { actualResult =>
            actualResult.interpretationTimeNanos should be > 0L
            actualResult.processedDisclosedContracts shouldBe processedDisclosedContracts
          }
          succeed
        }
    }

    "interpret successfully if time limit is not exceeded" in {
      val tolerance = NonNegativeFiniteDuration.tryOfSeconds(60)
      val sut = mkSut(testEngine.engine, tolerance = tolerance)

      val commands =
        createCycleApiCommand.focus(_.commands.ledgerEffectiveTime).replace(Time.Timestamp.now())
      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Right(_) => succeed
          case other => fail(s"Did not expect: $other")
        }
    }

    "abort interpretation when time limit is exceeded" in {
      val tolerance = NonNegativeFiniteDuration.tryOfSeconds(10)
      val let = Time.Timestamp.now().subtract(Duration.ofSeconds(20))
      val commands = createCycleApiCommand.focus(_.commands.ledgerEffectiveTime).replace(let)
      val sut = mkSut(testEngine.engine, tolerance = tolerance)
      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Left(InterpretationTimeExceeded(`let`, `tolerance`, _)) => succeed
          case other => fail(s"Did not expect: $other")
        }
    }

    "reject external calls directly instead of surfacing a synthetic external service error" in {
      val sut = mkSut(
        mkExternalCallEngine(),
        packageResolver = externalCallPackageResolver,
      )

      sut
        .interpret(externalCallCommands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Left(
                ErrorCause.DamlLf(
                  engine.Error.Interpretation(
                    engine.Error.Interpretation.Internal(_, message, _),
                    _,
                  )
                )
              ) =>
            message should include("External calls are not supported")
          case other =>
            fail(s"Expected direct internal rejection, got $other")
        }
    }
  }

  "Disclosed contract synchronizer id consideration" should {
    val synchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
    val synchronizerId2 = SynchronizerId.tryFromString("x::synchronizer2")
    val disclosedContractId1 = TransactionBuilder.newCid
    val disclosedContractId2 = TransactionBuilder.newCid

    implicit val traceContext: TraceContext = TraceContext.empty

    "not influence the prescribed synchronizer id if no disclosed contracts are attached" in {
      val result = for {
        synchronizerId_from_no_prescribed_no_disclosed <- StoreBackedCommandInterpreter
          .considerDisclosedContractsSynchronizerId(
            prescribedSynchronizerIdO = None,
            disclosedContractsUsedInInterpretation = ImmArray.empty,
            logger,
          )
        synchronizerId_from_prescribed_no_disclosed <-
          StoreBackedCommandInterpreter.considerDisclosedContractsSynchronizerId(
            prescribedSynchronizerIdO = Some(synchronizerId1),
            disclosedContractsUsedInInterpretation = ImmArray.empty,
            logger,
          )
      } yield {
        synchronizerId_from_no_prescribed_no_disclosed shouldBe None
        synchronizerId_from_prescribed_no_disclosed shouldBe Some(synchronizerId1)
      }

      result.value
    }

    "use the disclosed contracts synchronizer id" in {
      StoreBackedCommandInterpreter
        .considerDisclosedContractsSynchronizerId(
          prescribedSynchronizerIdO = None,
          disclosedContractsUsedInInterpretation = ImmArray(
            disclosedContractId1 -> Some(synchronizerId1),
            disclosedContractId2 -> Some(synchronizerId1),
          ),
          logger,
        )
        .map(_ shouldBe Some(synchronizerId1))
        .value
    }

    "return an error if synchronizer ids of disclosed contracts mismatch" in {
      def test(prescribedSynchronizerIdO: Option[SynchronizerId]) =
        inside(
          StoreBackedCommandInterpreter
            .considerDisclosedContractsSynchronizerId(
              prescribedSynchronizerIdO = prescribedSynchronizerIdO,
              disclosedContractsUsedInInterpretation = ImmArray(
                disclosedContractId1 -> Some(synchronizerId1),
                disclosedContractId2 -> Some(synchronizerId2),
              ),
              logger,
            )
        ) { case Left(error: ErrorCause.DisclosedContractsSynchronizerIdsMismatch) =>
          error.mismatchingDisclosedContractSynchronizerIds shouldBe Map(
            disclosedContractId1 -> synchronizerId1,
            disclosedContractId2 -> synchronizerId2,
          )
        }

      test(prescribedSynchronizerIdO = None)
      test(prescribedSynchronizerIdO = Some(SynchronizerId.tryFromString("x::anotherOne")))
    }

    "return an error if the synchronizer id of the disclosed contracts does not match the prescribed synchronizer id" in {
      val synchronizerIdOfDisclosedContracts = synchronizerId1
      val prescribedSynchronizerId = synchronizerId2

      inside(
        StoreBackedCommandInterpreter
          .considerDisclosedContractsSynchronizerId(
            prescribedSynchronizerIdO = Some(prescribedSynchronizerId),
            disclosedContractsUsedInInterpretation = ImmArray(
              disclosedContractId1 -> Some(synchronizerIdOfDisclosedContracts),
              disclosedContractId2 -> Some(synchronizerIdOfDisclosedContracts),
            ),
            logger,
          )
      ) { case Left(error: ErrorCause.PrescribedSynchronizerIdMismatch) =>
        error.commandsSynchronizerId shouldBe prescribedSynchronizerId
        error.synchronizerIdOfDisclosedContracts shouldBe synchronizerIdOfDisclosedContracts
        error.disclosedContractIds shouldBe Set(disclosedContractId1, disclosedContractId2)
      }
    }
  }

  "Contract provision" should {

    s"fail if invalid contract id prefix is used" in {

      val contractStore = mock[ContractStore]

      val invalidCid = ExampleContractFactory.buildContractId().mapCid {
        case Value.ContractId.V1(d, _) =>
          Value.ContractId.V1(d, Bytes.fromByteString(ByteString.copyFrom("invalid".getBytes)))
        case other => fail(s"Unexpected: $other")
      }

      when(
        contractStore.lookupContractState(
          contractId = any[ContractId]
        )(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(ContractState.NotFound)) // prefetch only

      val commands = repeatCycleApiCommand(invalidCid)

      val sut = mkSut(testEngine.engine, contractStore = contractStore)
      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .failed
        .map { _ =>
          succeed
        }

    }

    forAll(Seq(true, false)) { disclosed =>
      val contractType = if (disclosed) "disclosed contract" else "local contract"
      s"complete if $contractType authentication passes" in {

        val (_, _, contract) = createCycleContract()
        val inst: LfFatContractInst = contract.inst

        val contractStore = mock[ContractStore]

        when(
          contractStore.lookupContractState(
            contractId = any[ContractId]
          )(any[LoggingContextWithTrace])
        ).thenReturn(Future.successful(ContractState.NotFound)) // prefetch only

        // When a disclosed contract should be used the mock will cause a failure if it is tried and so
        // verifies that the disclosed key lookup takes precedence.
        if (!disclosed) {
          when(
            contractStore.lookupActiveContract(
              readers = any[Set[Ref.Party]],
              contractId = eqTo(inst.contractId),
            )(any[LoggingContextWithTrace])
          ).thenReturn(Future.successful(Some(inst)))
        }

        val commands =
          repeatCycleApiCommand(inst.contractId, if (disclosed) Seq(inst) else Seq.empty)

        val sut = mkSut(
          testEngine.engine,
          contractStore = contractStore,
          contractAuthenticator = (_, _) => Either.unit,
        )

        sut
          .interpret(commands, submissionSeed)(
            LoggingContextWithTrace(loggerFactory),
            executionContext,
          )
          .map {
            case Right(_) => succeed
            case other => fail(s"Expected success, got $other")
          }
      }

      s"error if $contractType authentication fails" in {

        val (_, _, contract) = createCycleContract()
        val inst: LfFatContractInst = contract.inst

        val contractStore = mock[ContractStore]

        when(
          contractStore.lookupContractState(
            contractId = any[ContractId]
          )(any[LoggingContextWithTrace])
        ).thenReturn(Future.successful(ContractState.NotFound)) // prefetch only

        // When a disclosed contract should be used the mock will cause a failure if it is tried and so
        // verifies that the disclosed lookup takes precedence.
        if (!disclosed) {
          when(
            contractStore.lookupActiveContract(
              readers = any[Set[Ref.Party]],
              contractId = eqTo(inst.contractId),
            )(any[LoggingContextWithTrace])
          ).thenReturn(Future.successful(Some(inst)))
        }

        val commands =
          repeatCycleApiCommand(inst.contractId, if (disclosed) Seq(inst) else Seq.empty)

        val sut = mkSut(
          testEngine.engine,
          contractStore = contractStore,
          contractAuthenticator = (_, _) => Left("Not authorized"),
        )

        sut
          .interpret(commands, submissionSeed)(
            LoggingContextWithTrace(loggerFactory),
            executionContext,
          )
          .map {
            case Left(ErrorCause.DamlLf(engine.Error.Interpretation(_, _))) => succeed
            case other => fail(s"Did not expect: $other")
          }

      }
    }

  }

  private val keyHash: crypto.Hash = crypto.Hash.hashPrivateKey("nuck-test-key")
  private val globalKey: GlobalKey =
    GlobalKey.assertBuild(identifier, packageName, Value.ValueText("key"), keyHash)

  private def mkContract(id: String): LfFatContractInst = {
    val (_, _, contract) = createCycleContract(id)
    contract.inst
  }

  private val testReaders: Set[Ref.Party] = Set(alice)
  private val testMetrics: LedgerApiServerMetrics = LedgerApiServerMetrics.ForTesting
  private implicit val testLoggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace(loggerFactory)

  private def mkMockContractStore(
      pages: Map[Option[Long], ContractKeyPage]
  ): ContractStore = {
    val store = mock[ContractStore]
    when(
      store.lookupNonUniqueContractKey(
        readers = any[Set[Ref.Party]],
        key = any[GlobalKey],
        pageToken = any[Option[Long]],
        limit = any[Int],
      )(any[LoggingContextWithTrace])
    ).thenAnswer[Set[Ref.Party], GlobalKey, Option[Long], Int, LoggingContextWithTrace] {
      case (_, _, pageToken, _, _) =>
        Future.successful(
          pages.getOrElse(
            pageToken,
            fail(s"Unexpected store lookup with pageToken $pageToken"),
          )
        )
    }
    store
  }

  private val emptyContractStore: ContractStore = mkMockContractStore(
    Map(None -> ContractKeyPage(contracts = Vector.empty, nextPageToken = None))
  )

  private val invalidContractStore: ContractStore = {
    val store = mock[ContractStore]
    when(
      store.lookupNonUniqueContractKey(
        readers = any[Set[Ref.Party]],
        key = any[GlobalKey],
        pageToken = any[Option[Long]],
        limit = any[Int],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      Future.failed(new IllegalStateException("Store lookup should not have been called"))
    )
    store
  }

  private def lookup(
      disclosedContracts: Vector[LfFatContractInst] = Vector.empty,
      disclosedContractsById: Map[Value.ContractId, LfFatContractInst] = Map.empty,
      limit: Int = 10,
      progression: NeedKeyProgression.CanContinue = NeedKeyProgression.Unstarted,
      contractStore: ContractStore = emptyContractStore,
  ): FutureUnlessShutdown[(Vector[LfFatContractInst], NeedKeyProgression.HasStarted)] =
    StoreBackedCommandInterpreter.disclosedOrStoreNKeyLookup(
      key = globalKey,
      limit = limit,
      disclosedContracts = disclosedContracts,
      progression = progression,
      disclosedContractsById = disclosedContractsById,
      contractStore = contractStore,
      metrics = testMetrics,
      readers = testReaders,
      lookupContractKeyTime = new AtomicLong(0L),
      lookupContractKeyCount = new AtomicLong(0L),
    )

  private def extractInProgress(
      hasStarted: NeedKeyProgression.HasStarted
  ): NeedKeyProgression.InProgress =
    hasStarted match {
      case ip: NeedKeyProgression.InProgress => ip
      case _ => fail("Expected InProgress")
    }

  "disclosedOrStoreNKeyLookup" should {

    "return empty when no disclosed contracts and store is empty" in {
      lookup().map { case (contracts, progression) =>
        contracts shouldBe empty
        progression shouldBe NeedKeyProgression.Finished
      }
    }

    "return disclosed contracts when they fit within the limit" in {
      val c1 = mkContract("1")
      val c2 = mkContract("2")
      val disclosed = Vector(c1, c2)

      lookup(
        disclosedContracts = disclosed,
        limit = 5,
        contractStore = emptyContractStore,
      ).map { case (contracts, _) =>
        contracts should contain theSameElementsInOrderAs disclosed
      }
    }

    "return only disclosed contracts when they do not fit within the limit" in {
      val disclosed = (1 to 5).map(i => mkContract(i.toString)).toVector

      lookup(
        disclosedContracts = disclosed,
        limit = 3,
        contractStore = invalidContractStore,
      ).map { case (contracts, progression) =>
        contracts should have size 3
        contracts shouldBe disclosed.take(3)
        progression shouldBe NeedKeyProgression.InProgress(
          StoreNeedKeyContinuationToken.ContinueDisclosed(3)
        )
      }
    }

    "paginate through disclosed contracts across multiple calls" in {
      val disclosed = (1 to 5).map(i => mkContract(i.toString)).toVector

      for {
        (page1, token1) <- lookup(
          disclosedContracts = disclosed,
          limit = 2,
          contractStore = invalidContractStore,
        )
        _ = token1 shouldBe NeedKeyProgression.InProgress(
          StoreNeedKeyContinuationToken.ContinueDisclosed(2)
        )
        (page2, token2) <- lookup(
          disclosedContracts = disclosed,
          limit = 2,
          progression = extractInProgress(token1),
          contractStore = invalidContractStore,
        )
        _ = token2 shouldBe NeedKeyProgression.InProgress(
          StoreNeedKeyContinuationToken.ContinueDisclosed(4)
        )
        (page3, token3) <- lookup(
          disclosedContracts = disclosed,
          limit = 2,
          progression = extractInProgress(token2),
          contractStore = emptyContractStore,
        )
      } yield {
        page1 shouldBe disclosed.slice(0, 2)
        page2 shouldBe disclosed.slice(2, 4)
        page3 shouldBe disclosed.slice(4, 5)
        token3 shouldBe NeedKeyProgression.Finished
      }
    }

    "fall back to store when no disclosed contracts" in {
      val inStore = mkContract("inStore")
      val store = mkMockContractStore(
        Map(None -> ContractKeyPage(Vector(inStore), nextPageToken = None))
      )

      lookup(
        disclosedContracts = Vector(),
        limit = 3,
        contractStore = store,
      ).map { case (contracts, progression) =>
        contracts shouldBe Vector(inStore)
        progression shouldBe NeedKeyProgression.Finished
      }
    }

    "fall back to store when disclosed contracts are exhausted" in {
      val c1 = mkContract("disclosed")
      val inStore = mkContract("in-store")
      val store = mkMockContractStore(
        Map(None -> ContractKeyPage(Vector(inStore), nextPageToken = None))
      )

      lookup(
        disclosedContracts = Vector(c1),
        limit = 3,
        contractStore = store,
      ).map { case (contracts, progression) =>
        contracts shouldBe Vector(c1, inStore)
        progression shouldBe NeedKeyProgression.Finished
      }
    }

    "deduplicate store contracts that are also in disclosed contracts" in {
      val sharedContract = mkContract("shared")
      val storeOnlyContract = mkContract("store-only")

      val disclosedById = Map(sharedContract.contractId -> sharedContract)
      val store = mkMockContractStore(
        Map(
          None -> ContractKeyPage(
            Vector(sharedContract, storeOnlyContract),
            nextPageToken = None,
          )
        )
      )

      lookup(
        disclosedContracts = Vector(sharedContract),
        disclosedContractsById = disclosedById,
        limit = 5,
        contractStore = store,
      ).map { case (contracts, _) =>
        contracts shouldBe Vector(sharedContract, storeOnlyContract)
      }
    }

    "correctly pass through store pagination tokens" in {
      val storeContract1 = mkContract("store1")
      val storeContract2 = mkContract("store2")

      val store = mkMockContractStore(
        Map(
          None -> ContractKeyPage(Vector(storeContract1), nextPageToken = Some(42L)),
          Some(42L) -> ContractKeyPage(Vector(storeContract2), nextPageToken = None),
        )
      )

      for {
        (page1, token1) <- lookup(
          limit = 1,
          contractStore = store,
        )
        _ = token1 shouldBe NeedKeyProgression.InProgress(
          StoreNeedKeyContinuationToken.ContinueFromStore(Some(42L))
        )
        (page2, token2) <- lookup(
          limit = 1,
          progression = extractInProgress(token1),
          contractStore = store,
        )
      } yield {
        page1 shouldBe Vector(storeContract1)
        page2 shouldBe Vector(storeContract2)
        token2 shouldBe NeedKeyProgression.Finished
      }
    }

    "delegate directly to store when progression has ContinueFromStore token" in {
      val storeContract = mkContract("store")
      val storeToken = StoreNeedKeyContinuationToken.ContinueFromStore(Some(99L))
      val store = mkMockContractStore(
        Map(Some(99L) -> ContractKeyPage(Vector(storeContract), nextPageToken = None))
      )

      lookup(
        disclosedContracts = Vector(mkContract("disclosed")),
        limit = 5,
        progression = NeedKeyProgression.InProgress(storeToken),
        contractStore = store,
      ).map { case (contracts, progression) =>
        contracts shouldBe Vector(storeContract)
        progression shouldBe NeedKeyProgression.Finished
      }
    }

    "throw on invalid continuation token" in {
      case object InvalidToken extends NeedKeyProgression.Token

      val result = the[IllegalArgumentException] thrownBy lookup(
        progression = NeedKeyProgression.InProgress(InvalidToken)
      )
      result.getMessage should include("Invalid token provided")
    }

    "filter out all store contracts if they are all in disclosed" in {
      val c1 = mkContract("1")
      val c2 = mkContract("2")

      val disclosedById = Map(
        c1.contractId -> c1,
        c2.contractId -> c2,
      )
      val store = mkMockContractStore(
        Map(None -> ContractKeyPage(Vector(c1, c2), nextPageToken = None))
      )

      lookup(
        disclosedContracts = Vector(c1, c2),
        disclosedContractsById = disclosedById,
        contractStore = store,
      ).map { case (contracts, _) =>
        contracts shouldBe Vector(c1, c2)
      }
    }

    "resume from ContinueDisclosed offset correctly" in {
      val disclosed = (1 to 6).map(i => mkContract(i.toString)).toVector
      val storeContract = mkContract("store")
      val store = mkMockContractStore(
        Map(None -> ContractKeyPage(Vector(storeContract), nextPageToken = None))
      )

      lookup(
        disclosedContracts = disclosed,
        limit = 3,
        progression = NeedKeyProgression.InProgress(
          StoreNeedKeyContinuationToken.ContinueDisclosed(4)
        ),
        contractStore = store,
      ).map { case (contracts, progression) =>
        contracts shouldBe disclosed.drop(4) :+ storeContract
        progression shouldBe NeedKeyProgression.Finished
      }
    }

    "deduplicate store contracts against disclosed when resuming from ContinueFromStore" in {
      // `shared` was already served from disclosed contracts in a previous page.
      val shared = mkContract("shared")
      val storeOnly = mkContract("store-only")

      val disclosedById = Map(shared.contractId -> shared)
      val storeToken = StoreNeedKeyContinuationToken.ContinueFromStore(Some(42L))
      val store = mkMockContractStore(
        Map(Some(42L) -> ContractKeyPage(Vector(shared, storeOnly), nextPageToken = None))
      )

      lookup(
        disclosedContracts = Vector(shared),
        disclosedContractsById = disclosedById,
        limit = 5,
        progression = NeedKeyProgression.InProgress(storeToken),
        contractStore = store,
      ).map { case (contracts, progression) =>
        contracts shouldBe Vector(storeOnly)
        progression shouldBe NeedKeyProgression.Finished
      }
    }
  }
}
