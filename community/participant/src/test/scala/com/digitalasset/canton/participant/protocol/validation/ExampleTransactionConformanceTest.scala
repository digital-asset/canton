// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory
import com.digitalasset.canton.participant.protocol.validation.ExampleTransactionConformanceTest.HashReInterpretationCounter
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.store.{ContractLookup, ReplayContractLookup}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.DAMLe.{
  HasReinterpret,
  ReInterpretationResult,
  UsedPackages,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator.ContractAuthenticatorFn
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.{ContractValidator, TestContractHasher}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfCommand,
  LfGlobalKeyMapping,
  LfPackageName,
  LfPackageVersion,
  LfPartyId,
  ProtocolVersionChecksAsyncWordSpec,
}
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.language.Ast.{DeclaredImports, Expr, GenPackage, PackageMetadata}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.Future

class ExampleTransactionConformanceTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {

  val sequencerTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(0)

  val ledgerTimeRecordTimeTolerance: Duration = Duration.ofSeconds(10)

  val packageName: LfPackageName = PackageName.assertFromString("package-name")
  val packageVersion: LfPackageVersion = LfPackageVersion.assertFromString("1.0.0")
  val packageMetadata: PackageMetadata = PackageMetadata(packageName, packageVersion, None)
  val genPackage: GenPackage[Expr] =
    GenPackage(
      modules = Map.empty,
      directDeps = Set.empty,
      languageVersion = LanguageVersion.defaultLfVersion,
      metadata = packageMetadata,
      imports = DeclaredImports(Set.empty),
      isUtilityPackage = true,
    )
  val packageResolver: PackageResolver = new PackageResolver {
    override protected def resolveInternal(packageId: PackageId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[Ast.Package]] =
      FutureUnlessShutdown.pure(Some(genPackage))
  }

  val pureCrypto = new SymbolicPureCrypto()

  CantonContractIdVersion.all.foreach { contractIdVersion =>
    val factory: ExampleTransactionFactory = new ExampleTransactionFactory()(
      cantonContractIdVersion = contractIdVersion
    )

    def reinterpretExample(
        example: ExampleTransaction,
        timeBoundaries: LedgerTimeBoundaries = LedgerTimeBoundaries.unconstrained,
    ): HasReinterpret & HashReInterpretationCounter = new HasReinterpret
      with HashReInterpretationCounter {

      override def reinterpret(
          contracts: ReplayContractLookup,
          contractAuthenticator: ContractAuthenticatorFn,
          submitters: Set[LfPartyId],
          command: LfCommand,
          topologySnapshot: TopologySnapshot,
          ledgerTime: CantonTimestamp,
          preparationTime: CantonTimestamp,
          rootSeed: Option[LfHash],
          packageResolution: Map[PackageName, PackageId],
          expectFailure: Boolean,
          getEngineAbortStatus: GetEngineAbortStatus,
          externalCallReplayData: () => FutureUnlessShutdown[DAMLe.ExternalCallReplayData],
      )(implicit traceContext: TraceContext): EitherT[
        FutureUnlessShutdown,
        DAMLe.ReinterpretationError,
        ReInterpretationResult,
      ] = {
        incrementInterpretations()
        ledgerTime shouldEqual factory.ledgerTime
        preparationTime shouldEqual factory.preparationTime

        val (_, (reinterpretedTx, metadata, keyResolver), _) =
          // The code below assumes that for reinterpretedSubtransactions the combination
          // of command and root-seed wil be unique. In the examples used to date this is
          // the case. A limitation of this approach is that only one LookupByKey transaction
          // can be returned as the root seed is unset in the ReplayCommand.
          example.reinterpretedSubtransactions.find { case (viewTree, (tx, md, _), _) =>
            viewTree.viewParticipantData.rootAction.command == command &&
            md.seeds.get(tx.roots(0)) == rootSeed
          }.value

        EitherT.rightT(
          ReInterpretationResult(
            reinterpretedTx,
            metadata,
            UsedPackages(Set.empty, Set.empty),
            timeBoundaries,
          )
        )
      }
    }

    def reinterpretTransaction(
        example: ExampleTransaction,
        transaction: WellFormedTransaction[WellFormedTransaction.WithoutSuffixes],
    ): HasReinterpret = new HasReinterpret {

      override def reinterpret(
          contracts: ReplayContractLookup,
          contractAuthenticator: ContractAuthenticatorFn,
          submitters: Set[LfPartyId],
          command: LfCommand,
          topologySnapshot: TopologySnapshot,
          ledgerTime: CantonTimestamp,
          preparationTime: CantonTimestamp,
          rootSeed: Option[LfHash],
          packageResolution: Map[PackageName, PackageId],
          expectFailure: Boolean,
          getEngineAbortStatus: GetEngineAbortStatus,
          externalCallReplayData: () => FutureUnlessShutdown[DAMLe.ExternalCallReplayData],
      )(implicit traceContext: TraceContext): EitherT[
        FutureUnlessShutdown,
        DAMLe.ReinterpretationError,
        ReInterpretationResult,
      ] = {
        ledgerTime shouldEqual factory.ledgerTime
        preparationTime shouldEqual factory.preparationTime

        val matchesView = example.reinterpretedSubtransactions.exists {
          case (viewTree, (tx, metadata, _), _) =>
            viewTree.viewParticipantData.rootAction.command == command &&
            metadata.seeds.get(tx.roots(0)) == rootSeed
        }
        matchesView shouldBe true

        EitherT.rightT(
          ReInterpretationResult(
            transaction.unwrap,
            transaction.metadata,
            UsedPackages(Set.empty, Set.empty),
            LedgerTimeBoundaries.unconstrained,
          )
        )
      }
    }

    def withExternalCallResults(
        example: ExampleTransaction,
        nodeId: LfNodeId,
        results: ImmArray[ExternalCallResult],
    ): WellFormedTransaction[WellFormedTransaction.WithoutSuffixes] = {
      val transaction = example.versionedUnsuffixedTransaction
      val exercise = transaction.nodes(nodeId).asInstanceOf[LfNodeExercises]
      val updatedTransaction = CantonOnly.lfVersionedTransaction(
        nodes = transaction.nodes.updated(
          nodeId,
          exercise.copy(
            externalCallResults = results,
            version = LfSerializationVersion.VDev,
          ),
        ),
        roots = transaction.roots,
      )
      WellFormedTransaction.checkOrThrow(
        updatedTransaction,
        example.metadata,
        WellFormedTransaction.WithoutSuffixes,
        PathRollbackContextFactory,
      )
    }

    def viewsWithNoInputKeys(
        rootViews: Seq[FullTransactionViewTree]
    ): NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfGlobalKeyMapping)])]] =
      NonEmptyUtil.fromUnsafe(rootViews.map { viewTree =>
        // Include resolvers for all the subviews
        val resolvers =
          viewTree.view.allSubviewsWithPosition(viewTree.viewPosition).map { case (view, _) =>
            view -> (Map.empty: LfGlobalKeyMapping)
          }
        (viewTree, resolvers)
      })

    val transactionTreeFactory: TransactionTreeFactory =
      TransactionTreeFactory(
        ExampleTransactionFactory.submittingParticipant,
        factory.psid,
        factory.cantonContractIdVersion,
        factory.cryptoOps,
        TestContractHasher.Async,
        loggerFactory,
      )

    def reInterpret(
        mcc: ModelConformanceChecker,
        view: TransactionView,
        commonData: TransactionProcessingSteps.CommonData,
    ): EitherT[Future, Error, ConformanceReInterpretationResult] =
      mcc
        .reInterpret(
          view,
          commonData.ledgerTime,
          commonData.preparationTime,
          getEngineAbortStatus = () => EngineAbortStatus.notAborted,
          topologySnapshot = factory.topologySnapshot,
        )
        .failOnShutdown

    def check(
        mcc: ModelConformanceChecker,
        views: NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfGlobalKeyMapping)])]],
        ips: TopologySnapshot = factory.topologySnapshot,
        reInterpretedTopLevelViews: ModelConformanceChecker.LazyAsyncReInterpretationMap = Map.empty,
    ): EitherT[Future, ErrorWithSubTransaction[Unit], Result] = {
      val rootViewTrees = views.map(_._1)
      val commonData = TransactionProcessingSteps.tryCommonData(rootViewTrees)
      val rootViewTreesWithEffects =
        rootViewTrees.map(tree => (tree, tree.view.allSubviews.map(_ => ())))
      mcc
        .check(
          rootViewTreesWithEffects,
          ips,
          commonData,
          getEngineAbortStatus = () => EngineAbortStatus.notAborted,
          reInterpretedTopLevelViews,
          testedProtocolVersion,
        )
        .failOnShutdown
    }

    def buildUnderTest(reinterpretCommand: HasReinterpret): ModelConformanceChecker =
      buildUnderTestWithFactory(reinterpretCommand, transactionTreeFactory)

    def buildUnderTestWithFactory(
        reinterpretCommand: HasReinterpret,
        treeFactory: TransactionTreeFactory,
    ): ModelConformanceChecker =
      new ModelConformanceChecker(
        reinterpretCommand,
        treeFactory,
        submittingParticipant,
        ContractValidator.AllowAll,
        packageResolver,
        mock[ContractLookup],
        PositiveInt.tryCreate(100),
        validateLegacyContractsV11 = true,
        pureCrypto,
        loggerFactory,
      )

    s"A model conformance checker for version $contractIdVersion" when {
      val relevantExamples = factory.standardHappyCases.filter {
        // If the transaction is empty there is no transaction view message. Therefore, the checker is not invoked.
        case factory.EmptyTransaction => false
        case _ => true
      }

      forEvery(relevantExamples) { example =>
        s"checking $example" must {

          val sut = buildUnderTest(reinterpretExample(example))

          "yield the correct result" in {
            for {
              result <- valueOrFail(
                check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees))
              )(s"model conformance check for root views")
            } yield {
              val Result(updateId, absoluteTransaction, _) = result
              updateId should equal(example.updateId)

              absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
              absoluteTransaction.unwrap.version should equal(
                example.versionedSuffixedTransaction.version
              )
              assert(
                absoluteTransaction.withoutVersion.equalForest(
                  example.wellFormedSuffixedTransaction.withoutVersion
                ),
                s"\n$absoluteTransaction should equal\n${example.wellFormedSuffixedTransaction} up to nid renaming",
              )
            }
          }

          "re-use pre-interpreted transactions" in {
            val topLevelViewTrees = NonEmptyUtil.fromUnsafe(
              example.rootTransactionViewTrees
                .filter(_.isTopLevel)
            )
            val reInterpretedTopLevelViews = topLevelViewTrees.forgetNE
              .map({ viewTree =>
                viewTree.view.viewHash ->
                  Eval.now(
                    reInterpret(
                      sut,
                      viewTree.view,
                      TransactionProcessingSteps.tryCommonData(topLevelViewTrees),
                    ).mapK(FutureUnlessShutdown.outcomeK)
                  )
              })
              .toMap

            val reInterpreter = reinterpretExample(example)
            val mcc = buildUnderTest(reInterpreter)

            for {
              result <- valueOrFail(
                check(
                  mcc,
                  viewsWithNoInputKeys(example.rootTransactionViewTrees),
                  reInterpretedTopLevelViews = reInterpretedTopLevelViews,
                )
              )(s"model conformance check for root views")
            } yield {
              val Result(updateId, absoluteTransaction, _) = result
              updateId should equal(example.updateId)
              absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
              absoluteTransaction.unwrap.version should equal(
                example.versionedSuffixedTransaction.version
              )
              assert(
                absoluteTransaction.withoutVersion.equalForest(
                  example.wellFormedSuffixedTransaction.withoutVersion
                ),
                s"$absoluteTransaction should equal ${example.wellFormedSuffixedTransaction} up to nid renaming",
              )
              reInterpreter.getInterpretationCount shouldBe 0
            }
          }

          "reinterpret views individually" in {
            example.transactionViewTrees
              .parTraverse_ { viewTree =>
                for {
                  result <- valueOrFail(check(sut, viewsWithNoInputKeys(Seq(viewTree))))(
                    s"model conformance check for view at ${viewTree.viewPosition}"
                  )
                } yield {
                  val Result(updateId, absoluteTransaction, _) = result
                  updateId should equal(example.updateId)
                  absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
                }
              }
              .map(_ => succeed)
          }
        }
      }

      "reject views with tampered external-call metadata during reconstruction" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val externalCallResult = ExternalCallResult(
          extensionId = "extension",
          functionId = "function",
          config = Bytes.fromStringUtf8("config"),
          input = Bytes.fromStringUtf8("input"),
          output = Bytes.fromStringUtf8("output"),
        )
        val example = factory.SingleExercise(factory.deriveNodeSeed(0))
        val transaction =
          withExternalCallResults(example, LfNodeId(0), ImmArray(externalCallResult))
        val contractOfId: LfContractId => EitherT[
          FutureUnlessShutdown,
          TransactionTreeFactory.ContractLookupError,
          GenContractInstance,
        ] =
          cId =>
            EitherT.fromEither[FutureUnlessShutdown](
              example.inputContracts
                .get(cId)
                .toRight(
                  TransactionTreeFactory.ContractLookupError(cId, "Not found")
                )
            )

        val createTree = transactionTreeFactory.createTransactionTree(
          transaction = transaction,
          submitterInfo = DefaultParticipantStateValues.submitterInfo(List(submitter)),
          workflowId = None,
          mediator = factory.mediatorGroup,
          transactionSeed = factory.transactionSeed,
          transactionUuid = factory.transactionUuid,
          topologySnapshot = factory.topologySnapshot,
          contractOfId = contractOfId,
          maxSequencingTime = factory.ledgerTime.plusSeconds(100),
          validatePackageVettings = true,
        )

        for {
          genTree <- valueOrFail(createTree.failOnShutdown)("create transaction tree")
          fullTree = FullTransactionViewTree.tryCreate(genTree)
          tamperedTree = FullTransactionViewTree.Optics.tree
            .andThen(GenTransactionTree.rootViewsUnsafe)
            .andThen(
              MerkleSeq.Optics.toSeq[TransactionView](factory.cryptoOps, testedProtocolVersion)
            )
            .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
            .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
            .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
            .andThen(ViewParticipantData.Optics.externalCallResultsUnsafe)
            .modify(_.map(_.copy(checkingParties = Set.empty)))(fullTree)
          _ = tamperedTree.validated shouldBe Right(tamperedTree)
          result <- check(
            buildUnderTest(reinterpretTransaction(example, transaction)),
            viewsWithNoInputKeys(Seq(tamperedTree)),
            factory.topologySnapshot,
          ).value
        } yield inside(result) { case Left(ErrorWithSubTransaction(errors, _, _)) =>
          inside(errors.head) { case ViewReconstructionError(received, reconstructed) =>
            val receivedRecord =
              received.viewParticipantData.tryUnwrap.externalCallResults.loneElement
            val reconstructedRecord =
              reconstructed.viewParticipantData.tryUnwrap.externalCallResults.loneElement

            receivedRecord.checkingParties shouldBe Set.empty
            reconstructedRecord.checkingParties shouldBe Set(submitter)
          }
        }
      }

      "aggregate visible external-call results for replay" in {
        val devFactory = new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))(
          psid = factory.psid.copy(protocolVersion = ProtocolVersion.dev),
          cantonContractIdVersion = contractIdVersion,
        )
        val devTreeFactory = TransactionTreeFactory(
          ExampleTransactionFactory.submittingParticipant,
          devFactory.psid,
          devFactory.cantonContractIdVersion,
          devFactory.cryptoOps,
          TestContractHasher.Async,
          loggerFactory,
        )
        val externalCallResult = ExternalCallResult(
          extensionId = "extension",
          functionId = "function",
          config = Bytes.fromStringUtf8("config"),
          input = Bytes.fromStringUtf8("input"),
          output = Bytes.fromStringUtf8("output"),
        )
        val expectedStoredResults =
          DAMLe.StoredExternalCallResults.fromResults(Seq(externalCallResult))
        val example = devFactory.SingleExercise(devFactory.deriveNodeSeed(0))
        val transaction =
          withExternalCallResults(example, LfNodeId(0), ImmArray(externalCallResult))
        val contractOfId: LfContractId => EitherT[
          FutureUnlessShutdown,
          TransactionTreeFactory.ContractLookupError,
          GenContractInstance,
        ] =
          cId =>
            EitherT.fromEither[FutureUnlessShutdown](
              example.inputContracts
                .get(cId)
                .toRight(
                  TransactionTreeFactory.ContractLookupError(cId, "Not found")
                )
            )

        val createTree = devTreeFactory.createTransactionTree(
          transaction = transaction,
          submitterInfo = DefaultParticipantStateValues.submitterInfo(List(submitter)),
          workflowId = None,
          mediator = devFactory.mediatorGroup,
          transactionSeed = devFactory.transactionSeed,
          transactionUuid = devFactory.transactionUuid,
          topologySnapshot = devFactory.topologySnapshot,
          contractOfId = contractOfId,
          maxSequencingTime = devFactory.ledgerTime.plusSeconds(100),
          validatePackageVettings = true,
        )

        def treeWithCheckingParties(
            fullTree: FullTransactionViewTree,
            checkingParties: Set[LfPartyId],
        ): FullTransactionViewTree =
          FullTransactionViewTree.Optics.tree
            .andThen(GenTransactionTree.rootViewsUnsafe)
            .andThen(
              MerkleSeq.Optics.toSeq[TransactionView](devFactory.cryptoOps, ProtocolVersion.dev)
            )
            .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
            .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
            .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
            .andThen(ViewParticipantData.Optics.externalCallResultsUnsafe)
            .modify(_.map(_.copy(checkingParties = checkingParties)))(fullTree)

        def observedExternalCallArguments(
            fullTree: FullTransactionViewTree,
            checkingParties: Set[LfPartyId],
        ): Future[DAMLe.StoredExternalCallResults] = {
          val observed = new AtomicReference[
            Option[DAMLe.StoredExternalCallResults]
          ](None)
          val recordingReinterpreter = new HasReinterpret {
            override def reinterpret(
                contracts: ReplayContractLookup,
                contractAuthenticator: ContractAuthenticatorFn,
                submitters: Set[LfPartyId],
                command: LfCommand,
                topologySnapshot: TopologySnapshot,
                ledgerTime: CantonTimestamp,
                preparationTime: CantonTimestamp,
                rootSeed: Option[LfHash],
                packageResolution: Map[PackageName, PackageId],
                expectFailure: Boolean,
                getEngineAbortStatus: GetEngineAbortStatus,
                externalCallReplayData: () => FutureUnlessShutdown[DAMLe.ExternalCallReplayData],
            )(implicit traceContext: TraceContext): EitherT[
              FutureUnlessShutdown,
              DAMLe.ReinterpretationError,
              ReInterpretationResult,
            ] =
              EitherT.right[DAMLe.ReinterpretationError](externalCallReplayData()).flatMap {
                replayData =>
                  observed.set(Some(replayData.storedExternalCallResults))
                  reinterpretTransaction(example, transaction).reinterpret(
                    contracts,
                    contractAuthenticator,
                    submitters,
                    command,
                    topologySnapshot,
                    ledgerTime,
                    preparationTime,
                    rootSeed,
                    packageResolution,
                    expectFailure,
                    getEngineAbortStatus,
                    () => FutureUnlessShutdown.pure(replayData),
                  )
              }
          }

          val viewTree = treeWithCheckingParties(fullTree, checkingParties)
          buildUnderTestWithFactory(recordingReinterpreter, devTreeFactory)
            .reInterpret(
              view = viewTree.view,
              ledgerTime = viewTree.ledgerTime,
              preparationTime = viewTree.preparationTime,
              getEngineAbortStatus = () => EngineAbortStatus.notAborted,
              topologySnapshot = devFactory.topologySnapshot,
            )
            .value
            .failOnShutdown
            .map { result =>
              inside(result) { case Right(_) => succeed }
              observed.get().getOrElse(fail("Expected reinterpreter arguments to be recorded"))
            }
        }

        for {
          genTree <- valueOrFail(createTree.failOnShutdown)("create transaction tree")
          fullTree = FullTransactionViewTree.tryCreate(genTree)
          hostedPartyArguments <- observedExternalCallArguments(
            fullTree,
            Set(ExampleTransactionFactory.submitter),
          )
          unhostedPartyArguments <- observedExternalCallArguments(
            fullTree,
            Set(ExampleTransactionFactory.signatory),
          )
        } yield {
          hostedPartyArguments shouldBe expectedStoredResults
          unhostedPartyArguments shouldBe expectedStoredResults
        }
      }

    }
  }
}

object ExampleTransactionConformanceTest {
  private trait HashReInterpretationCounter {
    private val counter = new AtomicInteger(0)
    def incrementInterpretations(): Unit = discard(counter.getAndIncrement())
    def getInterpretationCount: Int = counter.get()
  }
}
