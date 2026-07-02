// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{TestHash, TestSalt}
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.data.{
  GenTransactionTree,
  RollbackContextFactory,
  TransactionViewDecompositionFactory,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  defaultTestingIdentityFactory,
  defaultTestingTopology,
}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  ResolvedPackagesAndDependencies,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TestContractHasher
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.Ref.{IdString, PackageId}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
final class NextGenTransactionTreeFactoryTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {

  private val rolledBackState =
    TransactionViewDecompositionFactory.RollbackState.empty.enterRollback
  private val rollbackContextFactory = RollbackContextFactory(testedProtocolVersion)
  private val rolledBackContext = rollbackContextFactory.fromRollbackState(rolledBackState)

  private def successfulLookup(example: ExampleTransaction): ContractInstanceOfId = id =>
    EitherT.fromEither[FutureUnlessShutdown](
      example.inputContracts
        .get(id)
        .toRight(ContractLookupError(id, "Unable to lookup input contract from test data"))
    )

  private def failedLookup(testErrorMessage: String): ContractInstanceOfId =
    id => EitherT.leftT(ContractLookupError(id, testErrorMessage))

  private val externalCallResult = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.fromStringUtf8("config"),
    input = Bytes.fromStringUtf8("input"),
    output = Bytes.fromStringUtf8("output"),
  )

  private def withExternalCallResults(
      example: ExampleTransaction,
      nodeId: LfNodeId,
      results: ImmArray[ExternalCallResult],
  ): WellFormedTransaction[WithoutSuffixes] =
    withExternalCallResults(
      example.versionedUnsuffixedTransaction,
      example.metadata,
      nodeId,
      results,
    )

  private def withExternalCallResults(
      example: ExampleTransaction,
      resultsByNode: Map[LfNodeId, ImmArray[ExternalCallResult]],
  ): WellFormedTransaction[WithoutSuffixes] = {
    val updatedNodes =
      resultsByNode.foldLeft(example.versionedUnsuffixedTransaction.nodes) {
        case (nodes, (nodeId, results)) =>
          val exercise = nodes(nodeId).asInstanceOf[LfNodeExercises]
          nodes.updated(
            nodeId,
            exercise.copy(
              externalCallResults = results,
              version = LfSerializationVersion.VDev,
            ),
          )
      }
    val updatedTransaction = CantonOnly.lfVersionedTransaction(
      nodes = updatedNodes,
      roots = example.versionedUnsuffixedTransaction.roots,
    )
    WellFormedTransaction.checkOrThrow(
      updatedTransaction,
      example.metadata,
      WithoutSuffixes,
      rollbackContextFactory,
    )
  }

  private def withExternalCallResults(
      transaction: LfVersionedTransaction,
      metadata: TransactionMetadata,
      nodeId: LfNodeId,
      results: ImmArray[ExternalCallResult],
      updateExercise: LfNodeExercises => LfNodeExercises = identity,
  ): WellFormedTransaction[WithoutSuffixes] = {
    val exercise = updateExercise(transaction.nodes(nodeId).asInstanceOf[LfNodeExercises])
    val updatedExercise = exercise.copy(
      externalCallResults = results,
      version = LfSerializationVersion.VDev,
    )
    val updatedTransaction = CantonOnly.lfVersionedTransaction(
      nodes = transaction.nodes.updated(nodeId, updatedExercise),
      roots = transaction.roots,
    )
    WellFormedTransaction.checkOrThrow(
      updatedTransaction,
      metadata,
      WithoutSuffixes,
      rollbackContextFactory,
    )
  }

  private val testedContractIdVersions: Seq[CantonContractIdVersion] =
    if (testedProtocolVersion >= ProtocolVersion.v35) CantonContractIdVersion.all else Seq.empty

  forAll(Table("contract id version", testedContractIdVersions*)) { contractIdVersion =>
    val factory: ExampleTransactionFactory = new ExampleTransactionFactory(
      versionOverride = Some(testedProtocolVersion)
    )(cantonContractIdVersion = contractIdVersion)

    s"TransactionTreeFactoryImpl for contract ID version $contractIdVersion" should {

      def createTransactionTreeFactory(): TransactionTreeFactory =
        new NextGenTransactionTreeFactory(
          ExampleTransactionFactory.submittingParticipant,
          factory.psid,
          factory.cantonContractIdVersion,
          factory.cryptoOps,
          TestContractHasher.Async,
          loggerFactory,
        )

      def createTransactionTree(
          treeFactory: TransactionTreeFactory,
          transaction: WellFormedTransaction[WithoutSuffixes],
          contractInstanceOfId: ContractInstanceOfId,
          actAs: List[LfPartyId] = List(ExampleTransactionFactory.submitter),
          snapshot: TopologySnapshot = factory.topologySnapshot,
      ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree] = {
        val submitterInfo = DefaultParticipantStateValues.submitterInfo(actAs)
        treeFactory
          .createTransactionTree(
            transaction = transaction,
            submitterInfo = submitterInfo,
            workflowId = Some(WorkflowId.assertFromString("testWorkflowId")),
            mediator = factory.mediatorGroup,
            transactionSeed = factory.transactionSeed,
            transactionUuid = factory.transactionUuid,
            topologySnapshot = snapshot,
            contractOfId = contractInstanceOfId,
            maxSequencingTime = factory.ledgerTime.plusSeconds(100),
            validatePackageVettings = true,
          )
          .failOnShutdown
      }

      "A transaction tree factory" when {

        "everything is ok" must {
          forEvery(factory.standardHappyCases) { example =>
            lazy val treeFactory = createTransactionTreeFactory()

            s"create the correct views for: $example" in {
              createTransactionTree(
                treeFactory,
                example.wellFormedUnsuffixedTransaction,
                successfulLookup(example),
              ).value.flatMap(_ should equal(Right(example.transactionTree)))
            }
          }

          "record external call results from same-view exercise nodes with view-local occurrence indexes" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
            val treeFactory = createTransactionTreeFactory()
            val example = factory.MultipleRootsAndSimpleViewNesting
            val rootExerciseNodeId = LfNodeId(1)
            val sameViewExerciseNodeId = LfNodeId(5)
            val rootCall = externalCallResult.copy(functionId = "root-function")
            val sameViewCall = externalCallResult.copy(functionId = "same-view-function")

            createTransactionTree(
              treeFactory,
              withExternalCallResults(
                example,
                Map(
                  rootExerciseNodeId -> ImmArray(rootCall),
                  sameViewExerciseNodeId -> ImmArray(sameViewCall),
                ),
              ),
              successfulLookup(example),
            ).value.map { result =>
              val tree = result.value
              tree.rootViews.unblindedElements should have size 2
              val view1 = tree.rootViews.unblindedElements.drop(1).headOption.value
              val records = view1.viewParticipantData.tryUnwrap.externalCallResults

              records.map(record =>
                (record.result, record.exerciseIndex, record.callIndex, record.checkingParties)
              ) shouldBe Seq(
                (
                  rootCall,
                  NonNegativeInt.zero,
                  NonNegativeInt.zero,
                  Set(
                    ExampleTransactionFactory.signatory,
                    ExampleTransactionFactory.submitter,
                  ),
                ),
                (
                  sameViewCall,
                  NonNegativeInt.one,
                  NonNegativeInt.zero,
                  Set(
                    ExampleTransactionFactory.signatory,
                    ExampleTransactionFactory.submitter,
                  ),
                ),
              )
            }
          }

          "record repeated identical external call results on one exercise node with increasing call indexes" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
            val treeFactory = createTransactionTreeFactory()
            val example = factory.MultipleRootsAndSimpleViewNesting
            val nodeId = LfNodeId(5)

            createTransactionTree(
              treeFactory,
              withExternalCallResults(
                example,
                nodeId,
                ImmArray(externalCallResult, externalCallResult),
              ),
              successfulLookup(example),
            ).value.map { result =>
              val tree = result.value
              val view1 = tree.rootViews.unblindedElements.drop(1).headOption.value
              val records = view1.viewParticipantData.tryUnwrap.externalCallResults

              records should have size 2
              records.map(_.result) shouldBe Seq(externalCallResult, externalCallResult)
              records.map(_.exerciseIndex).toSet shouldBe Set(NonNegativeInt.tryCreate(1))
              records.map(_.callIndex) shouldBe Seq(NonNegativeInt.zero, NonNegativeInt.one)
              records.map(_.checkingParties).toSet shouldBe Set(
                Set(
                  ExampleTransactionFactory.signatory,
                  ExampleTransactionFactory.submitter,
                )
              )
            }
          }

          "record external call results from child views only once" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
            val treeFactory = createTransactionTreeFactory()
            val example = factory.ViewInterleavings
            val externalCallNodeId = LfNodeId(2)

            createTransactionTree(
              treeFactory,
              withExternalCallResults(example, externalCallNodeId, ImmArray(externalCallResult)),
              successfulLookup(example),
            ).value.map { result =>
              val tree = result.value
              tree.rootViews.unblindedElements should have size 3
              val parentView = tree.rootViews.unblindedElements.drop(1).headOption.value
              val childView = parentView.subviews.unblindedElements.headOption.value

              parentView.viewParticipantData.tryUnwrap.externalCallResults shouldBe Seq.empty
              val record =
                childView.viewParticipantData.tryUnwrap.externalCallResults.loneElement

              record.result shouldBe externalCallResult
              record.exerciseIndex shouldBe NonNegativeInt.zero
              record.callIndex shouldBe NonNegativeInt.zero
              record.checkingParties shouldBe Set(ExampleTransactionFactory.signatory)
            }
          }

          "preserve distinct same-result external call occurrences across parent and child views" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
            val treeFactory = createTransactionTreeFactory()
            val example = factory.TransientContracts
            val childExternalCallNodeId = LfNodeId(3)
            val parentExternalCallNodeId = LfNodeId(5)

            createTransactionTree(
              treeFactory,
              withExternalCallResults(
                example,
                Map(
                  childExternalCallNodeId -> ImmArray(externalCallResult),
                  parentExternalCallNodeId -> ImmArray(externalCallResult),
                ),
              ),
              successfulLookup(example),
            ).value.map { result =>
              val tree = result.value
              tree.rootViews.unblindedElements should have size 2
              val parentView = tree.rootViews.unblindedElements.drop(1).headOption.value
              val childView = parentView.subviews.unblindedElements.loneElement

              val parentRecord =
                parentView.viewParticipantData.tryUnwrap.externalCallResults.loneElement
              parentRecord.result shouldBe externalCallResult
              parentRecord.exerciseIndex shouldBe NonNegativeInt.tryCreate(1)
              parentRecord.callIndex shouldBe NonNegativeInt.zero
              parentRecord.checkingParties shouldBe Set(ExampleTransactionFactory.submitter)

              val childRecord =
                childView.viewParticipantData.tryUnwrap.externalCallResults.loneElement
              childRecord.result shouldBe externalCallResult
              childRecord.exerciseIndex shouldBe NonNegativeInt.zero
              childRecord.callIndex shouldBe NonNegativeInt.zero
              childRecord.checkingParties shouldBe Set(
                ExampleTransactionFactory.submitter,
                ExampleTransactionFactory.signatory,
              )
            }
          }

          "record external call checking parties from signatories and actors" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
            val treeFactory = createTransactionTreeFactory()
            val example = factory.SingleExerciseWithNonstakeholderActor(factory.deriveNodeSeed(0))

            createTransactionTree(
              treeFactory,
              withExternalCallResults(example, LfNodeId(0), ImmArray(externalCallResult)),
              successfulLookup(example),
            ).value.map { result =>
              val tree = result.value
              val view = tree.rootViews.unblindedElements.loneElement
              val record =
                view.viewParticipantData.tryUnwrap.externalCallResults.loneElement

              record.checkingParties shouldBe Set(
                ExampleTransactionFactory.signatory,
                ExampleTransactionFactory.submitter,
              )
            }
          }

          "reconstruct non-root same-view external call records with view-local exercise indexes" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
            val submittingTreeFactory = createTransactionTreeFactory()
            val validatingTreeFactory = createTransactionTreeFactory()
            val example = factory.MultipleRootsAndSimpleViewNesting
            val externalCallNodeId = LfNodeId(5)
            val transaction =
              withExternalCallResults(example, externalCallNodeId, ImmArray(externalCallResult))
            val (_, (reinterpretedTx, reinterpretedMetadata, _), _) =
              example.reinterpretedSubtransactions(1)
            val reinterpretedTransaction = withExternalCallResults(
              reinterpretedTx,
              reinterpretedMetadata,
              externalCallNodeId,
              ImmArray(externalCallResult),
            )

            createTransactionTree(
              submittingTreeFactory,
              transaction,
              successfulLookup(example),
            ).value.flatMap { result =>
              val tree = result.value
              val submittedView = tree.rootViews.unblindedElements.drop(1).headOption.value

              validatingTreeFactory
                .tryReconstruct(
                  transaction = reinterpretedTransaction,
                  rootPosition = tree.viewPosition(submittedView.viewHash.toRootHash).value,
                  mediator = factory.mediatorGroup,
                  submittingParticipantO = Some(ExampleTransactionFactory.submittingParticipant),
                  salts = submittingTreeFactory.saltsFromView(submittedView),
                  transactionUuid = factory.transactionUuid,
                  topologySnapshot = factory.topologySnapshot,
                  contractOfId = successfulLookup(example),
                  rbContext = rollbackContextFactory.empty,
                  absolutizer = factory.absolutizer(tree.updateId),
                )
                .failOnShutdown
                .value
                .map { reconstruction =>
                  val (reconstructedView, _) = reconstruction.value
                  val record =
                    reconstructedView.viewParticipantData.tryUnwrap.externalCallResults.loneElement

                  reconstructedView shouldBe submittedView
                  record.exerciseIndex shouldBe NonNegativeInt.tryCreate(1)
                }
            }
          }

        }

        "a contract lookup fails" must {
          lazy val errorMessage = "Test error message"
          lazy val treeFactory = createTransactionTreeFactory()

          lazy val example = factory.SingleExercise(
            factory.deriveNodeSeed(0)
          ) // pick an example that needs divulgence of absolute ids

          "reject the input" in {
            createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              failedLookup(errorMessage),
            ).value.flatMap(
              _ shouldEqual Left(
                ContractLookupError(example.absolutizedContractId, errorMessage)
              )
            )
          }
        }

        "empty actAs set is empty" must {
          lazy val treeFactory = createTransactionTreeFactory()

          "reject the input" in {
            val example = factory.standardHappyCases.headOption.value
            createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              actAs = List.empty,
            ).value
              .flatMap(
                _ should equal(Left(SubmitterMetadataError("The actAs set must not be empty.")))
              )
          }
        }

        "checking package vettings" must {
          lazy val treeFactory = createTransactionTreeFactory()
          "fail if the main package is not vetted" in {
            val example = factory.standardHappyCases(2)
            createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              snapshot = defaultTestingTopology.withPackages(Map.empty).build().topologySnapshot(),
            ).value.flatMap(_ should matchPattern { case Left(UnknownPackageError(_)) => })
          }

          "accept a package if it has a non-vetted dependency" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
            val example = factory.standardHappyCases(2)
            createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              snapshot = defaultTestingIdentityFactory.topologySnapshot(
                packageDependencyResolver = TestPackageDependencyResolver
              ),
            ).value.flatMap(_ should equal(Right(example.transactionTree)))
          }

          "fail gracefully if the present participant is misconfigured and somehow doesn't have a package that it should have" in {
            val example = factory.standardHappyCases(2)
            for {
              err <- createTransactionTree(
                treeFactory,
                example.wellFormedUnsuffixedTransaction,
                successfulLookup(example),
                snapshot = defaultTestingIdentityFactory.topologySnapshot(
                  packageDependencyResolver = MisconfiguredPackageDependencyResolver
                ),
              ).value
            } yield {
              inside(err) { case Left(UnknownPackageError(unknownTo)) =>
                forEvery(unknownTo) {
                  _.packageId shouldBe ExampleTransactionFactory.packageId
                }
                unknownTo should not be empty
              }
            }
          }
        }

        "an effectful rollback is encountered" must {
          val treeFactory = createTransactionTreeFactory()
          val create = ExampleContractFactory
            .build()
            .toLf
            .copy(coid = contractIdVersion match {
              case _: CantonContractIdV1Version => LfContractId.V1(ExampleContractFactory.lfHash(1))
              case _: CantonContractIdV2Version =>
                LfContractId.V2(ExampleContractFactory.lfHash(1).bytes, Bytes.Empty)
            })

          "reject the transaction" in {
            val tx = TreeTransactionBuilder.toVersionedTransaction(create)
            val meta = TransactionMetadata(
              ledgerTime = factory.ledgerTime,
              preparationTime = factory.ledgerTime,
              seeds = tx.nodes.view.mapValues(_ => ExampleContractFactory.lfHash(2)).toMap,
            )
            val wft = WellFormedTransaction.createUnsafe(tx, meta)

            val expected = ViewPosition(List(MerkleSeqIndex(List(Direction.Left, Direction.Right))))
            treeFactory
              .tryReconstruct(
                transaction = wft,
                rootPosition = expected,
                mediator = factory.mediatorGroup,
                submittingParticipantO = Some(ExampleTransactionFactory.submittingParticipant),
                salts = Iterator.range(0, 2).map(TestSalt.generateSalt).to(Iterable),
                transactionUuid = factory.transactionUuid,
                topologySnapshot = factory.topologySnapshot,
                contractOfId = cid => EitherT.leftT(ContractLookupError(cid, "")),
                rbContext = rolledBackContext,
                absolutizer = factory.absolutizer(UpdateId(TestHash.digest(1))),
              )
              .value
              .map { result =>
                inside(result) { case Left(RolledBackEffect(actual)) =>
                  actual shouldBe expected
                }
              }
              .failOnShutdown
          }
        }
      }

    }
  }

  object TestPackageDependencyResolver extends PackageDependencyResolver {
    val exampleDependency: IdString.PackageId = PackageId.assertFromString("example-dependency")
    override def resolvePackagesAndDependencies(packages: Set[PackageId])(implicit
        traceContext: TraceContext
    ): Either[(ParticipantId, Set[PackageId]), ResolvedPackagesAndDependencies] =
      if (packages.contains(ExampleTransactionFactory.packageId))
        Right(ResolvedPackagesAndDependencies(packages, Set(exampleDependency)))
      else Right(ResolvedPackagesAndDependencies(packages, Set.empty))
  }

  object MisconfiguredPackageDependencyResolver extends PackageDependencyResolver {
    private val participantId = ParticipantId("MisconfiguredPackageDependencyResolver")

    override def resolvePackagesAndDependencies(packages: Set[PackageId])(implicit
        traceContext: TraceContext
    ): Either[(ParticipantId, Set[PackageId]), ResolvedPackagesAndDependencies] = Left(
      participantId -> packages
    )
  }
}
