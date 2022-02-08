// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import java.time.Duration

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import com.daml.lf.data.ImmArray
import com.daml.lf.engine
import com.digitalasset.canton.data.{CantonTimestamp, TransactionViewTree}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactoryImpl
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker._
import com.digitalasset.canton.participant.store.ContractLookup
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfCommand, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
class ModelConformanceCheckerTest extends AsyncWordSpec with BaseTest {

  implicit val ec: ExecutionContext = directExecutionContext

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  val sequencerTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(0)

  val ledgerTimeRecordTimeTolerance: Duration = Duration.ofSeconds(10)

  def reinterpret(example: ExampleTransaction)(
      contracts: ContractLookup,
      submitters: Set[LfPartyId],
      cmd: LfCommand,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      inRollback: Boolean,
      traceContext: TraceContext,
  ): EitherT[Future, DAMLeError, (LfVersionedTransaction, TransactionMetadata)] = {

    ledgerTime shouldEqual factory.ledgerTime
    submissionTime shouldEqual factory.submissionTime

    val (_viewTree, (reinterpretedTx, metadata), _witnesses) =
      example.reinterpretedSubtransactions.find { case (viewTree, (tx, md), _) =>
        viewTree.viewParticipantData.rootAction.command == cmd &&
          // Commands are otherwise not sufficiently unique (whereas with nodes, we can produce unique nodes, e.g.
          // based on LfNodeCreate.agreementText not part of LfCreateCommand.
          rootSeed == md.seeds.get(tx.roots(0))
      }.value

    EitherT.rightT[Future, DAMLeError](reinterpretedTx -> metadata)
  }

  def viewsWithNoInputKeys(
      rootViews: Seq[TransactionViewTree]
  ): NonEmptyList[(TransactionViewTree, Map[LfGlobalKey, Option[LfContractId]])] =
    NonEmptyList.fromListUnsafe(
      rootViews.map(_ -> Map.empty[LfGlobalKey, Option[LfContractId]]).toList
    )

  val transactionTreeFactory: TransactionTreeFactoryImpl =
    new TransactionTreeFactoryImpl(
      ExampleTransactionFactory.submitterParticipant,
      factory.domainId,
      ExampleTransactionFactory.asSerializableRaw,
      ExampleTransactionFactory.defaultPackageInfoService,
      factory.cryptoOps,
      loggerFactory,
    )

  def check(
      mcc: ModelConformanceChecker,
      views: NonEmptyList[(TransactionViewTree, Map[LfGlobalKey, Option[LfContractId]])],
      ips: TopologySnapshot = factory.topologySnapshot,
  ): EitherT[Future, Error, Result] = {
    val rootViewTrees = views.map(_._1)
    val commonData = TransactionProcessingSteps.tryCommonData(rootViewTrees)
    mcc.check(views, 0L, ips, commonData)
  }

  "A model conformance checker" when {
    val relevantExamples = factory.standardHappyCases.filter {
      // If the transaction is empty there is no transaction view message. Therefore, the checker is not invoked.
      case factory.EmptyTransaction => false
      case _ => true
    }

    forEvery(relevantExamples) { example =>
      s"checking $example" must {

        val sut =
          new ModelConformanceChecker(reinterpret(example), transactionTreeFactory, loggerFactory)

        "yield the correct result" in {
          for {
            result <- valueOrFail(
              check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees))
            )(s"model conformance check for root views")
          } yield {
            val Result(transactionId, absoluteTransaction) = result
            transactionId should equal(example.transactionId)
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
          }
        }

        "reinterpret views individually" in {
          example.transactionViewTrees
            .traverse_ { viewTree =>
              for {
                result <- valueOrFail(check(sut, viewsWithNoInputKeys(Seq(viewTree))))(
                  s"model conformance check for view at ${viewTree.viewPosition}"
                )
              } yield {
                val Result(transactionId, absoluteTransaction) = result
                transactionId should equal(example.transactionId)
                absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
              }
            }
            .map(_ => succeed)
        }
      }
    }

    "transaction id is inconsistent" must {
      val sut = new ModelConformanceChecker(
        (_, _, _, _, _, _, _, _) => throw new UnsupportedOperationException(),
        transactionTreeFactory,
        loggerFactory,
      )

      val singleCreate = factory.SingleCreate(seed = ExampleTransactionFactory.lfHash(0))
      val viewTreesWithInconsistentTransactionIds = Seq(
        factory.MultipleRootsAndViewNestings.rootTransactionViewTrees.head,
        singleCreate.rootTransactionViewTrees.head,
      )

      "yield an error" in {
        assertThrows[IllegalArgumentException] {
          check(sut, viewsWithNoInputKeys(viewTreesWithInconsistentTransactionIds))
        }
      }
    }

    "reinterpretation fails" must {
      val error = DAMLeError(mock[engine.Error])
      val sut = new ModelConformanceChecker(
        (_, _, _, _, _, _, _, _) =>
          EitherT.leftT[Future, (LfVersionedTransaction, TransactionMetadata)](error),
        transactionTreeFactory,
        loggerFactory,
      )
      val example = factory.MultipleRootsAndViewNestings

      "yield an error" in {
        for {
          failure <- leftOrFail(
            check(
              sut,
              viewsWithNoInputKeys(example.rootTransactionViewTrees),
            )
          )("reinterpretation fails")
        } yield failure shouldBe error
      }
    }

    "differences in the reconstructed transaction must yield an error" should {
      import ExampleTransactionFactory._
      "subview missing" in {
        val subviewMissing = factory.SingleExercise(lfHash(0))
        val reinterpreted = transaction(
          Seq(0),
          subviewMissing.reinterpretedNode.copy(children = ImmArray(LfNodeId(1))),
          fetchNode(
            subviewMissing.contractId,
            actingParties = Set(submitter),
            signatories = Set(submitter),
          ),
        )
        val sut = new ModelConformanceChecker(
          (_, _, _, _, _, _, _, _) =>
            EitherT.pure[Future, DAMLeError](reinterpreted -> subviewMissing.metadata),
          transactionTreeFactory,
          loggerFactory,
        )
        for {
          result <- leftOrFail(
            check(sut, viewsWithNoInputKeys(subviewMissing.rootTransactionViewTrees))
          )("detect missing subview")
        } yield result shouldBe a[TransactionTreeError]
      }

      /* TODO(#3202) further error cases to test:
       * - extra subview
       * - input contract not declared
       * - extra input contract
       * - input contract with wrong contract data
       * - missing created contract
       * - extra created contract
       * - wrong discriminator of created contract
       * - wrong unicum of created contract
       * - wrong data for created contract
       */
    }
  }
}
