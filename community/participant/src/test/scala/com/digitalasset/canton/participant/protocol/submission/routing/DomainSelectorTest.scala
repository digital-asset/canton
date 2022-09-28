// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.test.TransactionBuilder.Implicits._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.{
  ExerciseByInterface,
  ThreeExercises,
}
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture._
import com.digitalasset.canton.participant.protocol.submission.DomainUsabilityChecker.{
  DomainNotSupportingMinimumProtocolVersion,
  UnknownPackage,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedDomainId
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.NoDomainForSubmission
import com.digitalasset.canton.participant.sync.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.participant.sync.{
  TransactionRoutingError,
  TransactionRoutingErrorWithDomain,
}
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  ExampleTransactionFactory,
  LfContractId,
  LfTransactionVersion,
  LfVersionedTransaction,
  WithContractMetadata,
}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  DomainAlias,
  HasExecutionContext,
  LfPackageId,
  LfWorkflowId,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class DomainSelectorTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  implicit class RichEitherT[A](val e: EitherT[Future, TransactionRoutingError, A]) {
    def leftValue: TransactionRoutingError = e.value.futureValue.left.value
  }

  implicit val _loggerFactor = loggerFactory

  /*
    Topology:
      - participants: submitter and observer, each hosting submitter party and observer party, respectively
      - packages are vetted
      - domain: da by default
        - input contract is on da
        - participants are connected to da
        - domainsOfSubmittersAndInformees is Set(da)

    Transaction:
      - exercise by interface
      - one contract as input
   */
  "DomainSelector (simple exercise by interface)" should {
    import DomainSelectorTest._
    import DomainSelectorTest.ForSimpleTopology._
    import SimpleTopology._

    implicit val _loggerFactor = loggerFactory

    val defaultDomainRank = DomainRank(Map.empty, 0, da)

    def transfersDaToAcme(contracts: Set[LfContractId]) = DomainRank(
      transfers = contracts.map(_ -> (signatory, da)).toMap, // current domain is da
      priority = 0,
      domainId = acme, // transfer to acme
    )

    "return correct value in happy path" in {
      val selector = selectorForExerciseByInterface()

      selector.forSingleDomain.futureValue shouldBe defaultDomainRank
      selector.forMultiDomain.futureValue shouldBe defaultDomainRank
    }

    "return an error when not connected to the domain" in {
      val selector = selectorForExerciseByInterface(connectedDomains = Set())
      val expected = TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(da)

      // Single domain
      selector.forSingleDomain.leftValue shouldBe expected

      // Multi domain
      selector.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(da -> expected.toString)
      )
    }

    "return proper response when submitters or informees are hosted on the wrong domain" in {
      val selector = selectorForExerciseByInterface(
        domainsOfSubmittersAndInformees = NonEmpty.mk(Set, acme), // different than da
        connectedDomains = Set(acme, da),
      )

      // Single domain: failure
      selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
        .NotAllInformeeAreOnDomain(
          da,
          domainsOfAllInformee = NonEmpty.mk(Set, acme),
        )

      // Multi domain: transfer proposal (da -> acme)
      val domainRank = transfersDaToAcme(selector.inputContractIds)
      selector.forMultiDomain.futureValue shouldBe domainRank

      // Multi domain, missing connection to acme: error
      val selectorMissingConnection = selectorForExerciseByInterface(
        domainsOfSubmittersAndInformees = NonEmpty.mk(Set, acme), // different than da
        connectedDomains = Set(da),
      )

      selectorMissingConnection.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(acme -> TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(acme).toString)
      )
    }

    "take priority into account (multi domain setting)" in {
      def pickDomain(bestDomain: DomainId): DomainId = selectorForExerciseByInterface(
        // da is not in the list to force transfer
        domainsOfSubmittersAndInformees = NonEmpty.mk(Set, acme, repair),
        connectedDomains = Set(acme, da, repair),
        priorityOfDomain = d => if (d == bestDomain) 10 else 0,
      ).forMultiDomain.futureValue.domainId

      pickDomain(acme) shouldBe acme
      pickDomain(repair) shouldBe repair
    }

    "take minimum protocol version into account" in {
      val oldPV = ProtocolVersion.v3
      val newPV = ProtocolVersion.dev
      val transactionVersion = LfTransactionVersion.VDev

      val selectorOldPV = selectorForExerciseByInterface(
        transactionVersion = LanguageVersion.v1_dev, // requires protocol version dev
        domainProtocolVersion = _ => oldPV,
      )

      // Domain protocol version is too low
      val expectedError = DomainNotSupportingMinimumProtocolVersion(
        domainId = da,
        currentPV = oldPV,
        requiredPV = newPV,
        lfVersion = transactionVersion,
      )

      selectorOldPV.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId.Generic(
        da,
        expectedError.toString,
      )

      selectorOldPV.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(da -> expectedError.toString)
      )

      // Happy path
      val selectorNewPV = selectorForExerciseByInterface(
        transactionVersion = LanguageVersion.v1_dev, // requires protocol version dev
        domainProtocolVersion = _ => newPV,
      )

      selectorNewPV.forSingleDomain.futureValue shouldBe defaultDomainRank
      selectorNewPV.forMultiDomain.futureValue shouldBe defaultDomainRank
    }

    "refuse to route to a domain with missing package vetting" in {
      val missingPackage = ExerciseByInterface.interfacePackageId

      val selector = selectorForExerciseByInterface(
        vettedPackages = ExerciseByInterface.correctPackages.filterNot(_ == missingPackage)
      )

      val expectedError = UnknownPackage(
        da,
        Set(
          unknownPackageFor(submitterParticipantId, missingPackage),
          unknownPackageFor(observerParticipantId, missingPackage),
        ),
      )

      selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId.Generic(
        da,
        expectedError.toString,
      )

      selector.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(da -> expectedError.toString)
      )
    }

    "a domain is prescribed" when {
      "return correct response when prescribed domain is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("da")
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return an error when prescribed domain is incorrect" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("acme"),
          connectedDomains = Set(acme, da),
        )

        // Single domain: prescribed domain should be domain of input contract
        selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
          .InputContractsNotOnDomain(
            domainId = acme,
            inputContractDomain = da,
          )

        // Multi domain
        selector.forMultiDomain.leftValue shouldBe InvalidPrescribedDomainId
          .NotAllInformeeAreOnDomain(
            acme,
            domainsOfAllInformee = NonEmpty.mk(Set, da),
          )
      }

      "propose transfers when needed" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("acme"),
          connectedDomains = Set(acme, da),
          domainsOfSubmittersAndInformees = NonEmpty.mk(Set, acme, da),
        )

        // Single domain: prescribed domain should be domain of input contract
        selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
          .InputContractsNotOnDomain(
            domainId = acme,
            inputContractDomain = da,
          )

        // Multi domain: transfer proposal (da -> acme)
        val domainRank = transfersDaToAcme(selector.inputContractIds)
        selector.forMultiDomain.futureValue shouldBe domainRank
      }
    }
  }

  /*
    Topology:
      - participants: submitter and observer, each hosting submitter party and observer party, respectively
        - packages are vetted
        - domain: da by default
          - input contract is on da
          - participants are connected to da
          - domainsOfSubmittersAndInformees is Set(da)

    Transaction:
      - three exercises
      - three contracts as input
   */
  "DomainSelector (simple transaction with three input contracts)" should {
    import DomainSelectorTest._
    import DomainSelectorTest.ForSimpleTopology._
    import SimpleTopology._

    "minimize the number of transfers" in {
      import ThreeExercises._

      val domains = NonEmpty.mk(Set, acme, da, repair)

      def selectDomain(domainOfContracts: Map[LfContractId, DomainId]): DomainRank =
        selectorForThreeExercises(
          connectedDomains = domains,
          domainsOfSubmittersAndInformees = domains,
          domainOfContracts = _ => domainOfContracts,
        ).forMultiDomain.futureValue

      /*
        Two contracts on acme, one on repair
        Expected: transfer to acme
       */
      {
        val domainsOfContracts =
          Map(inputContract1Id -> acme, inputContract2Id -> acme, inputContract3Id -> repair)

        val expectedDomainRank = DomainRank(
          transfers = Map(inputContract3Id -> (signatory, repair)),
          priority = 0,
          domainId = acme, // transfer to acme
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

      /*
        Two contracts on repair, one on acme
        Expected: transfer to repair
       */
      {
        val domainsOfContracts =
          Map(inputContract1Id -> acme, inputContract2Id -> repair, inputContract3Id -> repair)

        val expectedDomainRank = DomainRank(
          transfers = Map(inputContract1Id -> (signatory, acme)),
          priority = 0,
          domainId = repair, // transfer to repair
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

    }
  }
}

private[routing] object DomainSelectorTest {
  private def createDomainId(alias: String): DomainId = DomainId(
    UniqueIdentifier(Identifier.tryCreate(alias), DefaultTestIdentities.namespace)
  )

  val da = createDomainId("da")
  val acme = createDomainId("acme")
  val repair = createDomainId("repair")

  object ForSimpleTopology {
    import SimpleTopology._

    private val defaultDomainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
      contracts => contracts.map(_ -> da).toMap

    private val defaultPriorityOfDomain: DomainId => Int = _ => 0

    private val defaultDomain: DomainId = da

    private val defaultDomainsOfSubmittersAndInformees: NonEmpty[Set[DomainId]] =
      NonEmpty.mk(Set, da)

    private val defaultPrescribedDomainAlias: Option[String] = None

    private val defaultDomainProtocolVersion: DomainId => ProtocolVersion = _ => ProtocolVersion.v3

    private val defaultTransactionVersion: LanguageVersion = LanguageVersion.v1_14

    def selectorForExerciseByInterface(
        priorityOfDomain: DomainId => Int = defaultPriorityOfDomain,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
          defaultDomainOfContracts,
        connectedDomains: Set[DomainId] = Set(defaultDomain),
        domainsOfSubmittersAndInformees: NonEmpty[Set[DomainId]] =
          defaultDomainsOfSubmittersAndInformees,
        prescribedDomainAlias: Option[String] = defaultPrescribedDomainAlias,
        domainProtocolVersion: DomainId => ProtocolVersion = defaultDomainProtocolVersion,
        transactionVersion: LanguageVersion = defaultTransactionVersion,
        vettedPackages: Seq[LfPackageId] = ExerciseByInterface.correctPackages,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val exerciseByInterface = ExerciseByInterface(transactionVersion)

      val inputContractsMetadata = Set(
        WithContractMetadata[LfContractId](
          exerciseByInterface.inputContractId,
          ContractMetadata.tryCreate(
            signatories = Set(signatory),
            stakeholders = Set(signatory, observer),
            maybeKeyWithMaintainers = None,
          ),
        )
      )

      new Selector(loggerFactory)(
        priorityOfDomain,
        domainOfContracts,
        connectedDomains,
        domainsOfSubmittersAndInformees,
        prescribedDomainAlias,
        domainProtocolVersion,
        transactionVersion,
        vettedPackages,
        exerciseByInterface.tx,
        inputContractsMetadata,
      )
    }

    def selectorForThreeExercises(
        priorityOfDomain: DomainId => Int = defaultPriorityOfDomain,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
          defaultDomainOfContracts,
        connectedDomains: Set[DomainId] = Set(defaultDomain),
        domainsOfSubmittersAndInformees: NonEmpty[Set[DomainId]] =
          defaultDomainsOfSubmittersAndInformees,
        prescribedDomainAlias: Option[String] = defaultPrescribedDomainAlias,
        domainProtocolVersion: DomainId => ProtocolVersion = defaultDomainProtocolVersion,
        transactionVersion: LanguageVersion = defaultTransactionVersion,
        vettedPackages: Seq[LfPackageId] = ExerciseByInterface.correctPackages,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val inputContractsMetadata = ThreeExercises.inputContractIds.map { inputContractId =>
        WithContractMetadata[LfContractId](
          inputContractId,
          ContractMetadata.tryCreate(
            signatories = Set(signatory),
            stakeholders = Set(signatory, observer),
            maybeKeyWithMaintainers = None,
          ),
        )
      }

      new Selector(loggerFactory)(
        priorityOfDomain,
        domainOfContracts,
        connectedDomains,
        domainsOfSubmittersAndInformees,
        prescribedDomainAlias,
        domainProtocolVersion,
        transactionVersion,
        vettedPackages,
        ThreeExercises.tx,
        inputContractsMetadata,
      )
    }

    class Selector(loggerFactory: NamedLoggerFactory)(
        priorityOfDomain: DomainId => Int,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId],
        connectedDomains: Set[DomainId],
        domainsOfSubmittersAndInformees: NonEmpty[Set[DomainId]],
        prescribedDomainAlias: Option[String],
        domainProtocolVersion: DomainId => ProtocolVersion,
        transactionVersion: LanguageVersion,
        vettedPackages: Seq[LfPackageId],
        tx: LfVersionedTransaction,
        inputContractsMetadata: Set[WithContractMetadata[LfContractId]],
    )(implicit ec: ExecutionContext, traceContext: TraceContext) {

      import SimpleTopology._

      val inputContractIds: Set[LfContractId] = inputContractsMetadata.map(_.unwrap)

      private def domainStateProvider(
          d: DomainId
      ): Either[TransactionRoutingErrorWithDomain, (TopologySnapshot, ProtocolVersion)] =
        snapshotProvider(d).map((_, domainProtocolVersion(d)))

      private def snapshotProvider(
          d: DomainId
      ): Either[TransactionRoutingErrorWithDomain, TopologySnapshot] = Either.cond(
        connectedDomains.contains(d),
        SimpleTopology.defaultTestingIdentityFactory(
          correctTopology,
          vettedPackages,
        ),
        UnableToQueryTopologySnapshot.Failed(d): TransactionRoutingErrorWithDomain,
      )

      private val domainAliasResolver: DomainAlias => Option[DomainId] = alias =>
        Some(
          DomainId(
            UniqueIdentifier(Identifier.tryCreate(alias.unwrap), DefaultTestIdentities.namespace)
          )
        )

      private val domainRankComputation = new DomainRankComputation(
        participantId = submitterParticipantId,
        priorityOfDomain = priorityOfDomain,
        snapshotProvider = snapshotProvider,
        loggerFactory = loggerFactory,
      )

      private val transactionDataET = TransactionData
        .create(
          submitters = Set(signatory),
          transaction = tx,
          workflowIdO = prescribedDomainAlias.map(LfWorkflowId.assertFromString),
          domainOfContracts = ids => Future.successful(domainOfContracts(ids)),
          domainIdResolver = domainAliasResolver,
          inputContractsMetadata = inputContractsMetadata,
        )

      private val domainSelector: EitherT[Future, TransactionRoutingError, DomainSelector] =
        transactionDataET.map { transactionData =>
          new DomainSelector(
            participantId = submitterParticipantId,
            transactionData = transactionData,
            domainsOfSubmittersAndInformees = domainsOfSubmittersAndInformees,
            priorityOfDomain = priorityOfDomain,
            domainRankComputation = domainRankComputation,
            packageService = ExampleTransactionFactory.defaultPackageInfoService,
            domainStateProvider = domainStateProvider,
            loggerFactory = loggerFactory,
          )
        }

      def forSingleDomain: EitherT[Future, TransactionRoutingError, DomainRank] =
        domainSelector.flatMap(_.forSingleDomain)

      def forMultiDomain: EitherT[Future, TransactionRoutingError, DomainRank] =
        domainSelector.flatMap(_.forMultiDomain)
    }
  }

}