// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.Implicits._
import com.daml.lf.value.Value.ValueRecord
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.submission.DomainsFilterTest._
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfVersionedTransaction}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPackageId, LfPartyId, LfValue}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class DomainsFilterTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "DomainsFilter (simple create)" should {
    import SimpleTopology._

    val filter = DomainsFilterForTx(Transactions.Create.tx)
    val correctPackages = Transactions.Create.correctPackages

    "keep domains that satisfy all the constraints" in {
      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, correctTopology, correctPackages).futureValue

      unusableDomains shouldBe empty
      usableDomains shouldBe List(DefaultTestIdentities.domainId)
    }

    "reject domains when informees don't have an active participant" in {
      val partyNotConnected = observer
      val topology = correctTopology.filterNot { case (partyId, _) => partyId == partyNotConnected }

      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, topology, correctPackages).futureValue

      unusableDomains shouldBe List(
        DomainUsabilityChecker.MissingActiveParticipant(
          DefaultTestIdentities.domainId,
          Set(partyNotConnected),
        )
      )
      usableDomains shouldBe empty
    }

    "reject domains when packages are missing" in {
      val missingPackage = defaultPackageId
      val packages = correctPackages.filterNot(_ == missingPackage)

      def unknownPackageFor(participantId: ParticipantId) = DomainUsabilityChecker.PackageUnknownTo(
        missingPackage,
        "package does not exist on local node",
        participantId,
      )

      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, correctTopology, packages).futureValue
      usableDomains shouldBe empty

      unusableDomains shouldBe List(
        DomainUsabilityChecker.UnknownPackage(
          DefaultTestIdentities.domainId,
          Set(
            unknownPackageFor(submitterParticipantId),
            unknownPackageFor(observerParticipantId),
          ),
        )
      )
    }
  }

  "DomainsFilter (simple exercise by interface)" should {
    import SimpleTopology._

    val filter = DomainsFilterForTx(Transactions.ExerciseByInterface.tx)
    val correctPackages = Transactions.ExerciseByInterface.correctPackages

    "keep domains that satisfy all the constraints" in {
      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, correctTopology, correctPackages).futureValue

      unusableDomains shouldBe empty
      usableDomains shouldBe List(DefaultTestIdentities.domainId)
    }

    "reject domains when packages are missing" in {
      val testInstances = Seq(
        Set(defaultPackageId),
        Set(Transactions.ExerciseByInterface.interfacePackageId),
        Set(defaultPackageId, Transactions.ExerciseByInterface.interfacePackageId),
      )

      forAll(testInstances) { missingPackages =>
        val packages = correctPackages.filterNot(missingPackages.contains)

        def unknownPackageFor(
            participantId: ParticipantId
        ): Set[DomainUsabilityChecker.PackageUnknownTo] = missingPackages.map { missingPackage =>
          DomainUsabilityChecker.PackageUnknownTo(
            missingPackage,
            "package does not exist on local node",
            participantId,
          )
        }

        val (unusableDomains, usableDomains) =
          filter.split(loggerFactory, correctTopology, packages).futureValue

        usableDomains shouldBe empty
        unusableDomains shouldBe List(
          DomainUsabilityChecker.UnknownPackage(
            DefaultTestIdentities.domainId,
            unknownPackageFor(submitterParticipantId) ++ unknownPackageFor(observerParticipantId),
          )
        )
      }
    }
  }

}

private[submission] object DomainsFilterTest {
  /*
  Simple topology, with two parties (signatory, observer) each connected to one
  participant (submitterParticipantId, observerParticipantId)
   */
  object SimpleTopology {
    val submitterParticipantId: ParticipantId = ParticipantId("submitter")
    val observerParticipantId: ParticipantId = ParticipantId("counter")

    val signatory: LfPartyId = LfPartyId.assertFromString("signatory::default")
    val observer: LfPartyId = LfPartyId.assertFromString("observer::default")

    val correctTopology = Map(
      signatory -> List(submitterParticipantId),
      observer -> List(observerParticipantId),
    )

    def defaultTestingIdentityFactory(
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[LfPackageId] = Seq(),
    ): TopologySnapshot = {
      val defaultParticipantAttributes = ParticipantAttributes(Submission, TrustLevel.Vip)

      val testingIdentityFactory = TestingTopology(
        topology = topology.map { case (partyId, participantIds) =>
          partyId -> participantIds.map(_ -> defaultParticipantAttributes).toMap
        }
      ).build()

      testingIdentityFactory.topologySnapshot(packages = packages)
    }
  }

  object Transactions {
    object Create {
      import SimpleTopology._

      private val builder = TransactionBuilder()
      private val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = ValueRecord(None, ImmArray.Empty),
        signatories = Seq(signatory),
        observers = Seq(observer),
        key = None,
      )

      val tx: LfVersionedTransaction = {
        builder.add(createNode)
        builder.build()
      }

      val correctPackages = Seq(defaultPackageId)
    }

    object ExerciseByInterface {
      import SimpleTopology._

      /* To be sure that we have two different package ID (one for the create
      and the other for the interface id).
       */
      val interfacePackageId = s"$defaultPackageId for interface"

      val correctPackages = Seq[LfPackageId](defaultPackageId, interfacePackageId)

      private val builder = TransactionBuilder()
      private val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          templateId = "M:T",
          argument = LfValue.ValueUnit,
          signatories = List(signatory),
          observers = List(observer),
          key = None,
        )

        val interfaceId: Ref.Identifier =
          Ref.Identifier(interfacePackageId, QualifiedName.assertFromString("module:template"))
        builder.exercise(
          contract = createNode,
          choice = "someChoice",
          consuming = true,
          actingParties = Set(signatory),
          argument = LfValue.ValueUnit,
          interfaceId = Some(interfaceId),
          result = Some(LfValue.ValueUnit),
          choiceObservers = Set.empty,
          byKey = false,
        )
      }

      val tx = {
        builder.add(exerciseNode)
        builder.build()
      }
    }
  }

  case class DomainsFilterForTx(tx: LfVersionedTransaction) {
    def split(
        loggerFactory: NamedLoggerFactory,
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[LfPackageId] = Seq(),
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): Future[(List[DomainUsabilityChecker.DomainNotUsedReason], List[DomainId])] = {
      val domains = List(
        (
          DefaultTestIdentities.domainId,
          SimpleTopology.defaultTestingIdentityFactory(topology, packages),
          ExampleTransactionFactory.defaultPackageInfoService,
        )
      )

      new DomainsFilter(
        localParticipantId = SimpleTopology.submitterParticipantId,
        submittedTransaction = tx,
        domains = domains,
        loggerFactory = loggerFactory,
      ).split
    }
  }
}
