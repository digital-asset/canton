// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.Implicits._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.{language, transaction}
import com.digitalasset.canton.protocol.{LfContractId, LfVersionedTransaction}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.{LfPackageId, LfPartyId, LfValue}

private[submission] object DomainSelectionFixture {
  def unknownPackageFor(participantId: ParticipantId, missingPackage: LfPackageId) =
    DomainUsabilityChecker.PackageUnknownTo(
      missingPackage,
      "package does not exist on local node",
      participantId,
    )

  /*
  Simple topology, with two parties (signatory, observer) each connected to one
  participant (submitterParticipantId, observerParticipantId)
   */
  object SimpleTopology {
    val submitterParticipantId: ParticipantId = ParticipantId("submitter")
    val observerParticipantId: ParticipantId = ParticipantId("counter")

    val signatory: LfPartyId = LfPartyId.assertFromString("signatory::default")
    val observer: LfPartyId = LfPartyId.assertFromString("observer::default")

    val correctTopology: Map[LfPartyId, List[ParticipantId]] = Map(
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
    def addExerciseNode(
        builder: TransactionBuilder,
        inputContractId: LfContractId,
        signatory: LfPartyId,
        observer: LfPartyId,
        interfaceId: Option[Ref.Identifier] = None,
    ): transaction.NodeId = {
      val createNode = builder.create(
        id = inputContractId,
        templateId = "M:T",
        argument = LfValue.ValueUnit,
        signatories = List(signatory),
        observers = List(observer),
        key = None,
      )

      val exerciseNode = builder.exercise(
        contract = createNode,
        choice = "someChoice",
        consuming = true,
        actingParties = Set(signatory),
        argument = LfValue.ValueUnit,
        interfaceId = interfaceId,
        result = Some(LfValue.ValueUnit),
        choiceObservers = Set.empty,
        byKey = false,
      )

      builder.add(exerciseNode)
    }

    object Create {
      val correctPackages = Seq(defaultPackageId)

      def tx(
          version: language.LanguageVersion = LanguageVersion.StableVersions.max
      ): LfVersionedTransaction = {
        import SimpleTopology._

        val builder = TransactionBuilder(_ => version)
        val createNode = builder.create(
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
        tx
      }
    }

    object ThreeExercises {

      import SimpleTopology._

      private val builder = TransactionBuilder()

      val inputContract1Id: LfContractId = builder.newCid
      val inputContract2Id: LfContractId = builder.newCid
      val inputContract3Id: LfContractId = builder.newCid
      val inputContractIds: Set[LfContractId] =
        Set(inputContract1Id, inputContract2Id, inputContract3Id)

      inputContractIds.foreach(addExerciseNode(builder, _, signatory, observer))

      val tx: LfVersionedTransaction = builder.build()
    }

    object ExerciseByInterface {
      /* To be sure that we have two different package ID (one for the create
      and the other for the interface id).
       */
      val interfacePackageId = s"$defaultPackageId for interface"

      val correctPackages = Seq[LfPackageId](defaultPackageId, interfacePackageId)
    }

    case class ExerciseByInterface(
        version: language.LanguageVersion = LanguageVersion.StableVersions.max
    ) {
      import SimpleTopology._
      import ExerciseByInterface._

      private val builder = TransactionBuilder(_ => version)

      val inputContractId: Value.ContractId = builder.newCid

      addExerciseNode(
        builder,
        inputContractId,
        signatory,
        observer,
        interfaceId = Some(
          Ref.Identifier(interfacePackageId, QualifiedName.assertFromString("module:template"))
        ),
      )

      val tx: LfVersionedTransaction = builder.build()
    }
  }
}
