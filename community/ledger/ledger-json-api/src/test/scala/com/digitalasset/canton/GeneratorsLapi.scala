// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.value.{Identifier, Record, Value}
import com.digitalasset.canton.http.json.StdGenerators.*
import com.digitalasset.canton.http.json.v2.ProtocolConvertersMocks.{
  defaultJsValue,
  defaultKeyLapiValue,
  defaultLapiRecord,
  defaultLapiValue,
}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.Code
import com.google.rpc.status.Status
import magnolify.scalacheck.auto.*
import magnolify.scalacheck.semiauto.ArbitraryDerivation
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

object GeneratorsLapi {

  def smallSeqArbitrary[T](implicit arbT: Arbitrary[T]): Arbitrary[Seq[T]] =
    Arbitrary(Gen.choose(0, 5).flatMap(n => Gen.listOfN(n, arbT.arbitrary)))

  implicit val arbStringSeq: Arbitrary[Seq[String]] =
    smallSeqArbitrary

  implicit val arbIdentifier: Arbitrary[Option[Identifier]] = Arbitrary(for {
    packageId <- Gen.stringOfN(8, Gen.alphaChar)
    moduleName <- Gen.stringOfN(8, Gen.alphaChar)
    scriptName <- Gen.stringOfN(8, Gen.alphaChar)
  } yield Some(Identifier(packageId, moduleName, scriptName)))

  implicit val arbJsValue: Arbitrary[ujson.Value] = Arbitrary {
    defaultJsValue
  }
  implicit val arbLapiValue: Arbitrary[Value] = Arbitrary {
    defaultLapiValue
  }

  implicit val arbLapiRecord: Arbitrary[Record] = Arbitrary {
    defaultLapiRecord
  }

  implicit val arbOptionLapiRecord: Arbitrary[Option[Record]] = Arbitrary {
    Some(defaultLapiRecord)
  }

  implicit val arbOptionLapiValue: Arbitrary[Option[Value]] = Arbitrary {
    Some(defaultLapiValue)
  }

  def nonEmptyScalaPbOneOf[T <: _root_.scalapb.GeneratedOneof](
      arb: Arbitrary[T]
  ): Arbitrary[T] =
    Arbitrary {
      arb.arbitrary.retryUntil(_.isDefined)
    }

  implicit val arbExerciseByKeyCommand: Arbitrary[lapi.commands.ExerciseByKeyCommand] = {
    val arb = ArbitraryDerivation[lapi.commands.ExerciseByKeyCommand]
    Arbitrary {
      arb.arbitrary.map(_.copy(contractKey = Some(defaultKeyLapiValue)))
    }
  }

  implicit val arbCommand: Arbitrary[lapi.commands.Command.Command] =
    nonEmptyScalaPbOneOf[lapi.commands.Command.Command](
      ArbitraryDerivation[lapi.commands.Command.Command]
    )

  implicit val arbEventEvent: Arbitrary[lapi.event.Event.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.event.Event.Event]
    )

  implicit val arbEventSeq: Arbitrary[Seq[lapi.event.Event]] =
    smallSeqArbitrary[lapi.event.Event]

  implicit val arbTopologyEvent: Arbitrary[lapi.topology_transaction.TopologyEvent.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.topology_transaction.TopologyEvent.Event]
    )
  implicit val arbPrefetchContractKey: Arbitrary[lapi.commands.PrefetchContractKey] =
    Arbitrary {
      for {
        templateId <- Arbitrary.arbitrary[Identifier]
        prefetchContractKey <- ArbitraryDerivation[lapi.commands.PrefetchContractKey].arbitrary
      } yield prefetchContractKey.copy(
        contractKey = Some(defaultKeyLapiValue),
        templateId = Some(templateId),
      )
    }

  private def byteStringOfN(n: Int): Gen[ByteString] =
    Gen.listOfN(n, Arbitrary.arbitrary[Byte]).map(list => ByteString.copyFrom(list.toArray))

  implicit val arbTransaction: Arbitrary[lapi.transaction.Transaction] =
    Arbitrary {
      for {
        effectiveAt <- Arbitrary.arbitrary[Timestamp]
        recordTime <- Arbitrary.arbitrary[Timestamp]
        transactionHash <- byteStringOfN(32)
        externalTransactionHash <- byteStringOfN(32)
        transaction <- ArbitraryDerivation[lapi.transaction.Transaction].arbitrary
      } yield transaction.copy(
        effectiveAt = Some(effectiveAt),
        recordTime = Some(recordTime),
        transactionHash = Some(transactionHash),
        externalTransactionHash = Some(externalTransactionHash),
      )
    }

  implicit val arbCommandSeq: Arbitrary[Seq[lapi.commands.Command]] =
    smallSeqArbitrary

  implicit val arbPrefContractKeySeq: Arbitrary[Seq[lapi.commands.PrefetchContractKey]] =
    smallSeqArbitrary

  implicit val arbCommands: Arbitrary[lapi.commands.Commands] =
    Arbitrary {
      for {
        // Ensure there is at least one `actAs`
        oneActAs <- Arbitrary.arbitrary[String]
        commands <- ArbitraryDerivation[lapi.commands.Commands].arbitrary
      } yield commands.copy(actAs = oneActAs +: commands.actAs)
    }

  implicit val arbOptCommands: Arbitrary[Option[lapi.commands.Commands]] = Arbitrary {
    arbCommands.arbitrary.map(Some(_))
  }

  implicit val arbReassignmentEventEvent: Arbitrary[lapi.reassignment.ReassignmentEvent.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.reassignment.ReassignmentEvent.Event]
    )

  implicit val arbGetUpdatesResponseUpdate
      : Arbitrary[lapi.update_service.GetUpdatesResponse.Update] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.update_service.GetUpdatesResponse.Update]
    )
  implicit val arbSubmitAndWaitTransactionResponse
      : Arbitrary[lapi.command_service.SubmitAndWaitForTransactionResponse] =
    Arbitrary {
      for {
        transaction <- Arbitrary.arbitrary[lapi.transaction.Transaction]
      } yield lapi.command_service.SubmitAndWaitForTransactionResponse(
        transaction = Some(transaction)
      )
    }

  implicit val arbReassignment: Arbitrary[lapi.reassignment.Reassignment] =
    Arbitrary {
      for {
        recordTime <- Arbitrary.arbitrary[Timestamp]
        reassignment <- ArbitraryDerivation[lapi.reassignment.Reassignment].arbitrary
      } yield reassignment.copy(recordTime = Some(recordTime))
    }

  implicit val arbSubmitAndReassignmentResponse
      : Arbitrary[lapi.command_service.SubmitAndWaitForReassignmentResponse] =
    Arbitrary {
      for {
        reassignment <- Arbitrary.arbitrary[lapi.reassignment.Reassignment]
      } yield lapi.command_service.SubmitAndWaitForReassignmentResponse(
        reassignment = Some(reassignment)
      )
    }

  implicit val arbInterfaceView: Arbitrary[lapi.event.InterfaceView] =
    Arbitrary {
      for {
        viewStatus <- Arbitrary.arbitrary[Status]
        viewValue <- Arbitrary.arbitrary[lapi.value.Record]
        interfaceView <- ArbitraryDerivation[lapi.event.InterfaceView].arbitrary
      } yield interfaceView.copy(
        viewStatus = Some(viewStatus),
        // if status is an error, viewValue should be empty
        viewValue = Option.when(viewStatus.code == Code.OK.getNumber)(viewValue),
      )
    }

  implicit val arbCreatedEvent: Arbitrary[lapi.event.CreatedEvent] =
    Arbitrary {
      for {
        createdAt <- Arbitrary.arbitrary[Timestamp]
        event <- ArbitraryDerivation[lapi.event.CreatedEvent].arbitrary
      } yield event.copy(createdAt = Some(createdAt), contractKey = Some(defaultKeyLapiValue))
    }

  implicit val arbOptCreatedEvent: Arbitrary[Option[lapi.event.CreatedEvent]] =
    Arbitrary {
      arbCreatedEvent.arbitrary.map(Some(_))
    }

  implicit val arbOptArchivedEvent: Arbitrary[Option[lapi.event.ArchivedEvent]] = {
    implicit val arb: Arbitrary[lapi.event.ArchivedEvent] =
      ArbitraryDerivation[lapi.event.ArchivedEvent]
    Arbitrary {
      arb.arbitrary.map(Some(_))
    }
  }

  implicit val arbIncompleteAssigned: Arbitrary[lapi.state_service.IncompleteAssigned] =
    Arbitrary {
      for {
        assignedEvent <- Arbitrary.arbitrary[lapi.reassignment.AssignedEvent]
      } yield lapi.state_service.IncompleteAssigned(assignedEvent = Some(assignedEvent))
    }

  implicit val arbOptUnAssignedEvent: Arbitrary[Option[lapi.reassignment.UnassignedEvent]] =
    Arbitrary {
      ArbitraryDerivation[lapi.reassignment.UnassignedEvent].arbitrary.map(Some(_))
    }

  implicit val arbGetActiveContractsResponse
      : Arbitrary[lapi.state_service.GetActiveContractsResponse] =
    ArbitraryDerivation[lapi.state_service.GetActiveContractsResponse]

  implicit val arbGetActiveContractsPageResponse
      : Arbitrary[lapi.state_service.GetActiveContractsPageResponse] =
    ArbitraryDerivation[lapi.state_service.GetActiveContractsPageResponse]

  implicit class OptM[T](val v: Option[T]) {
    def getValue()(implicit ct: ClassTag[T]): T =
      v.getOrElse(
        throw new RuntimeException(s"Could not create arbitrary for ${ct.runtimeClass.getName}")
      )
  }

}
