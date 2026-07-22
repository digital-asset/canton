// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, Counter, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.{
  ContractStakeholdersAndReassignmentCounter,
  InternalIndexService,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfPartyId, ReassignmentCounter, ReassignmentDiscriminator}
import com.digitalasset.daml.lf.data.Ref.Party
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.language.implicitConversions

trait BaseDigestProcessorTest extends AnyWordSpec with BaseTest {

  import BaseDigestProcessorTest.*

  val alice: LfPartyId = party("alice::aaa")
  val bob: LfPartyId = party("bob::bbb")
  val charlie: LfPartyId = party("charlie::ccc")

  val p1: ParticipantId = ParticipantId.tryFromProtoPrimitive("PAR::p1::zzz")
  val p2: ParticipantId = ParticipantId.tryFromProtoPrimitive("PAR::p2::yyy")
  val p3: ParticipantId = ParticipantId.tryFromProtoPrimitive("PAR::p3::xxx")
  val p4: ParticipantId = ParticipantId.tryFromProtoPrimitive("PAR::p4::www")
  val thisParticipant: ParticipantId = p1

  val ts0: CantonTimestamp = ts(0)
  val off1: Offset = off(1)
  val tp100: Timepoint = tp(100)
  val tp1_0: Timepoint = Timepoint(off1)(ts0)
}

object BaseDigestProcessorTest {
  val rc: Counter[ReassignmentDiscriminator] = ReassignmentCounter.Genesis

  implicit def toAcsChangeData(
      parties: Set[LfPartyId]
  ): ContractStakeholdersAndReassignmentCounter =
    ContractStakeholdersAndReassignmentCounter(parties, rc)
  implicit class RichOffset(off: Offset) {
    def toEpochTimestamp: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(off.positive)
  }

  def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)
  def ts(i: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(i.toLong)
  def off(i: Int): Offset = Offset.tryFromLong(i.toLong)
  def tp(i: Int): Timepoint = Timepoint(off(i))(ts(i))
  def tte(events: PartyToParticipantAuthorization*) =
    InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(events.toSet, None)
  def party(s: String): LfPartyId = LfPartyId.assertFromString(s)
  def acsDigest[K, V](at: Int, key: K, digestO: Option[V] = None): AcsDigest[K, V] =
    AcsDigest.empty(key, tp(at)).copy(digestO = digestO)

  def partyHosting(party: LfPartyId)(participants: ParticipantId*): (LfPartyId, PartyInfo) =
    (
      party,
      PartyInfo(PositiveInt.one, participants.map(_ -> ParticipantAttributes(Submission)).toMap),
    )

  def mkIndexService(
      contractsWithStakeholders: (Offset, LfContractId, Seq[LfPartyId])*
  ): InternalIndexService = {
    val acs = for {
      (offset, cid, rawStakeholders) <- contractsWithStakeholders
      stakeholders = rawStakeholders.map(LfPartyId.assertFromString)
      activeContract = InternalIndexService.ActiveContract(cid, stakeholders.toSet, rc)
      stakeholder <- stakeholders
    } yield {
      stakeholder -> (offset, activeContract)
    }
    val partyToContracts = immutable.MultiDict.from(acs)

    new InternalIndexService {
      override def activeContracts(partyIds: Set[LfPartyId], validAt: Option[Offset])(implicit
          traceContext: TraceContext
      ): Source[GetActiveContractsResponse, NotUsed] = ???

      override def topologyTransactions(partyId: LfPartyId, fromExclusive: Offset)(implicit
          traceContext: TraceContext
      ): Source[TopologyTransaction, NotUsed] = ???

      override def acsUpdates(synchronizerId: SynchronizerId, fromExclusive: Option[Offset])(
          implicit traceContext: TraceContext
      ): Source[InternalIndexService.AcsUpdateContainer, NotUsed] = ???

      override def acs(
          synchronizerId: SynchronizerId,
          activeAt: Offset,
          stakeholders1: Set[Party],
          stakeholders2: Set[Party],
      )(implicit
          traceContext: TraceContext
      ): Source[InternalIndexService.ActiveContract, NotUsed] = {
        val result =
          partyToContracts.values.collect {
            case (offset, contract)
                if offset <= activeAt &&
                  (stakeholders1.isEmpty || contract.stakeholders.exists(
                    stakeholders1
                  )) && (stakeholders2.isEmpty || contract.stakeholders.exists(stakeholders2)) =>
              contract
          }.toSet

        Source(result)
      }

      override def counterParties(
          synchronizerId: SynchronizerId,
          activeAt: Offset,
          party: Option[Party],
      )(implicit traceContext: TraceContext): Source[LfPartyId, NotUsed] = Source(
        party
          .flatMap(partyToContracts.sets.get(_))
          .getOrElse(partyToContracts.sets.values.flatten)
          .flatMap { case (offset, c) => if (offset <= activeAt) c.stakeholders else Set.empty }
          .toSet
      )
    }
  }
}
