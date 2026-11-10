// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreTest,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState.*
import com.digitalasset.canton.topology.{ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{HasTestCloseContext, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}

final class AcsReplicationTopologyWorkflowTest
    extends TopologyStoreTest
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasTestCloseContext {

  override def closeContext: CloseContext = testCloseContext

  private val partyId = PartyId.tryFromProtoPrimitive("alice::1220abcd")
  private val sourceParticipant = ParticipantId.tryFromProtoPrimitive("PAR::source::participant")
  private val targetParticipant = ParticipantId.tryFromProtoPrimitive("PAR::target::participant")
  private val targetParticipant2 = ParticipantId.tryFromProtoPrimitive("PAR::target2::participant")
  private val psid =
    PhysicalSynchronizerId.tryFromString(s"da::one::${testedProtocolVersion.toString}-0")

  private def topologyStore(
      ptpMappingInfos: Map[ParticipantId, HostingState]*
  ): FutureUnlessShutdown[TopologyStore[SynchronizerStore]] = {
    val store = new InMemoryTopologyStore(
      SynchronizerStore(psid),
      predecessor = None,
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )
    type MaybePtpReplace = Option[TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]]
    MonadUtil
      .foldLeftM(None: MaybePtpReplace, ptpMappingInfos.zipWithIndex) {
        case (prevTxn, (ptpMappingInfo, idx)) =>
          val ts = CantonTimestamp.ofEpochMilli(idx.toLong)

          if (ptpMappingInfo.nonEmpty) {
            val hostingParticipants = ptpMappingInfo.map { case (pid, hostingState) =>
              HostingParticipant(
                pid,
                ParticipantPermission.Confirmation,
                hostingState,
              )
            }.toSeq
            val ptp = PartyToParticipant.tryCreate(
              partyId,
              threshold = PositiveInt.one,
              hostingParticipants,
            )

            val txn = TopologyTransaction.tryCreate(
              TopologyChangeOp.Replace,
              PositiveInt.tryCreate(idx + 1),
              ptp,
              testedProtocolVersion,
            )
            val tx = ValidatedTopologyTransaction[TopologyChangeOp, PartyToParticipant](
              SignedTopologyTransaction.withSignature(
                transaction = txn,
                signature = Signature.noSignature,
                isProposal = false,
                protocolVersion = testedProtocolVersion,
              )
            )

            store
              .update(
                SequencedTime(ts),
                EffectiveTime(ts),
                removals = prevTxn.toList
                  .map(prev => prev.mapping.uniqueKey -> (Some(prev.serial), Set(prev.hash)))
                  .toMap,
                additions = Seq(tx),
              )
              .map[MaybePtpReplace](_ => Some(txn))
          } else {
            store
              .update(
                SequencedTime(ts),
                EffectiveTime(ts),
                removals = prevTxn.toList
                  .map(prev => prev.mapping.uniqueKey -> (Some(prev.serial), Set(prev.hash)))
                  .toMap,
                additions = Seq.empty,
              )
              .map[MaybePtpReplace](_ => None)
          }
      }
      .map(_ => store)
  }

  private def expect(ptpOrdinal: Int): Option[EffectiveTime] = Some(
    EffectiveTime(CantonTimestamp.ofEpochMilli(ptpOrdinal.toLong).immediateSuccessor)
  )

  private def locatePartyFreezing(
      store: TopologyStore[SynchronizerStore],
      tp: ParticipantId = targetParticipant,
  ) =
    AcsReplicationTopologyWorkflow.partyToParticipantHostingStateValidFrom(
      partyId,
      tp,
      Freezing,
      store,
    )

  "AcsReplicationTopologyWorkflow" when {

    "looking up effective time via partyToParticipantHostingStateValidFrom" should {

      "find party freezing effective time in trivial setup" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(targetParticipant -> Freezing)
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe expect(0)
        }
      }

      "find party freezing effective time in simple realistic setup" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Nominated),
            Map(sourceParticipant -> Active, targetParticipant -> Freezing),
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe expect(2)
        }
      }

      "find party freezing effective time in multi-target setup" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(
              sourceParticipant -> Active,
              targetParticipant -> Nominated,
              targetParticipant2 -> Nominated,
            ),
            Map(
              sourceParticipant -> Active,
              targetParticipant -> Freezing,
              targetParticipant2 -> Nominated,
            ),
            Map(
              sourceParticipant -> Active,
              targetParticipant -> Freezing,
              targetParticipant2 -> Freezing,
            ),
          )
          effectiveAtTp1 <- locatePartyFreezing(store)
          effectiveAtTp2 <- locatePartyFreezing(store, targetParticipant2)
        } yield {
          effectiveAtTp1 shouldBe expect(2)
          effectiveAtTp2 shouldBe expect(3)
        }
      }

      "find the latest party freezing" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Nominated),
            Map(sourceParticipant -> Active, targetParticipant -> Freezing),
            Map(sourceParticipant -> Active, targetParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Freezing),
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe expect(4)
        }
      }

      "not find party freezing if party does not exist" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore()
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe None
        }
      }

      "not find party freezing if target is listed in the wrong hosting state" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active, targetParticipant -> Active)
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe None
        }
      }

      "not find party freezing if target never added in the expected hosting state" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Active),
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe None
        }
      }

      "not find party freezing before target active" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Nominated),
            Map(sourceParticipant -> Active, targetParticipant -> Freezing),
            Map(sourceParticipant -> Active, targetParticipant -> Active),
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe None
        }
      }

      "not find party freezing before target removed" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Nominated),
            Map(sourceParticipant -> Active, targetParticipant -> Freezing),
            Map(sourceParticipant -> Active),
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe None
        }
      }

      "not find party freezing before party removed" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
        for {
          store <- topologyStore(
            Map(sourceParticipant -> Active),
            Map(sourceParticipant -> Active, targetParticipant -> Nominated),
            Map(sourceParticipant -> Active, targetParticipant -> Freezing),
            Map.empty,
          )
          effectiveAt <- locatePartyFreezing(store)
        } yield {
          effectiveAt shouldBe None
        }
      }
    }
  }
}
