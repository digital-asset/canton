// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

import java.util.UUID
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{HashOps, Salt, TestHash, TestSalt}
import com.digitalasset.canton.data.{ConfirmingParty, PlainInformee, _}
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.protocol.messages.Verdict.{MediatorReject, RejectReasons}
import com.digitalasset.canton.protocol.messages.{
  InformeeMessage,
  LocalApprove,
  LocalReject,
  LocalVerdict,
  MediatorResponse,
  Verdict,
}
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RequestId, RootHash, ViewHash}
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.canton.util.ShowUtil._
import org.scalatest.funspec.PathAnyFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ResponseAggregationTest extends PathAnyFunSpec with BaseTest {

  private implicit val ec: ExecutionContext = directExecutionContext

  describe(classOf[ResponseAggregation].getSimpleName) {
    def b[A](i: Int): BlindedNode[A] = BlindedNode(RootHash(TestHash.digest(i)))

    val hashOps: HashOps = new SymbolicPureCrypto
    def salt(i: Int): Salt = TestSalt.generate(i)

    val domainId = DefaultTestIdentities.domainId
    val mediatorId = DefaultTestIdentities.mediator
    val alice = ConfirmingParty(LfPartyId.assertFromString("alice"), 3)
    val bob = ConfirmingParty(LfPartyId.assertFromString("bob"), 2)
    val charlie = PlainInformee(LfPartyId.assertFromString("charlie"))
    val dave = ConfirmingParty(LfPartyId.assertFromString("dave"), 1)
    val solo = ParticipantId("solo")

    val viewCommonData2 =
      ViewCommonData.create(hashOps)(Set(bob, charlie), NonNegativeInt.tryCreate(2), salt(54170))
    val viewCommonData1 =
      ViewCommonData.create(hashOps)(Set(alice, bob), NonNegativeInt.tryCreate(3), salt(54171))
    val view2 = TransactionView(hashOps)(viewCommonData2, b(100), Nil)
    val view1 = TransactionView(hashOps)(viewCommonData1, b(8), view2 :: Nil)

    val requestId = RequestId(CantonTimestamp.Epoch)

    val commonMetadataSignatory = CommonMetadata(hashOps)(
      ConfirmationPolicy.Signatory,
      domainId,
      mediatorId,
      salt(5417),
      new UUID(0L, 0L),
    )
    def mkResponse(
        viewHash: ViewHash,
        verdict: LocalVerdict,
        confirmingParties: Set[LfPartyId],
        rootHashO: Option[RootHash],
    ): MediatorResponse =
      MediatorResponse.tryCreate(
        requestId,
        solo,
        Some(viewHash),
        verdict,
        rootHashO,
        confirmingParties,
        domainId,
      )

    describe("under the Signatory policy") {
      def testReject() = LocalReject.ConsistencyRejections.LockedContracts.Reject(Seq())
      val fullInformeeTree =
        FullInformeeTree(
          GenTransactionTree(hashOps)(
            b(0),
            commonMetadataSignatory,
            b(2),
            MerkleSeq.fromSeq(hashOps)(view1 :: Nil),
          )
        )
      val requestId = RequestId(CantonTimestamp.Epoch)
      val informeeMessage = InformeeMessage(fullInformeeTree)
      val rootHash = informeeMessage.rootHash
      val someOtherRootHash = Some(RootHash(TestHash.digest(12345)))

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      val sut = ResponseAggregation(requestId, informeeMessage)(loggerFactory)

      it("should have initially all pending confirming parties listed") {
        sut.state shouldBe Right(
          Map(
            view1.viewHash -> ViewState(Set(alice, bob), 3, Nil),
            view2.viewHash -> ViewState(Set(bob), 2, Nil),
          )
        )
      }

      it("should check the policy's minimum threshold") {
        val viewcommonDataThresholdTooLow =
          ViewCommonData.create(hashOps)(Set(alice), NonNegativeInt.zero, salt(54172))
        val viewThresholdTooLow =
          TransactionView(hashOps)(viewcommonDataThresholdTooLow, b(100), Nil)
        val fullInformeeTreeThresholdTooLow = FullInformeeTree(
          GenTransactionTree(hashOps)(
            b(0),
            commonMetadataSignatory,
            b(2),
            MerkleSeq.fromSeq(hashOps)(viewThresholdTooLow :: Nil),
          )
        )

        val sut = ResponseAggregation(requestId, InformeeMessage(fullInformeeTreeThresholdTooLow))(
          loggerFactory
        )

        sut.state shouldBe Left(
          MediatorReject.MaliciousSubmitter.ViewThresholdBelowMinimumThreshold
            .Reject(show"viewHash=${viewThresholdTooLow.viewHash}, threshold=0")
        )

      }

      it("should reject responses with the wrong root hash") {
        val responseWithWrongRootHash = MediatorResponse.tryCreate(
          requestId,
          solo,
          Some(view1.viewHash),
          LocalApprove,
          someOtherRootHash,
          Set(alice.party),
          domainId,
        )
        val result =
          sut
            .progress(requestId.unwrap.plusSeconds(1), responseWithWrongRootHash, topologySnapshot)
            .value
            .futureValue
        result shouldBe Left(
          MediatorRequestNotFound(requestId, Some(view1.viewHash), someOtherRootHash)
        )
      }

      when(topologySnapshot.canConfirm(eqTo(solo), any[LfPartyId], any[TrustLevel]))
        .thenReturn(Future.successful(true))

      describe("rejection") {
        val changeTs1 = requestId.unwrap.plusSeconds(1)

        describe("by Alice with veto rights due to her weight of 3") {
          it("rejects the transaction") {
            val response1 = mkResponse(view1.viewHash, testReject(), Set(alice.party), rootHash)
            val rejected1 =
              valueOrFail(sut.progress(changeTs1, response1, topologySnapshot).value.futureValue)(
                "Alice's rejection"
              )

            rejected1 shouldBe ResponseAggregation(
              requestId,
              informeeMessage,
              changeTs1,
              RejectReasons(List(Set(alice.party) -> testReject())),
              TraceContext.empty,
            )(loggerFactory)
          }
        }

        describe("by a 'light-weight' party") {
          val response1 = mkResponse(view1.viewHash, testReject(), Set(bob.party), rootHash)
          lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
            valueOrFail(sut.progress(changeTs1, response1, topologySnapshot).value.futureValue)(
              "Bob's rejection"
            )
          }

          it("leaves possibility of overall approval") {
            rejected1.version shouldBe changeTs1
            rejected1.state shouldBe
              Right(
                Map(
                  view1.viewHash -> ViewState(Set(alice), 3, List(Set(bob.party) -> testReject())),
                  view2.viewHash -> ViewState(Set(bob), 2, Nil),
                )
              )
          }

          describe("a subsequent rejection that ensure no possibility of overall approval") {
            val changeTs2 = changeTs1.plusSeconds(1)
            val response2 = mkResponse(view1.viewHash, testReject(), Set(alice.party), rootHash)
            lazy val rejected2 =
              valueOrFail(
                rejected1.progress(changeTs2, response2, topologySnapshot).value.futureValue
              )("Alice's second rejection")
            val rejection =
              RejectReasons(List(Set(alice.party) -> testReject(), Set(bob.party) -> testReject()))
            it("rejects the transaction") {
              rejected2 shouldBe ResponseAggregation(
                requestId,
                informeeMessage,
                changeTs2,
                rejection,
                TraceContext.empty,
              )(loggerFactory)
            }

            describe("further rejection") {
              val changeTs3 = changeTs2.plusSeconds(1)
              val response3 = mkResponse(view1.viewHash, testReject(), Set(bob.party), rootHash)
              lazy val rejected3 =
                rejected2.progress(changeTs3, response3, topologySnapshot).value.futureValue
              it("should not rejection after finalization") {
                rejected3 shouldBe Left(MediatorRequestAlreadyFinalized(requestId, rejection))
              }
            }

            describe("further approval") {
              val changeTs3 = changeTs2.plusSeconds(1)
              val response3 = mkResponse(view1.viewHash, LocalApprove, Set(alice.party), rootHash)
              lazy val rejected3 =
                rejected2.progress(changeTs3, response3, topologySnapshot).value.futureValue
              it("should not allow approval after finalization") {
                rejected3 shouldBe Left(MediatorRequestAlreadyFinalized(requestId, rejection))
              }
            }
          }
        }
      }

      describe("approval") {
        lazy val changeTs = requestId.unwrap.plusSeconds(1)
        val response1 = mkResponse(view1.viewHash, LocalApprove, Set(bob.party), rootHash)
        lazy val result =
          valueOrFail(sut.progress(changeTs, response1, topologySnapshot).value.futureValue)(
            "Bob's approval"
          )
        it("should update the pending confirming parties set") {
          result.version shouldBe changeTs
          result.state shouldBe
            Right(
              Map(
                view1.viewHash -> ViewState(Set(alice), 1, Nil),
                view2.viewHash -> ViewState(Set(bob), 2, Nil),
              )
            )
        }
        describe("if approvals meet the threshold") {
          val response2 = mkResponse(view1.viewHash, LocalApprove, Set(alice.party), rootHash)
          lazy val step2 =
            valueOrFail(result.progress(changeTs, response2, topologySnapshot).value.futureValue)(
              "Alice's approval"
            )
          val response3 = mkResponse(view2.viewHash, LocalApprove, Set(bob.party), rootHash)
          lazy val step3 =
            valueOrFail(step2.progress(changeTs, response3, topologySnapshot).value.futureValue)(
              "Bob's approval for view 2"
            )
          it("should get an approved verdict") {
            step3 shouldBe ResponseAggregation(
              requestId,
              informeeMessage,
              changeTs,
              Verdict.Approve,
              TraceContext.empty,
            )(loggerFactory)
          }

          describe("further rejection") {
            val response4 =
              mkResponse(
                view1.viewHash,
                LocalReject.MalformedRejects.Payloads.Reject("test4"),
                Set.empty,
                rootHash,
              )
            lazy val result =
              step3
                .progress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .value
                .futureValue
            it("should not allow repeated rejection") {
              result shouldBe Left(MediatorRequestAlreadyFinalized(requestId, Verdict.Approve))
            }
          }

          describe("further redundant approval") {
            val response4 = mkResponse(view1.viewHash, LocalApprove, Set(alice.party), rootHash)
            lazy val result =
              step3
                .progress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .value
                .futureValue
            it("should not allow repeated rejection") {
              result shouldBe Left(MediatorRequestAlreadyFinalized(requestId, Verdict.Approve))
            }
          }
        }
      }
    }

    describe("response Malformed") {

      val viewCommonData1 = ViewCommonData.create(hashOps)(
        Set(alice, bob, charlie),
        NonNegativeInt.tryCreate(3),
        salt(54170),
      )
      val viewCommonData2 = ViewCommonData.create(hashOps)(
        Set(alice, bob, dave),
        NonNegativeInt.tryCreate(3),
        salt(54171),
      )
      val view2 = TransactionView(hashOps)(viewCommonData2, b(100), Nil)
      val view1 = TransactionView(hashOps)(viewCommonData1, b(8), Nil)

      val informeeMessage = InformeeMessage(
        FullInformeeTree(
          GenTransactionTree(hashOps)(
            b(0),
            commonMetadataSignatory,
            b(2),
            MerkleSeq.fromSeq(hashOps)(view1 :: view2 :: Nil),
          )
        )
      )

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      when(topologySnapshot.canConfirm(eqTo(solo), eqTo(bob.party), any[TrustLevel]))
        .thenReturn(Future.successful(true))
      when(topologySnapshot.canConfirm(eqTo(solo), eqTo(dave.party), any[TrustLevel]))
        .thenReturn(Future.successful(true))
      when(topologySnapshot.canConfirm(eqTo(solo), eqTo(alice.party), any[TrustLevel]))
        .thenReturn(Future.successful(false))

      val sut = ResponseAggregation(requestId, informeeMessage)(loggerFactory)
      lazy val changeTs = requestId.unwrap.plusSeconds(1)
      def testReject(reason: String) = LocalReject.MalformedRejects.Payloads.Reject(reason)

      describe("for a single view") {
        it("should update the pending confirming parties set for all hosted parties") {
          val response = mkResponse(view1.viewHash, testReject("malformed view"), Set.empty, None)
          val result = loggerFactory.assertLogs(
            valueOrFail(sut.progress(changeTs, response, topologySnapshot).value.futureValue)(
              "Malformed response for a view hash"
            ),
            _.shouldBeCantonError(testReject("malformed view")),
          )

          result.version shouldBe changeTs
          result.state shouldBe Right(
            Map(
              view1.viewHash -> ViewState(
                Set(alice),
                3,
                List(Set(bob.party) -> testReject("malformed view")),
              ),
              view2.viewHash -> ViewState(Set(alice, bob, dave), 3, Nil),
            )
          )
        }
      }

      describe("without a view hash") {
        it("should update the pending confirming parties for all hosted parties in all views") {
          val response =
            MediatorResponse.tryCreate(
              requestId,
              solo,
              None,
              testReject("malformed request"),
              None,
              Set.empty,
              domainId,
            )
          val result = loggerFactory.assertLogs(
            valueOrFail(sut.progress(changeTs, response, topologySnapshot).value.futureValue)(
              "Malformed response without view hash"
            ),
            _.shouldBeCantonError(
              testReject("malformed request"),
              Map("reportedBy" -> s"$solo", "requestId" -> requestId.toString),
            ),
          )
          result.version shouldBe changeTs
          result.state shouldBe Right(
            Map(
              view1.viewHash -> ViewState(
                Set(alice),
                3,
                List(Set(bob.party) -> testReject("malformed request")),
              ),
              view2.viewHash -> ViewState(
                Set(alice),
                3,
                List(Set(bob.party, dave.party) -> testReject("malformed request")),
              ),
            )
          )
        }
      }
    }

    describe("under the VIP policy") {
      val commonMetadata = CommonMetadata(hashOps)(
        ConfirmationPolicy.Vip,
        domainId,
        mediatorId,
        salt(5417),
        new UUID(0L, 0L),
      )
      val fullInformeeTree =
        FullInformeeTree(
          GenTransactionTree(hashOps)(
            b(0),
            commonMetadata,
            b(2),
            MerkleSeq.fromSeq(hashOps)(view1 :: Nil),
          )
        )
      val informeeMessage = InformeeMessage(fullInformeeTree)
      val rootHash = informeeMessage.rootHash
      val nonVip = ParticipantId("notAVip")

      val topologySnapshotVip: TopologySnapshot = mock[TopologySnapshot]
      when(topologySnapshotVip.canConfirm(eqTo(solo), eqTo(alice.party), eqTo(TrustLevel.Vip)))
        .thenReturn(Future.successful(true))
      when(topologySnapshotVip.canConfirm(eqTo(solo), eqTo(bob.party), eqTo(TrustLevel.Vip)))
        .thenReturn(Future.successful(false))
      when(topologySnapshotVip.canConfirm(eqTo(nonVip), any[LfPartyId], eqTo(TrustLevel.Vip)))
        .thenReturn(Future.successful(false))
      when(topologySnapshotVip.canConfirm(eqTo(nonVip), any[LfPartyId], eqTo(TrustLevel.Ordinary)))
        .thenReturn(Future.successful(true))

      val sut = ResponseAggregation(requestId, informeeMessage)(loggerFactory)
      val initialState =
        Map(
          view1.viewHash -> ViewState(Set(alice, bob), 3, Nil),
          view2.viewHash -> ViewState(Set(bob), 2, Nil),
        )

      it("should have all pending confirming parties listed") {
        sut.state shouldBe Right(initialState)
      }

      it("should reject non-VIP responses") {
        val response =
          mkResponse(view1.viewHash, LocalApprove, Set(alice.party, bob.party), rootHash)
        val result =
          leftOrFail(
            sut
              .progress(requestId.unwrap.plusSeconds(1), response, topologySnapshotVip)
              .value
              .futureValue
          )("solo confirms without VIP trust level for bob")
        result shouldBe UnauthorizedMediatorResponse(
          requestId,
          view1.viewHash,
          solo,
          Set(bob.party),
        )
      }

      it("should ignore malformed non-VIP responses") {
        val reject = LocalReject.MalformedRejects.Payloads.Reject("malformed request")
        val response =
          MediatorResponse.tryCreate(requestId, nonVip, None, reject, None, Set.empty, domainId)
        val result = loggerFactory.assertLogs(
          valueOrFail(
            sut
              .progress(requestId.unwrap.plusSeconds(1), response, topologySnapshotVip)
              .value
              .futureValue
          )(s"$nonVip responds Malformed"),
          _.shouldBeCantonError(
            reject,
            Map("reportedBy" -> s"$nonVip", "requestId" -> requestId.toString),
          ),
        )
        result.state shouldBe Right(initialState)
      }

      it("should accept VIP responses") {
        val changeTs = requestId.unwrap.plusSeconds(1)
        val response = mkResponse(view1.viewHash, LocalApprove, Set(alice.party), rootHash)
        val result = valueOrFail(
          sut.progress(changeTs, response, topologySnapshotVip).value.futureValue
        )("solo confirms with VIP trust level for alice")

        result.version shouldBe changeTs
        result.state shouldBe
          Right(
            Map(
              view1.viewHash -> ViewState(Set(bob), 0, Nil),
              view2.viewHash -> ViewState(Set(bob), 2, Nil),
            )
          )
      }
    }
  }
}
