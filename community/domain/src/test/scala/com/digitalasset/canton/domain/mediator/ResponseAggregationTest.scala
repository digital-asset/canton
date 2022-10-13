// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{HashOps, Salt, TestHash, TestSalt}
import com.digitalasset.canton.data._
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.protocol.messages.Verdict.ParticipantReject
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RequestId, RootHash, ViewHash, v0}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.Assertion
import org.scalatest.funspec.PathAnyFunSpec

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ResponseAggregationTest extends PathAnyFunSpec with BaseTest {

  private implicit val ec: ExecutionContext = directExecutionContext

  describe(classOf[ResponseAggregation].getSimpleName) {
    def b[A](i: Int): BlindedNode[A] = BlindedNode(RootHash(TestHash.digest(i)))

    val hashOps: HashOps = new SymbolicPureCrypto
    def salt(i: Int): Salt = TestSalt.generateSalt(i)

    val domainId = DefaultTestIdentities.domainId
    val mediatorId = DefaultTestIdentities.mediator

    val alice = ConfirmingParty(LfPartyId.assertFromString("alice"), 3)
    val bob = ConfirmingParty(LfPartyId.assertFromString("bob"), 2)
    val charlie = PlainInformee(LfPartyId.assertFromString("charlie"))
    val dave = ConfirmingParty(LfPartyId.assertFromString("dave"), 1)
    val solo = ParticipantId("solo")

    val viewCommonData2 =
      ViewCommonData.create(hashOps)(
        Set(bob, charlie),
        NonNegativeInt.tryCreate(2),
        salt(54170),
        testedProtocolVersion,
      )
    val viewCommonData1 =
      ViewCommonData.create(hashOps)(
        Set(alice, bob),
        NonNegativeInt.tryCreate(3),
        salt(54171),
        testedProtocolVersion,
      )
    val view2 = TransactionView.tryCreate(hashOps)(viewCommonData2, b(100), Nil)
    val view1 = TransactionView.tryCreate(hashOps)(viewCommonData1, b(8), view2 :: Nil)

    val requestId = RequestId(CantonTimestamp.Epoch)

    val commonMetadataSignatory = CommonMetadata(hashOps)(
      ConfirmationPolicy.Signatory,
      domainId,
      mediatorId,
      salt(5417),
      new UUID(0L, 0L),
      testedProtocolVersion,
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
        testedProtocolVersion,
      )

    // TODO(i10210): We probably do not want this appearing in the context of the logged error
    val checkTestedProtocolVersion: Map[String, String] => Assertion =
      _ should contain(
        "representativeProtocolVersion" -> Verdict
          .protocolVersionRepresentativeFor(
            testedProtocolVersion
          )
          .toString
      )

    describe("under the Signatory policy") {
      def testReject() =
        LocalReject.ConsistencyRejections.LockedContracts.Reject(Seq())(testedProtocolVersion)
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
      val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
      val rootHash = informeeMessage.rootHash
      val someOtherRootHash = Some(RootHash(TestHash.digest(12345)))

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      val sut =
        ResponseAggregation(requestId, informeeMessage, testedProtocolVersion)(loggerFactory)

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
          ViewCommonData.create(hashOps)(
            Set(alice),
            NonNegativeInt.zero,
            salt(54172),
            testedProtocolVersion,
          )
        val viewThresholdTooLow =
          TransactionView.tryCreate(hashOps)(viewcommonDataThresholdTooLow, b(100), Nil)
        val fullInformeeTreeThresholdTooLow = FullInformeeTree(
          GenTransactionTree(hashOps)(
            b(0),
            commonMetadataSignatory,
            b(2),
            MerkleSeq.fromSeq(hashOps)(viewThresholdTooLow :: Nil),
          )
        )

        val alarmMsg =
          s"Rejected transaction as a view has threshold below the confirmation policy's minimum threshold. viewHash=${viewThresholdTooLow.viewHash}, threshold=0"
        val alarm = MediatorError.MalformedMessage.Reject(
          alarmMsg,
          v0.MediatorRejection.Code.ViewThresholdBelowMinimumThreshold,
          testedProtocolVersion,
        )

        val sut = loggerFactory.assertLogs(
          ResponseAggregation(
            requestId,
            InformeeMessage(fullInformeeTreeThresholdTooLow)(testedProtocolVersion),
            testedProtocolVersion,
          )(loggerFactory),
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ shouldBe alarmMsg,
            checkTestedProtocolVersion,
          ),
        )

        sut.state shouldBe Left(alarm)
      }

      it("should reject responses with the wrong root hash") {
        val responseWithWrongRootHash = MediatorResponse.tryCreate(
          requestId,
          solo,
          Some(view1.viewHash),
          LocalApprove()(testedProtocolVersion),
          someOtherRootHash,
          Set(alice.party),
          domainId,
          testedProtocolVersion,
        )
        val result = loggerFactory.assertLogs(
          sut
            .progress(requestId.unwrap.plusSeconds(1), responseWithWrongRootHash, topologySnapshot)
            .value
            .futureValue,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ shouldBe s"Unknown request $requestId: received mediator response by $solo for view hash ${view1.viewHash} for root hash ${someOtherRootHash.value}",
            checkTestedProtocolVersion,
          ),
        )
        result shouldBe None
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
              ParticipantReject(
                NonEmpty(List, Set(alice.party) -> testReject()),
                testedProtocolVersion,
              ),
              testedProtocolVersion,
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
              ParticipantReject(
                NonEmpty(List, Set(alice.party) -> testReject(), Set(bob.party) -> testReject()),
                testedProtocolVersion,
              )
            it("rejects the transaction") {
              rejected2 shouldBe ResponseAggregation(
                requestId,
                informeeMessage,
                changeTs2,
                rejection,
                testedProtocolVersion,
                TraceContext.empty,
              )(loggerFactory)
            }

            describe("further rejection") {
              val changeTs3 = changeTs2.plusSeconds(1)
              val response3 = mkResponse(view1.viewHash, testReject(), Set(bob.party), rootHash)
              lazy val rejected3 =
                rejected2.progress(changeTs3, response3, topologySnapshot).value.futureValue
              it("should not rejection after finalization") {
                rejected3 shouldBe None
              }
            }

            describe("further approval") {
              val changeTs3 = changeTs2.plusSeconds(1)
              val response3 = mkResponse(
                view1.viewHash,
                LocalApprove()(testedProtocolVersion),
                Set(alice.party),
                rootHash,
              )
              lazy val rejected3 =
                rejected2.progress(changeTs3, response3, topologySnapshot).value.futureValue
              it("should not allow approval after finalization") {
                rejected3 shouldBe None
              }
            }
          }
        }
      }

      describe("approval") {
        lazy val changeTs = requestId.unwrap.plusSeconds(1)
        val response1 = mkResponse(
          view1.viewHash,
          LocalApprove()(testedProtocolVersion),
          Set(bob.party),
          rootHash,
        )
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
          val response2 = mkResponse(
            view1.viewHash,
            LocalApprove()(testedProtocolVersion),
            Set(alice.party),
            rootHash,
          )
          lazy val step2 =
            valueOrFail(result.progress(changeTs, response2, topologySnapshot).value.futureValue)(
              "Alice's approval"
            )
          val response3 = mkResponse(
            view2.viewHash,
            LocalApprove()(testedProtocolVersion),
            Set(bob.party),
            rootHash,
          )
          lazy val step3 =
            valueOrFail(step2.progress(changeTs, response3, topologySnapshot).value.futureValue)(
              "Bob's approval for view 2"
            )
          it("should get an approved verdict") {
            step3 shouldBe ResponseAggregation(
              requestId,
              informeeMessage,
              changeTs,
              Verdict.Approve(testedProtocolVersion),
              testedProtocolVersion,
              TraceContext.empty,
            )(loggerFactory)
          }

          describe("further rejection") {
            val response4 =
              mkResponse(
                view1.viewHash,
                LocalReject.MalformedRejects.Payloads.Reject("test4")(testedProtocolVersion),
                Set.empty,
                rootHash,
              )
            lazy val result =
              step3
                .progress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .value
                .futureValue
            it("should not allow repeated rejection") {
              result shouldBe None
            }
          }

          describe("further redundant approval") {
            val response4 = mkResponse(
              view1.viewHash,
              LocalApprove()(testedProtocolVersion),
              Set(alice.party),
              rootHash,
            )
            lazy val result =
              step3
                .progress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .value
                .futureValue
            it("should not allow repeated rejection") {
              result shouldBe None
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
        testedProtocolVersion,
      )
      val viewCommonData2 = ViewCommonData.create(hashOps)(
        Set(alice, bob, dave),
        NonNegativeInt.tryCreate(3),
        salt(54171),
        testedProtocolVersion,
      )
      val view2 = TransactionView.tryCreate(hashOps)(viewCommonData2, b(100), Nil)
      val view1 = TransactionView.tryCreate(hashOps)(viewCommonData1, b(8), Nil)

      val informeeMessage = InformeeMessage(
        FullInformeeTree(
          GenTransactionTree(hashOps)(
            b(0),
            commonMetadataSignatory,
            b(2),
            MerkleSeq.fromSeq(hashOps)(view1 :: view2 :: Nil),
          )
        )
      )(testedProtocolVersion)

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      when(topologySnapshot.canConfirm(eqTo(solo), eqTo(bob.party), any[TrustLevel]))
        .thenReturn(Future.successful(true))
      when(topologySnapshot.canConfirm(eqTo(solo), eqTo(dave.party), any[TrustLevel]))
        .thenReturn(Future.successful(true))
      when(topologySnapshot.canConfirm(eqTo(solo), eqTo(alice.party), any[TrustLevel]))
        .thenReturn(Future.successful(false))

      val sut =
        ResponseAggregation(requestId, informeeMessage, testedProtocolVersion)(loggerFactory)
      lazy val changeTs = requestId.unwrap.plusSeconds(1)
      def testReject(reason: String) =
        LocalReject.MalformedRejects.Payloads.Reject(reason)(testedProtocolVersion)

      describe("for a single view") {
        it("should update the pending confirming parties set for all hosted parties") {
          val response = mkResponse(view1.viewHash, testReject("malformed view"), Set.empty, None)
          val result = loggerFactory.assertLogs(
            valueOrFail(sut.progress(changeTs, response, topologySnapshot).value.futureValue)(
              "Malformed response for a view hash"
            ),
            _.shouldBeCantonError(
              LocalReject.MalformedRejects.Payloads,
              _ shouldBe "Rejected transaction due to malformed payload within views malformed view",
            ),
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
          val rejectMsg = "malformed request"
          val response =
            MediatorResponse.tryCreate(
              requestId,
              solo,
              None,
              testReject(rejectMsg),
              None,
              Set.empty,
              domainId,
              testedProtocolVersion,
            )
          val result = loggerFactory.assertLogs(
            valueOrFail(sut.progress(changeTs, response, topologySnapshot).value.futureValue)(
              "Malformed response without view hash"
            ),
            _.shouldBeCantonError(
              LocalReject.MalformedRejects.Payloads,
              _ shouldBe s"Rejected transaction due to malformed payload within views $rejectMsg",
              _ should (contain("reportedBy" -> s"$solo") and contain(
                "requestId" -> requestId.toString
              )),
            ),
          )
          result.version shouldBe changeTs
          result.state shouldBe Right(
            Map(
              view1.viewHash -> ViewState(
                Set(alice),
                3,
                List(Set(bob.party) -> testReject(rejectMsg)),
              ),
              view2.viewHash -> ViewState(
                Set(alice),
                3,
                List(Set(bob.party, dave.party) -> testReject(rejectMsg)),
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
        testedProtocolVersion,
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
      val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
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

      val sut =
        ResponseAggregation(requestId, informeeMessage, testedProtocolVersion)(loggerFactory)
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
          mkResponse(
            view1.viewHash,
            LocalApprove()(testedProtocolVersion),
            Set(alice.party, bob.party),
            rootHash,
          )

        loggerFactory.assertLogs(
          sut
            .progress(requestId.unwrap.plusSeconds(1), response, topologySnapshotVip)
            .value
            .futureValue shouldBe None,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ shouldBe s"Request ${requestId.unwrap}: unauthorized mediator response for view ${view1.viewHash} by $solo on behalf of ${Set(bob.party)}",
            checkTestedProtocolVersion,
          ),
        )
      }

      it("should ignore malformed non-VIP responses") {
        val rejectMsg = "malformed request"
        val reject =
          LocalReject.MalformedRejects.Payloads.Reject(rejectMsg)(testedProtocolVersion)
        val response =
          MediatorResponse.tryCreate(
            requestId,
            nonVip,
            None,
            reject,
            None,
            Set.empty,
            domainId,
            testedProtocolVersion,
          )
        val result = loggerFactory.assertLogs(
          valueOrFail(
            sut
              .progress(requestId.unwrap.plusSeconds(1), response, topologySnapshotVip)
              .value
              .futureValue
          )(s"$nonVip responds Malformed"),
          _.shouldBeCantonError(
            LocalReject.MalformedRejects.Payloads,
            _ shouldBe s"Rejected transaction due to malformed payload within views $rejectMsg",
            _ should (contain("reportedBy" -> s"$nonVip") and contain(
              "requestId" -> requestId.toString
            )),
          ),
        )
        result.state shouldBe Right(initialState)
      }

      it("should accept VIP responses") {
        val changeTs = requestId.unwrap.plusSeconds(1)
        val response = mkResponse(
          view1.viewHash,
          LocalApprove()(testedProtocolVersion),
          Set(alice.party),
          rootHash,
        )
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
