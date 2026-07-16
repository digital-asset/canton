// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule.FetchBatchesSingleWorkflowId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore.BatchIdAndEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  FakePipeToSelfCellUnitTestContext,
  FakePipeToSelfCellUnitTestEnv,
  IgnoringUnitTestEnv,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
  OrderingMode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalOutputFetch,
  RemoteOutputFetch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  fakeCellModule,
}
import com.digitalasset.canton.tracing.Traced
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class AvailabilityModuleOutputFetchTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

    "it receives OutputFetch.FetchBatchDataFromNodes (from local store) and " +
      "it is already fetching it" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> AMissingBatchStatusNode1And2Acks
          )
          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            LocalOutputFetch.FetchBatchDataFromNodes(
              ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
              OrderingMode.Consensus,
            )
          )

          outputFetchProtocolState.localOutputMissingBatches should contain only ABatchId -> AMissingBatchStatusNode1And2Acks
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchBatchDataFromNodes (from local store) and " +
      "it is not already fetching it" should {

        "update the fetch progress, " +
          "set a fetch timeout and " +
          "send OutputFetch.FetchRemoteBatchData to the currently attempted node" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
              jitterConstructor = (_, _) => jitterStream,
            )
            availability.receive(
              LocalOutputFetch.FetchBatchDataFromNodes(
                ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                OrderingMode.Consensus,
              )
            )

            outputFetchProtocolState.localOutputMissingBatches should contain only ABatchId -> AMissingBatchStatusNode1And2Acks
            outputFetchProtocolState.incomingBatchRequests should be(empty)
            p2pNetworkOutCell.get() shouldBe None
            context.runPipedMessagesAndReceiveOnModule(availability)

            p2pNetworkOutCell.get() should matchPattern {
              case Some(
                    P2PNetworkOut.SendToRandomAuthenticated(
                      P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(signedMessage),
                      Seq(`Node1`, `Node2`),
                      Some(`FetchBatchesSingleWorkflowId`),
                      None, // nodeThatFailed
                      Some(_), // onRecipientDecision
                    )
                  )
                  if signedMessage ==
                    RemoteOutputFetch.FetchRemoteBatchData
                      .create(ABatchId, anEpochNumber, Node0)
                      .fakeSign =>
            }

            p2pNetworkOutCell
              .get()
              .getOrElse(fail("failed to get P2PNetworkOut message"))
              .asInstanceOf[P2PNetworkOut.SendToRandomAuthenticated]
              .onRecipientDecision
              .foreach(_(Some(Node1)))

            context.delayedMessages should matchPattern {
              case Seq(
                    LocalOutputFetch.FetchRemoteBatchDataTimeout(
                      Some(`Node1`),
                      `ABatchId`,
                      `anEpochNumber`,
                      _,
                    )
                  ) =>
            }
          }
      }

    "it receives OutputFetch.FetchRemoteBatchData (from node) and " +
      "there is an incoming request for the batch already" should {

        "just record the new requesting node" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, anEpochNumber, Node2)
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should contain only ABatchId -> Set(
            Node1,
            Node2,
          )
        }
      }

    "it receives OutputFetch.FetchRemoteBatchData (from node) and " +
      "there is no incoming request for the batch" should {

        "record the first requesting node and " +
          "fetch batch from local store" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()

            val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
            val availability = createAndStartAvailability[IgnoringUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              availabilityStore = availabilityStore,
            )
            availability.receive(
              RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, anEpochNumber, Node1)
            )

            outputFetchProtocolState.localOutputMissingBatches should be(empty)
            outputFetchProtocolState.incomingBatchRequests should contain only ABatchId -> Set(
              Node1
            )
            verify(availabilityStore).fetchBatches(
              Seq(BatchIdAndEpochNumber(ABatchId, anEpochNumber))
            )
          }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad (from local store) and " +
      "the batch was not found" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad and " +
      "the batch was found and " +
      "there is no incoming request for the batch" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad and " +
      "the batch is found and " +
      "there is an incoming request for the batch" should {

        "send OutputFetch.RemoteBatchDataFetched to all requesting node and " +
          "remove the batch from incoming requests" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
            val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            )
            availability.receive(
              LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
            )

            outputFetchProtocolState.localOutputMissingBatches should be(empty)
            outputFetchProtocolState.incomingBatchRequests should be(empty)

            p2pNetworkOutCell.get() shouldBe None

            context.runPipedMessagesAndReceiveOnModule(availability)
            p2pNetworkOutCell.get() should contain(
              P2PNetworkOut.Multicast(
                P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                  RemoteOutputFetch.RemoteBatchDataFetched
                    .create(Node0, ABatchId, ABatch)
                    .fakeSign
                ),
                Set(Node1),
              )
            )
          }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad and " +
      "the batch is NOT found and " +
      "there is an incoming request for the batch" should {

        "just remove the batch from incoming requests" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
          val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
          )
          availability.receive(
            LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None)
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)

          p2pNetworkOutCell.get() shouldBe None

          context.runPipedMessagesAndReceiveOnModule(availability)
          p2pNetworkOutCell.get() shouldBe None
        }
      }

    "it receives OutputFetch.RemoteBatchDataFetched and " +
      "the batch is not missing" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.RemoteBatchDataFetched and " +
      "the batch is missing" should {

        "just store the batch in the local store" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> AMissingBatchStatusNode1And2Acks
          )
          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            availabilityStore = availabilityStore,
          )
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
          )

          outputFetchProtocolState.localOutputMissingBatches should
            contain only ABatchId -> AMissingBatchStatusNode1And2Acks
          outputFetchProtocolState.incomingBatchRequests should be(empty)
          verify(availabilityStore).addBatch(ABatchId, ABatch)
        }
      }

    "it receives OutputFetch.FetchedBatchStored and " +
      "the batch is not missing" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.FetchedBatchStored(ABatchId))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchedBatchStored but the batchId doesn't match" should {

      "not store the batch" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val otherBatchId = WrongBatchId
        outputFetchProtocolState.localOutputMissingBatches.addOne(
          otherBatchId -> AMissingBatchStatusNode1And2Acks
        )
        val availability = createAndStartAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          availabilityStore = availabilityStore,
        )
        assertLogs(
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, otherBatchId, ABatch)
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include("BatchId doesn't match digest")
          },
        )

        outputFetchProtocolState.localOutputMissingBatches should contain only otherBatchId -> AMissingBatchStatusNode1And2Acks
        outputFetchProtocolState.incomingBatchRequests should be(empty)
        verifyZeroInteractions(availabilityStore)
      }
    }

    "it receives OutputFetch.FetchedBatchStored but there are more requests than allowed" should {

      "not store the batch" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        outputFetchProtocolState.localOutputMissingBatches.addOne(
          ABatchId -> AMissingBatchStatusNode1And2Acks
        )
        val availability = createAndStartAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          availabilityStore = availabilityStore,
          maxRequestsInBatch = 0,
        )
        assertLogs(
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include regex (
              """Batch BatchId\([^)]+\) from 'node1' contains more requests \(1\) than allowed \(0\), skipping"""
            )
          },
        )

        outputFetchProtocolState.localOutputMissingBatches should
          contain only ABatchId -> AMissingBatchStatusNode1And2Acks
        outputFetchProtocolState.incomingBatchRequests should be(empty)
        verifyZeroInteractions(availabilityStore)
      }
    }

    "it receives OutputFetch.FetchedBatchStored and " +
      "the batch is missing" should {

        "just remove it from missing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> AMissingBatchStatusNode1And2Acks
          )
          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.FetchedBatchStored(ABatchId))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchRemoteBatchDataTimeout and " +
      "the batch is not missing" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAndStartAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            LocalOutputFetch.FetchRemoteBatchDataTimeout(
              Some(Node1),
              ABatchId,
              anEpochNumber,
              1.second,
            )
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchRemoteBatchDataTimeout, " +
      "the batch is missing" should {

        "update the fetch progress with the remaining nodes, " +
          "update the missing batches, " +
          "set a fetch timeout and " +
          "send OutputFetch.FetchRemoteBatchData" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> AMissingBatchStatusNode1And2Acks
            )
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
              otherNodes = Set(Node1),
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            )
            availability.receive(
              LocalOutputFetch.FetchRemoteBatchDataTimeout(
                Some(Node1),
                ABatchId,
                anEpochNumber,
                1.second,
              )
            )

            outputFetchProtocolState.localOutputMissingBatches should
              contain only ABatchId -> AMissingBatchStatusNode1And2Acks.copy(numberOfAttempts = 2)
            outputFetchProtocolState.incomingBatchRequests should be(empty)
            p2pNetworkOutCell.get() shouldBe None
            context.runPipedMessagesAndReceiveOnModule(availability)

            p2pNetworkOutCell.get() should matchPattern {
              case Some(
                    P2PNetworkOut.SendToRandomAuthenticated(
                      signedMessage,
                      Seq(`Node1`, `Node2`),
                      Some(`FetchBatchesSingleWorkflowId`),
                      Some(`Node1`), // nodeThatFailed
                      Some(_), // onRecipientDecision
                    )
                  )
                  if signedMessage == P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                    RemoteOutputFetch.FetchRemoteBatchData
                      .create(ABatchId, anEpochNumber, Node0)
                      .fakeSign
                  ) =>
            }

            p2pNetworkOutCell
              .get()
              .getOrElse(fail("failed to get P2PNetworkOut message"))
              .asInstanceOf[P2PNetworkOut.SendToRandomAuthenticated]
              .onRecipientDecision
              .foreach(_(Some(Node2)))

            context.delayedMessages should matchPattern {
              case Seq(
                    LocalOutputFetch.FetchRemoteBatchDataTimeout(
                      Some(`Node2`),
                      `ABatchId`,
                      `anEpochNumber`,
                      _,
                    )
                  ) =>
            }
          }
      }

    "it receives " +
      "Dissemination.StoreLocalBatch, " +
      "Dissemination.StoreRemoteBatch " +
      "and OutputFetch.StoreFetchedBatch" should {

        type Msg = Availability.Message[ProgrammableUnitTestEnv]

        "store the batch" in {
          forAll(
            Table[Msg, Msg](
              ("message", "reply"),
              (
                Availability.LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest)),
                Availability.LocalDissemination.LocalBatchesStored(Seq(Traced(ABatchId) -> ABatch)),
              ),
              (
                Availability.RemoteDissemination.RemoteBatch.create(
                  ABatchId,
                  ABatch,
                  Node0,
                ),
                Availability.LocalDissemination
                  .RemoteBatchStored(ABatchId, anEpochNumber, Node0, addedToStore = true),
              ),
              (
                Availability.RemoteOutputFetch.RemoteBatchDataFetched.create(
                  Node0,
                  ABatchId,
                  ABatch,
                ),
                Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
              ),
            )
          ) { (message, reply) =>
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> MissingBatchStatus(
                ABatchId,
                ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                numberOfAttempts = 1,
                jitterStream = jitterStream,
                orderingMode = OrderingMode.Consensus,
              )
            )
            val storage = TrieMap[BatchId, OrderingRequestBatch]()
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              availabilityStore = new FakeAvailabilityStore[ProgrammableUnitTestEnv](storage),
            )

            availability.receive(message)

            storage shouldBe empty
            context.runPipedMessages() shouldBe Seq(reply)
            storage should contain only (ABatchId -> ABatch)
          }
        }

        "LocalBatchStored and RemoteBatchStored should be signed" in {
          forAll(
            Table[Msg, Msg, String](
              ("message", "reply", "crypto operation ID"),
              (
                Availability.LocalDissemination.LocalBatchesStored(Seq(Traced(ABatchId) -> ABatch)),
                Availability.LocalDissemination
                  .LocalBatchesStoredSigned(
                    Seq(
                      LocalBatchStoredSigned(
                        Traced(ABatchId),
                        ABatch,
                        MembershipNode0,
                        Some(Signature.noSignature),
                      )
                    )
                  ),
                "availability-sign-local-batchId",
              ),
              (
                Availability.LocalDissemination
                  .RemoteBatchStored(ABatchId, anEpochNumber, Node0, addedToStore = true),
                Availability.LocalDissemination
                  .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
                "availability-sign-remote-batchId",
              ),
            )
          ) { case (message, reply, cryptoOperationId) =>
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
            val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
              cryptoProvider = cryptoProvider
            )

            availability.receive(message)

            context.runPipedMessages() shouldBe Seq(reply)
            verify(cryptoProvider).signHash(
              AvailabilityAck.hashFor(ABatchId, anEpochNumber, Node0, metrics),
              cryptoOperationId,
            )
          }
        }

        "after having stored a remote batch, " +
          "update the pending requests and fetch it " +
          "when it's the only one missing" in {
            forAll(
              Table[Msg](
                "message",
                Availability.LocalDissemination
                  .RemoteBatchStored(ABatchId, anEpochNumber, Node0, addedToStore = true),
                Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
              )
            ) { message =>
              val singleBatchMissingRequest = new BatchesRequest(
                AnOrderedBlockForOutput,
                missingBatches = mutable.SortedSet(ABatchId),
                traceContext,
              )

              val outputFetchProtocolState = new MainOutputFetchProtocolState()
              outputFetchProtocolState.pendingBatchesRequests.addOne(singleBatchMissingRequest)
              outputFetchProtocolState.localOutputMissingBatches.addOne(
                ABatchId -> MissingBatchStatus(
                  ABatchId,
                  ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                  numberOfAttempts = 1,
                  jitterStream = jitterStream,
                  orderingMode = OrderingMode.Consensus,
                )
              )
              implicit val context
                  : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
                new ProgrammableUnitTestContext

              val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv]())
              val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
                availabilityStore = availabilityStore,
                outputFetchProtocolState = outputFetchProtocolState,
                cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              )
              val result = mock[AvailabilityStore.FetchBatchesResult]
              when(
                availabilityStore.fetchBatches(Seq(BatchIdAndEpochNumber(ABatchId, anEpochNumber)))
              ) thenReturn (() => result)

              availability.receive(message)

              outputFetchProtocolState.pendingBatchesRequests shouldBe empty
              singleBatchMissingRequest.missingBatches shouldBe empty
              context.runPipedMessages() should contain(
                LocalOutputFetch.FetchedBlockDataFromStorage(singleBatchMissingRequest, result)
              )
            }
          }

        "after having stored a remote batch, " +
          "update the pending requests and " +
          "don't fetch it when batches are still missing" in {
            forAll(
              Table[Msg](
                "message",
                Availability.LocalDissemination
                  .RemoteBatchStored(ABatchId, anEpochNumber, Node0, addedToStore = true),
                Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
              )
            ) { message =>
              val multipleBatchMissingRequest = new BatchesRequest(
                AnotherOrderedBlockForOutput,
                missingBatches = mutable.SortedSet(ABatchId, AnotherBatchId),
                traceContext,
              )

              val outputFetchProtocolState = new MainOutputFetchProtocolState()
              outputFetchProtocolState.pendingBatchesRequests.addOne(multipleBatchMissingRequest)
              outputFetchProtocolState.localOutputMissingBatches.addOne(
                ABatchId -> MissingBatchStatus(
                  ABatchId,
                  ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                  numberOfAttempts = 1,
                  jitterStream = jitterStream,
                  orderingMode = OrderingMode.Consensus,
                )
              )
              implicit val context
                  : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
                new ProgrammableUnitTestContext

              val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv]())
              val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
                availabilityStore = availabilityStore,
                outputFetchProtocolState = outputFetchProtocolState,
                cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              )

              availability.receive(message)

              outputFetchProtocolState.pendingBatchesRequests.size should be(1)
              outputFetchProtocolState
                .pendingBatchesRequests(0)
                .missingBatches should contain only AnotherBatchId
              context.runPipedMessages()
              multipleBatchMissingRequest.missingBatches.toSet shouldBe Set(AnotherBatchId)
              verifyZeroInteractions(availabilityStore)
            }
          }
      }

    "it receives OutputFetch.FetchedBlockDataFromStorage and there are no missing batches" should {

      "send to output" in {
        val storage = TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
        val availabilityStore =
          spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv](storage))
        val cellContextFake =
          new AtomicReference[
            Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
          ](None)
        val expectedOutputCell =
          new AtomicReference[Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]](None)
        implicit val context: FakePipeToSelfCellUnitTestContext[
          Availability.Message[FakePipeToSelfCellUnitTestEnv]
        ] =
          FakePipeToSelfCellUnitTestContext(cellContextFake)
        val availability = createAndStartAvailability(
          availabilityStore = availabilityStore,
          output = fakeCellModule(expectedOutputCell),
        )
        val request =
          new BatchesRequest(AnOrderedBlockForOutput, mutable.SortedSet(ABatchId), traceContext)

        availability.receive(
          Availability.LocalOutputFetch.FetchedBlockDataFromStorage(
            request,
            AvailabilityStore.AllBatches(Seq(ABatchId -> ABatch)),
          )
        )

        expectedOutputCell.get() shouldBe Some(
          Output.BlockDataFetched(ACompleteBlock)
        )
      }
    }

    "it receives OutputFetch.FetchedBlockDataFromStorage and there are missing batches" should {

      "record the missing batches and ask other nodes for missing data" in {
        forAll(
          Table[OrderingMode, Seq[BftNodeId]](
            ("orderingMode mode", "possible recipients"),
            (OrderingMode.Consensus, Seq(Node1, Node2)),
            // Ignore nodes from the PoA, use the current topology
            (OrderingMode.StateTransfer, Seq(Node3)),
          )
        ) { (orderingMode, possibleRecipients) =>
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val expectedMessageCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val cellNetwork = fakeCellModule(expectedMessageCell)
          val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv])
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAndStartAvailability(
            otherNodes = Set(Node3),
            availabilityStore = availabilityStore,
            outputFetchProtocolState = outputFetchProtocolState,
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            p2pNetworkOut = cellNetwork,
          )

          val request = new BatchesRequest(
            OrderedBlockForOutput(
              OrderedBlock(
                ABlockMetadata,
                Seq(ProofOfAvailabilityNode1And2AcksNode1And2InTopology),
                CanonicalCommitSet(Set.empty),
              ),
              ViewNumber.First,
              isLastInEpoch = false, // Irrelevant for availability
              originalLeader = Node0,
              orderingMode = orderingMode,
            ),
            mutable.SortedSet(ABatchId),
            traceContext,
          )
          outputFetchProtocolState.pendingBatchesRequests.addOne(request)

          availability.receive(
            Availability.LocalOutputFetch
              .FetchedBlockDataFromStorage(
                request,
                AvailabilityStore.MissingBatches(
                  Set(BatchIdAndEpochNumber(ABatchId, anEpochNumber))
                ),
              )
          )

          outputFetchProtocolState.pendingBatchesRequests.size should be(1)
          outputFetchProtocolState
            .pendingBatchesRequests(0)
            .missingBatches should contain only ABatchId

          expectedMessageCell.get() shouldBe None

          context.runPipedMessagesAndReceiveOnModule(availability)

          expectedMessageCell.get() should matchPattern {
            case Some(
                  P2PNetworkOut.SendToRandomAuthenticated(
                    signedMessage,
                    `possibleRecipients`,
                    Some(`FetchBatchesSingleWorkflowId`),
                    None, // nodeThatFailed
                    Some(_), // onRecipientDecision
                  )
                )
                if signedMessage == P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                  Availability.RemoteOutputFetch.FetchRemoteBatchData
                    .create(ABatchId, anEpochNumber, from = Node0)
                    .fakeSign
                ) =>
          }
        }
      }
    }

    "it receives OutputFetch.FetchedBlockDataFromStorage and there are missing batches that are pending storage" should {
      "not ask other nodes for missing batches" in {
        implicit val context
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val expectedMessageCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
        val cellNetwork = fakeCellModule(expectedMessageCell)
        val availability = createAndStartAvailability[ProgrammableUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          p2pNetworkOut = cellNetwork,
        )
        outputFetchProtocolState.pendingRemoteBatchIdsToStore.add(ABatchId)
        val singleBatchMissingRequest = new BatchesRequest(
          AnOrderedBlockForOutput,
          missingBatches = mutable.SortedSet(ABatchId),
          traceContext,
        )
        assertLogs(
          availability.receive(
            Availability.LocalOutputFetch.FetchedBlockDataFromStorage(
              singleBatchMissingRequest,
              AvailabilityStore.MissingBatches(Set(BatchIdAndEpochNumber(ABatchId, anEpochNumber))),
            )
          ),
          log => {
            log.level shouldBe Level.DEBUG
            log.message should include(
              s"Missing batch $ABatchId is actually in the process of being stored"
            )
          },
        )
        context.runPipedMessagesAndReceiveOnModule(availability)
        expectedMessageCell.get() shouldBe None
      }
    }

    "it receives OutputFetch.LoadBatchData and the batch is present" should {

      "reply to local availability with the batch" in {
        val storage = TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
        val cellContextFake =
          new AtomicReference[
            Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
          ](None)
        val availabilityStore =
          spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv](storage))
        implicit val context: FakePipeToSelfCellUnitTestContext[
          Availability.Message[FakePipeToSelfCellUnitTestEnv]
        ] =
          FakePipeToSelfCellUnitTestContext(cellContextFake)
        val availability = createAndStartAvailability(
          availabilityStore = availabilityStore
        )

        availability.receive(
          Availability.RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, anEpochNumber, Node0)
        )

        cellContextFake.get() shouldBe defined
        cellContextFake.get().foreach { f =>
          f() shouldBe Some(
            Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
          )
        }
      }
    }

    "it receives OutputFetch.LoadBatchData and the batch is missing" should {

      "reply to local availability without the batch" in {
        val cellContextFake =
          new AtomicReference[
            Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
          ](None)
        val availabilityStore = spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv])
        implicit val context: FakePipeToSelfCellUnitTestContext[
          Availability.Message[FakePipeToSelfCellUnitTestEnv]
        ] =
          FakePipeToSelfCellUnitTestContext(cellContextFake)
        val availability = createAndStartAvailability(
          availabilityStore = availabilityStore
        )

        availability.receive(
          Availability.RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, anEpochNumber, Node0)
        )

        cellContextFake.get() shouldBe defined
        cellContextFake.get().foreach { f =>
          f() shouldBe
            Some(Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None))
        }
      }
    }
  }
}
