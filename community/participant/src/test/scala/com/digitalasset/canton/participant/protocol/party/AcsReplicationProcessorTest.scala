// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.SignatureCheckError.InvalidSignature
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{
  Hash,
  LtHash16Blake3,
  Signature,
  SigningKeyUsage,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
  TestHash,
}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.ledger.participant.state.SynchronizerUpdate
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.data.{ActiveContract, RepairContract}
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationStatus,
  PartyReplicationTestInterceptor,
}
import com.digitalasset.canton.participant.config.OnlinePartyReplicationTargetConfig
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.party.acsreplication.{
  AcsReplicationProcessor,
  AcsReplicationSourceParticipantMessage,
  AcsReplicationTargetParticipantMessage,
  AcsReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.store.{
  AcsReplicationStateManager,
  PartyReplicationIndexingStore,
}
import com.digitalasset.canton.participant.util.{CreatesActiveContracts, TimeOfChange}
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ReassignmentTag}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  NeedsNewLfContractIds,
  ProtocolVersionChecksFixtureAsyncWordSpec,
  ReassignmentCounter,
  RepairCounter,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

sealed trait AcsReplicationProcessorTestBaseWithFixture
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with NeedsNewLfContractIds
    with CreatesActiveContracts {
  override protected val testSymbolicCrypto = new SymbolicPureCrypto()

  class Env(cryptoClientO: Option[SynchronizerCryptoClient] = None) extends FlagCloseableAsync {
    override val timeouts: ProcessingTimeout =
      AcsReplicationProcessorTestBaseWithFixture.this.timeouts
    protected val logger: TracedLogger = AcsReplicationProcessorTestBaseWithFixture.this.logger
    val tp: AcsReplicationTargetParticipantProcessor = mkTP()

    val messagesSentByTP =
      mutable.Buffer[(String, AcsReplicationTargetParticipantMessage.Instruction)]()

    private var hasTestProcessorCompleted: Boolean = false
    var tpProceedOrWait: PartyReplicationTestInterceptor.ProceedOrWait =
      PartyReplicationTestInterceptor.Proceed
    var tpSendErrorOverrides: Map[String, String] = Map.empty
    var contractsReceived: Map[LfContractId, ReassignmentCounter] = Map.empty
    var persistContractMaybeInjectedError: EitherT[FutureUnlessShutdown, String, Unit] =
      EitherTUtil.unitUS

    def targetProcessor: AcsReplicationTargetParticipantProcessor = tp

    private def mkTP(): AcsReplicationTargetParticipantProcessor = {
      val rop = mock[RecordOrderPublisher]
      val requestTracker = mock[RequestTracker]
      val indexingStore = mock[PartyReplicationIndexingStore]
      when(
        rop.schedulePublishAddContracts(any[CantonTimestamp => SynchronizerUpdate])(
          anyTraceContext
        )
      ).thenReturn(UnlessShutdown.unit)
      when(rop.publishBufferedEvents()).thenReturn(UnlessShutdown.unit)
      when(
        requestTracker.addReplicatedContracts(
          any[Hash],
          any[CantonTimestamp],
          any[Seq[
            (
                LfContractId,
                ReassignmentTag.Source[SynchronizerId],
                ReassignmentCounter,
                TimeOfChange,
            )
          ]],
        )(anyTraceContext)
      )
        .thenReturn(EitherTUtil.unitUS)
      when(
        indexingStore.addImportedContractActivations(
          any[PartyReplicationIndexingStore.Watermark],
          any[NonEmpty[Seq[ContractReassignment]]],
        )(anyTraceContext)
      )
        .thenReturn(FutureUnlessShutdown.unit)

      val inMemoryStorageForTesting = new MemoryStorage(loggerFactory, timeouts)
      val sourceParticipantId = DefaultTestIdentities.participant1
      val targetParticipantId = DefaultTestIdentities.participant2
      val initialStatus = PartyReplicationStatus(
        PartyReplicationStatus.ReplicationParams(
          addPartyRequestId,
          alice,
          psid.logical,
          sourceParticipantId,
          targetParticipantId,
          PositiveInt.one,
          ParticipantPermission.Submission,
        ),
        testedProtocolVersion,
        replicationO = Some(
          PartyReplicationStatus.PersistentProgress(
            processedContractCount = NonNegativeLong.zero,
            nextPersistenceCounter = RepairCounter.Genesis,
            acsHashO = None,
            fullyProcessedAcs = false,
          )
        ),
      )

      def inMemoryAcsReplicationStateManager =
        new AcsReplicationStateManager(
          targetParticipantId,
          inMemoryStorageForTesting,
          futureSupervisor,
          exitOnFatalFailures = false,
          loggerFactory,
          timeouts,
        ).tap(_.add(initialStatus).value.futureValueUS.value)

      val persistsContracts = new TargetParticipantAcsPersistence.PersistsContracts {
        def persistContracts(
            contracts: NonEmpty[Seq[ContractInstance]]
        )(implicit
            executionContext: ExecutionContext,
            traceContext: TraceContext,
        ): EitherT[FutureUnlessShutdown, String, Map[LfContractId, Long]] =
          EitherT(
            persistContractMaybeInjectedError.value.map(
              _.map(_ =>
                contracts.forgetNE.zipWithIndex.map { case (contract, idx) =>
                  contract.contractId -> idx.toLong
                }.toMap
              )
            )(executionContext)
          )
      }
      val processorLoggerFactory = loggerFactory
      val replicationProgressState = inMemoryAcsReplicationStateManager

      val acsTransferContractHandler = new TargetParticipantAcsPersistence(
        requestId = addPartyRequestId,
        psid = psid,
        partyOnboardingAt = EffectiveTime(CantonTimestamp.ofEpochSecond(10)),
        persistsContracts = persistsContracts,
        requestTracker = requestTracker,
        indexingStore = indexingStore,
        loggerFactory = processorLoggerFactory,
      )

      new AcsReplicationTargetParticipantProcessor(
        psid = psid,
        partyId = alice,
        requestId = addPartyRequestId,
        partyOnboardingAt = EffectiveTime(CantonTimestamp.ofEpochSecond(10)),
        excludedStakeholders = Set.empty,
        sourceParticipantId = sourceParticipantId,
        agreedAt = CantonTimestamp.Epoch,
        replicationProgressState = replicationProgressState,
        onError = logger.info(_),
        onDisconnect = logger.info(_)(_),
        acsTransferContractHandler = acsTransferContractHandler,
        config = OnlinePartyReplicationTargetConfig(disableAcsDigestSourceSignatureValidation =
          cryptoClientO.isEmpty
        ),
        futureSupervisor = futureSupervisor,
        exitOnFatalFailures = true,
        timeouts = timeouts,
        loggerFactory = processorLoggerFactory,
        testOnlyInterceptor = new PartyReplicationTestInterceptor {
          override def onTargetParticipantProgress(
              progress: PartyReplicationStatus.AcsReplicationProgress
          )(implicit
              traceContext: TraceContext
          ): PartyReplicationTestInterceptor.ProceedOrWait = tpProceedOrWait
        },
      ) {
        override protected def sendPayload(operation: String, payload: ByteString)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, Unit] =
          sendMaybeOverridden(operation)(
            EitherT(
              FutureUnlessShutdown.pure(
                AcsReplicationTargetParticipantMessage
                  .fromByteString(protocolVersion, payload)
                  .bimap(
                    _.message,
                    msg => {
                      messagesSentByTP.append(operation -> msg.instruction)
                      this.logger.info(s"Sent message: $operation")
                    },
                  )
              )
            )
          )

        override def sendCompleted(status: String)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, Unit] = {
          hasTestProcessorCompleted = true
          sendMaybeOverridden("completed") {
            this.logger.info(s"Sent completed: $status")
            EitherTUtil.unitUS
          }
        }

        override def sendError(error: String)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, Unit] = {
          hasTestProcessorCompleted = true
          this.logger.info(s"Sent error: $error")
          EitherTUtil.unitUS
        }

        private def sendMaybeOverridden(operation: String)(
            send: => EitherT[FutureUnlessShutdown, String, Unit]
        ): EitherT[FutureUnlessShutdown, String, Unit] =
          tpSendErrorOverrides
            .get(operation)
            .fold {
              send
            } { err =>
              this.logger.info(s"Send \"$operation\" overridden with error: $err")
              EitherT(FutureUnlessShutdown.pure(Left(err): Either[String, Unit]))
            }

        override def hasChannelCompleted: Boolean = hasTestProcessorCompleted

        override def getCryptoClient: Option[SynchronizerCryptoClient] = cryptoClientO
      }
    }

    lazy val acsDigest = AcsReplicationSourceParticipantMessage.AcsDigest(
      acsHash = contractsReceived
        .foldLeft(LtHash16Blake3.empty) { case (digest, (cid, ctr)) =>
          digest.tap(
            _.add(acsreplication.PartyReplicationAcsDigestHelper.bytesFromReassignment(cid, ctr))
          )
        }
        .getByteString,
      getAcsArgs = AcsReplicationSourceParticipantMessage.GetAcsArguments(
        partyId = alice,
        synchronizerId = psid.logical,
        asOf = CantonTimestamp.ofEpochSecond(10),
        excludedStakeholders = Set.empty,
      ),
      sourceParticipantUid = DefaultTestIdentities.participant1.uid,
      agreedAt = CantonTimestamp.Epoch,
      protocolVersion = testedProtocolVersion,
    )

    lazy val endOfAcs: ByteString =
      AcsReplicationSourceParticipantMessage(
        AcsReplicationSourceParticipantMessage.EndOfAcs(
          acsDigest,
          acsDigest.toByteString,
          Signature.noSignature,
        ),
        testedProtocolVersion,
      ).toByteString

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("tp-processor", tp.close())
    )
  }

  protected def execUntilDone[P <: AcsReplicationProcessor](processor: P, clue: String)(
      test: P => EitherT[FutureUnlessShutdown, String, Unit]
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Begin \"$clue\"")
    valueOrFail(test(processor))(clue)
      .map { _ =>
        eventually()(processor.isExecutionQueueEmpty shouldBe true)
        logger.debug(s"End \"$clue\"")
      }
  }

  protected def execUntilDone[P <: AcsReplicationProcessor](processor: P)(
      test: P => Unit
  ): Unit = {
    test(processor)
    eventually()(processor.isExecutionQueueEmpty shouldBe true)
  }

  override type FixtureParam = Env

  private val alice = PartyId.tryFromProtoPrimitive("alice::default")

  private val addPartyRequestId = TestHash.digest(0)

  override protected val psid: PhysicalSynchronizerId =
    PhysicalSynchronizerId(
      DefaultTestIdentities.synchronizerId,
      defaultStaticSynchronizerParameters,
    )

  protected def createAcsBatch(n: PositiveInt): ByteString = createAcsBatch(createAcs(n))

  protected def createAcsBatch(contracts: Seq[ActiveContract]): ByteString =
    AcsReplicationSourceParticipantMessage(
      AcsReplicationSourceParticipantMessage.AcsBatch(
        NonEmpty
          .from(contracts)
          .getOrElse(fail("should not be empty"))
      ),
      testedProtocolVersion,
    ).toByteString

  protected def createAcs(n: PositiveInt): Seq[ActiveContract] =
    (0 until n.unwrap).map(_ => createActiveContract())

  protected def contractIdsWithCounters(
      contracts: Seq[ActiveContract]
  ): Map[LfContractId, ReassignmentCounter] =
    contracts
      .traverse { ac =>
        RepairContract
          .fromLapiActiveContract(ac.contract)
          .map(rc => (rc.contract.contractId, rc.reassignmentCounter))
      }
      .getOrElse(fail("bad contract"))
      .toMap
}

/** The PartyReplicationProcessorTest tests that the OnPR ACS replication protocol implemented by
  * the source and target processors correctly handle expected and unexpected messages from the
  * counter participant and local error conditions.
  */
final class AcsReplicationProcessorTest extends AcsReplicationProcessorTestBaseWithFixture {

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "TargetParticipantPartyReplicationProcessor" when {

    "observing valid interaction" should {
      "initialize" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        for {
          _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
        } yield {
          val messagesSent = eventually()(env.messagesSentByTP.toList.tap(_.size shouldBe 2))
          messagesSent.head._2 shouldBe AcsReplicationTargetParticipantMessage.Initialize(
            NonNegativeLong.zero
          )
          val firstBatchEndOrdinal = NonNegativeLong.tryCreate(
            TargetParticipantAcsPersistence.contractsToRequestEachTime.decrement.unwrap.toLong
          )
          messagesSent(1)._2 shouldBe AcsReplicationTargetParticipantMessage.SendAcsUpTo(
            firstBatchEndOrdinal
          )
        }
      }

      "handle empty ACS" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        for {
          _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
          _ <- execUntilDone(tp, "handle empty acs")(
            _.handlePayload(endOfAcs)
          )
          _ = execUntilDone(tp)(_.progressAcsReplication())
        } yield tp.hasChannelCompleted shouldBe true
      }

      "handle single ACS batch" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        val firstBatchSize = TargetParticipantAcsPersistence.contractsToRequestEachTime
        for {
          _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
          acs = createAcs(firstBatchSize)
          _ = { env.contractsReceived = contractIdsWithCounters(acs) }
          acsBatch = createAcsBatch(acs)
          _ <- execUntilDone(tp, "receive acs batch")(
            _.handlePayload(acsBatch)
          )
          _ <- execUntilDone(tp, "end of acs")(_.handlePayload(endOfAcs))
          _ = execUntilDone(tp)(_.progressAcsReplication())
        } yield {
          tp.hasChannelCompleted shouldBe true
          tp.replicatedContractsCount shouldBe firstBatchSize.toNonNegative
        }
      }
    }

    "encountering invalid messages from source participant (SP)" should {
      "complain if SP sends malformed message" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        for {
          _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
          malformedMessage = ByteString.copyFromUtf8("not a valid message")
          err <- leftOrFail(tp.handlePayload(malformedMessage))("fail on malformed message")
        } yield {
          err should include regex "Failed to parse payload message from SP: .*"
        }
      }

      "complain if SP sends message before channel initialization" onlyRunWith ProtocolVersion.dev inUS {
        env =>
          import env.*
          for {
            errAcsBatch <- leftOrFail(tp.handlePayload(createAcsBatch(PositiveInt.one)))(
              "fail on premature AcsBatch"
            )
            errEndOfAcs <- leftOrFail(tp.handlePayload(endOfAcs))("fail on premature EndOfAcs")
          } yield {
            errAcsBatch should include regex "Received unexpected message from SP before initialized by TP: .*AcsBatch"
            errEndOfAcs should include regex "Received unexpected message from SP before initialized by TP: .*EndOfAcs"
          }
      }

      "complain if SP sends more contract batches than requested" onlyRunWith ProtocolVersion.dev inUS {
        env =>
          import env.*
          val batchSize = TargetParticipantAcsPersistence.contractsToRequestEachTime
          for {
            _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
            // Make the TP processor wait to prevent it from automatically requesting more contracts
            _ = env.tpProceedOrWait = PartyReplicationTestInterceptor.Wait
            _ <- execUntilDone(tp, "receive requested")(_.handlePayload(createAcsBatch(batchSize)))
            // Sending another, unrequested batch should cause an error
            errTooMany <- leftOrFail(tp.handlePayload(createAcsBatch(batchSize)))(
              "fail on premature AcsBatch"
            )
          } yield {
            errTooMany should include regex "Received too many contracts from SP: processed .* received .* > requested"
            tp.replicatedContractsCount shouldBe batchSize.toNonNegative
            tp.hasChannelCompleted shouldBe true
          }
      }

      "complain if SP sends more contracts than requested" onlyRunWith ProtocolVersion.dev inUS {
        env =>
          import env.*
          val batchSizeTooLarge =
            TargetParticipantAcsPersistence.contractsToRequestEachTime.increment.value
          for {
            _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
            // Make the TP processor wait to prevent it from automatically requesting more contracts
            _ = env.tpProceedOrWait = PartyReplicationTestInterceptor.Wait
            errTooMany <- leftOrFail(tp.handlePayload(createAcsBatch(batchSizeTooLarge)))(
              "fail on AcsBatch with more contracts than requested"
            )
          } yield {
            errTooMany should include regex "Received too many contracts from SP: processed .* received .* > requested"
            tp.replicatedContractsCount shouldBe NonNegativeInt.zero
            tp.hasChannelCompleted shouldBe true
          }
      }

      "complain if SP sends another message after EndOfACS" onlyRunWith ProtocolVersion.dev inUS {
        env =>
          import env.*
          for {
            _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
            _ <- execUntilDone(tp, "end of acs")(_.handlePayload(endOfAcs))
            errEndOfACSNotLast <- leftOrFail(tp.handlePayload(createAcsBatch(PositiveInt.one)))(
              "fail on ACS batch after EndOfACS"
            )
          } yield {
            errEndOfACSNotLast should include("Received ACS batch from SP after EndOfACS at")
            tp.hasChannelCompleted shouldBe true
          }
      }
    }

    "encountering local problems on target participant (TP)" should {
      "fail double initialize" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        for {
          _ <- valueOrFail(tp.onConnected())("initialize tp")
          err <- leftOrFail(tp.onConnected())("double initialize tp")
        } yield {
          err shouldBe "Channel already connected"
        }
      }

      "log upon problem sending message to SP" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        // Fail TP send on the second message to request more contracts
        val acsBatchSize = TargetParticipantAcsPersistence.contractsToRequestEachTime
        val secondRequestUpperBound = acsBatchSize.unwrap * 2 - 1
        tpSendErrorOverrides = Map(
          s"request next set of contracts up to ordinal $secondRequestUpperBound" -> "simulated request batch error"
        )
        loggerFactory.assertLogs(
          for {
            _ <- valueOrFail(tp.onConnected())("initialize tp")
            _ <- execUntilDone(tp, "receive acs batch")(
              _.handlePayload(
                createAcsBatch(
                  TargetParticipantAcsPersistence.contractsToRequestEachTime
                )
              )
            )
          } yield {
            // send error observed via logged warning instead of here due to async send
            succeed
          },
          _.warningMessage should include regex "Respond to source participant .* failed .*simulated request batch error",
        )
      }

      "log upon send completed notification failure" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*
        tpSendErrorOverrides = Map(s"completed" -> "simulated complete error")
        loggerFactory.assertLogs(
          for {
            _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
            _ <- execUntilDone(tp, "handle empty acs")(
              _.handlePayload(endOfAcs)
            )
            _ = execUntilDone(tp)(_.progressAcsReplication())
          } yield { tp.hasChannelCompleted shouldBe true },
          _.warningMessage should include regex "Respond to source participant .* failed .*simulated complete error",
        )
      }

      "error when unable to persist contracts" onlyRunWith ProtocolVersion.dev inUS { env =>
        import env.*

        persistContractMaybeInjectedError =
          EitherT.fromEither[FutureUnlessShutdown](Left("simulated persist contracts error"))

        for {
          _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
          err <- leftOrFail(tp.handlePayload(createAcsBatch(PositiveInt.one)))(
            "fail on persist contracts"
          )
        } yield {
          err should include regex "Failed to persist contracts:.* simulated persist contracts error"
          // channel completed because of error
          tp.hasChannelCompleted shouldBe true
        }
      }
    }
  }
}

/** The AcsReplicationProcessorBadSourceParticipantSignatureTest tests that the target participant
  * processor flags problems with the SP ACS digest signature.
  */
final class AcsReplicationProcessorBadSourceParticipantSignatureTest
    extends AcsReplicationProcessorTestBaseWithFixture {

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val snapshotCryptoClient = mock[SynchronizerSnapshotSyncCryptoApi]
    val cryptoClient = mock[SynchronizerCryptoClient]
    when(
      cryptoClient.snapshot(any[CantonTimestamp])(anyTraceContext)
    ).thenReturn(FutureUnlessShutdown.pure(snapshotCryptoClient))
    when(
      snapshotCryptoClient.verifySignature(
        any[Hash],
        any[Member],
        any[Signature],
        any[NonEmpty[Set[SigningKeyUsage]]],
      )(anyTraceContext)
    ).thenReturn(
      EitherT.leftT[FutureUnlessShutdown, Unit](
        InvalidSignature(Signature.noSignature, ByteString.empty(), "signature mismatch")
      )
    )

    val env = new Env(Some(cryptoClient))

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "TargetParticipantPartyReplicationProcessor" when {

    "complain if SP signs ACS digest with invalid signature" onlyRunWith ProtocolVersion.dev inUS {
      env =>
        import env.*
        val firstBatchSize = TargetParticipantAcsPersistence.contractsToRequestEachTime
        for {
          _ <- execUntilDone(tp, "initialize tp")(_.onConnected())
          acs = createAcs(firstBatchSize)
          _ = { env.contractsReceived = contractIdsWithCounters(acs) }
          acsBatch = createAcsBatch(acs)
          _ <- execUntilDone(tp, "receive acs batch")(
            _.handlePayload(acsBatch)
          )
          err <- leftOrFail(tp.handlePayload(endOfAcs))("signature failure")
        } yield {
          err should include(
            "Problem with Source Participant ACS digest signature: InvalidSignature"
          )
        }
    }
  }
}
