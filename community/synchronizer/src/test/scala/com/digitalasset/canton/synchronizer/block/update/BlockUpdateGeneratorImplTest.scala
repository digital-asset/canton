// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{
  HashPurpose,
  KeyName,
  SigningKeyUsage,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencing.protocol.ProtocolObjectTestUtils.normalizeSubmissionRequest
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  Batch,
  ClosedCompressedEnvelope,
  ClosedEnvelope,
  ClosedUncompressedEnvelope,
  MediatorGroupRecipient,
  MessageId,
  Recipients,
  SequencerErrors,
  SequencersOfSynchronizer,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.HasTopologyTransactionTestFactory
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.{Acknowledgment, Send}
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGenerator.{
  EndOfBlock,
  MaybeTopologyTickChunk,
  NextChunk,
}
import com.digitalasset.canton.synchronizer.block.{BlockEvents, LedgerBlockEvent}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.SubmissionOutcome
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.block.RawLedgerBlock
import com.digitalasset.canton.synchronizer.sequencer.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.time.LsuSequencingBounds
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  mediatorId,
  physicalSynchronizerId,
  sequencerId,
}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.topology.{
  MediatorGroup,
  MediatorId,
  Member,
  ParticipantId,
  SequencerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.TracedPossiblyPrevalidated
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  HasExecutorService,
  ProtocolVersionChecksAsyncWordSpec,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.time.Instant
import scala.concurrent.Future

class BlockUpdateGeneratorImplTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasExecutorService
    with ProtocolVersionChecksAsyncWordSpec
    with HasTopologyTransactionTestFactory {

  implicit lazy val closeContext: CloseContext = CloseContext(
    FlagCloseable.withCloseContext(logger, ProcessingTimeout())
  )

  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))

  private def assertBlockEventsEqual(
      actual: BlockEvents,
      expected: BlockEvents,
  ): Assertion = {
    actual.events
      .map(_.value)
      .collectFirst { case send: Send =>
        send.signedSubmissionRequest.content.batch.envelopes.headOption
      }
      .flatten
      .foreach { envelope =>
        envelope shouldBe a[ClosedCompressedEnvelope]
      }

    normalizeBlockEvents(actual) shouldBe normalizeBlockEvents(expected)
  }

  private def normalizeBlockEvents(blockEvents: BlockEvents) =
    blockEvents.copy(
      events = blockEvents.events.map { traced =>
        traced.map {
          case send: Send =>
            send.copy(
              signedSubmissionRequest = send.signedSubmissionRequest.copy(
                content = normalizeSubmissionRequest(send.signedSubmissionRequest.content)
              )
            )
          case other => other
        }
      }
    )

  private lazy val metrics = SequencerTestMetrics(this.getClass.getSimpleName)
  private lazy val parameters = BlockProcessingParameters(
    OrderingTimeFixMode.ValidateOnly,
    lsuSequencingBounds = None,
    parallelism = PositiveInt.two,
    enablePrevalidation = true,
    enableAsyncLoggingWithOutcome = false,
  )

  "BlockUpdateGeneratorImpl.processBlockChunk" should {

    val participantSenderId = topologyTransactionFactory.participant1
    val participantRecipientId = topologyTransactionFactory.participant6
    val absentMediator =
      MediatorId(UniqueIdentifier.tryCreate("absent-mediator", mediatorId.uid.namespace))
    val absentSequencer =
      SequencerId(UniqueIdentifier.tryCreate("absent-sequencer", sequencerId.uid.namespace))
    val observerMediator =
      MediatorId(UniqueIdentifier.tryCreate("observer-mediator", mediatorId.uid.namespace))
    val observerSequencer =
      SequencerId(UniqueIdentifier.tryCreate("observer-sequencer", sequencerId.uid.namespace))
    // Observer sequencers may receive events but are not authorized senders.
    //  Observer mediators in an active group are considered active for send and recipient
    //  authorization; permission to issue verdicts is checked by the protocol.
    val defaultTopology = TestingTopology()
    val authorizationTopology = defaultTopology.copy(
      mediatorGroups = defaultTopology.mediatorGroups.map(_.copy(passive = Seq(observerMediator))),
      sequencerGroup = defaultTopology.sequencerGroup.copy(passive = Seq(observerSequencer)),
    )
    val lastBUGStateTs = aTimestamp.immediatePredecessor
    val sequencingTs = aTimestamp

    "authorize senders (from)" should {

      Seq[(String, Member, Option[ParticipantAttributes])](
        (
          "discard a submission from a participant absent from the topology",
          participantSenderId,
          None,
        ),
        (
          "discard a submission from a participant unable to log in at the sequencing time",
          participantSenderId,
          Some(
            ParticipantAttributes(
              ParticipantPermission.Submission,
              Some(sequencingTs.immediateSuccessor),
            )
          ),
        ),
        (
          "discard a submission from a mediator absent from topology",
          absentMediator,
          None,
        ),
        (
          "discard a submission from a sequencer absent from topology",
          absentSequencer,
          None,
        ),
        ("discard a submission from an observer sequencer", observerSequencer, None),
      ).map { case (name, sender, attributes) => (name, sender, attributes, false, false) }
        .++(
          Seq(
            (
              "discard a submission from an active-listed mediator in a group below threshold",
              mediatorId,
              None,
              true,
              false,
            ),
            (
              "discard a submission from an observer mediator in a group below threshold",
              observerMediator,
              None,
              true,
              false,
            ),
            (
              "discard a submission from an unregistered sender through PV 37",
              participantSenderId,
              Some(ParticipantAttributes(ParticipantPermission.Submission, None)),
              false,
              true,
            ),
          )
        )
        .foreach {
          case (testName, requestSender, participantSenderPermissionO, inactiveGroup, legacy) =>
            testName onlyRunWhen (legacy == (testedProtocolVersion <= ProtocolVersion.v37)) in {
              val topology = authorizationTopology
                .withParticipants(
                  participantSenderPermissionO.toList.map(participantSenderId -> _)*
                )
                .build(topologyTransactionFactory.crypto, loggerFactory)
              val memberValidator = mock[SequencerMemberValidator]
              when(memberValidator.isMemberRegisteredAt(requestSender, sequencingTs))
                .thenReturn(FutureUnlessShutdown.pure(!legacy))
              val originalCryptoApi =
                topology.forOwnerAndSynchronizer(
                  sequencerId,
                  physicalSynchronizerId,
                  sequencingTs,
                )
              val syncCryptoApi =
                if (inactiveGroup)
                  withMediatorGroup(
                    originalCryptoApi,
                    authorizationTopology.mediatorGroups.head.copy(threshold = PositiveInt.two),
                  )
                else originalCryptoApi

              val blockUpdateGenerator =
                new BlockUpdateGeneratorImpl(
                  syncCryptoApi,
                  sequencerId,
                  mock[SequencerRateLimitManager],
                  drSequencingTimeUpperBound = None,
                  getAnnouncedLsu = None,
                  consistencyChecks = true, // Not relevant for this test
                  parameters = parameters,
                  metrics,
                  memberValidator,
                  loggerFactory,
                )
              val state = BlockUpdateGenerator.AccumulatedStateProcessingBlocks(
                lastBlockTs = lastBUGStateTs,
                lastChunkTs = lastBUGStateTs,
                latestSequencerEventTimestamp = None,
                latestPendingTopologyTransactionTimestamp = None,
                inFlightAggregations = InFlightAggregations.empty,
              )

              for {
                request <- senderSignedSubmissionRequest(
                  requestSender,
                  // Use a recognized sender/recipient pattern to avoid an unrelated submission-type
                  //  warning before sender authorization. Recipients do not affect this check.
                  Recipients.cc(requestSender match {
                    case _: SequencerId => requestSender
                    case _ => participantSenderId
                  }),
                )
                result <- loggerFactory
                  .assertLogs(rule = SuppressionRule.LevelAndAbove(Level.WARN))(
                    blockUpdateGenerator.processBlockChunk(
                      state,
                      NextChunk(
                        1L,
                        0,
                        NonEmpty(
                          Seq,
                          TracedPossiblyPrevalidated.notValidated[LedgerBlockEvent](
                            Send(sequencingTs, request, sequencerId)
                          )(TraceContext.empty),
                        ),
                      ),
                    ),
                    log =>
                      log.message should include(
                        if (legacy) "is not registered so cannot send or receive events"
                        else s"not authorized by the topology snapshot at $sequencingTs to send"
                      ),
                  )
                  .failOnShutdown
              } yield {
                if (legacy) {
                  val _ = verify(memberValidator).isMemberRegisteredAt(requestSender, sequencingTs)
                }
                result._1.lastChunkTs shouldBe state.lastChunkTs
                result._2 match {
                  case update: ChunkUpdate => update.submissionsOutcomes shouldBe empty
                  case other => fail(s"Expected a chunk update, got $other")
                }
              }
            }
        }

      Seq[(String, Member, Option[ParticipantPermission])](
        (
          "deliver a submission from an active participant",
          participantSenderId,
          Some(ParticipantPermission.Submission),
        ),
        (
          "deliver a submission from a confirmation-only participant",
          participantSenderId,
          Some(ParticipantPermission.Confirmation),
        ),
        (
          "deliver a submission from an observation-only participant",
          participantSenderId,
          Some(ParticipantPermission.Observation),
        ),
        (
          "deliver a submission from an active mediator",
          mediatorId,
          None,
        ),
        (
          "deliver a submission from an active sequencer",
          sequencerId,
          None,
        ),
        (
          "deliver a submission from an observer mediator in an active mediator group",
          observerMediator,
          None,
        ),
      ).foreach { case (testName, requestSender, participantSenderPermissionO) =>
        testName in {
          checkDelivery(
            authorizationTopology.withParticipants(
              (participantSenderPermissionO.toList.map(permission =>
                participantSenderId -> ParticipantAttributes(permission, None)
              ) :+ (participantRecipientId -> ParticipantAttributes(
                ParticipantPermission.Submission,
                None,
              )))*
            ),
            requestSender,
            participantRecipientId,
            authorizedRecipient = true,
          )
        }
      }
    }

    "authorize recipients (to)" should {

      "reject a mediator group containing unregistered members with SubmissionRequestRefused " +
        "through PV 37 and UnknownRecipients thereafter" in {
          checkDelivery(
            authorizationTopology.withSimpleParticipants(participantSenderId),
            participantSenderId,
            mediatorId,
            authorizedRecipient = true,
            mediatorGroup = authorizationTopology.mediatorGroups.headOption,
            registeredRecipients = false,
          )
        }

      "deliver to SequencersOfSynchronizer containing unregistered members through PV 37 " +
        "and reject with UnknownRecipients thereafter" in {
          checkDelivery(
            authorizationTopology.withSimpleParticipants(participantSenderId),
            participantSenderId,
            observerSequencer,
            authorizedRecipient = true,
            useSequencerGroup = true,
            registeredRecipients = false,
          )
        }

      "preserve registration-only sender validation through PV 37" onlyRunWithOrLessThan
        ProtocolVersion.v37 in {
          checkDelivery(
            authorizationTopology.withParticipants(
              participantSenderId -> ParticipantAttributes(
                ParticipantPermission.Submission,
                Some(sequencingTs.immediateSuccessor),
              ),
              participantRecipientId -> ParticipantAttributes(
                ParticipantPermission.Submission,
                None,
              ),
            ),
            participantSenderId,
            participantRecipientId,
            authorizedRecipient = true,
            authorizedSender = false,
          )
        }

      "preserve the unknown-recipient error for an unregistered member" in {
        checkDelivery(
          authorizationTopology.withSimpleParticipants(participantSenderId, participantRecipientId),
          participantSenderId,
          participantRecipientId,
          authorizedRecipient = true,
          registeredRecipients = false,
        )
      }

      Seq(true, false).foreach { active =>
        val group = authorizationTopology.mediatorGroups.head
          .copy(threshold = if (active) PositiveInt.one else PositiveInt.two)
        val testName =
          if (active) "deliver a mediator group request to its active and observer members"
          else if (testedProtocolVersion > ProtocolVersion.v37)
            "reject a mediator group request with registered members in an inactive group"
          else "deliver to registered members of an inactive mediator group through PV 37"
        testName in {
          checkDelivery(
            authorizationTopology.withSimpleParticipants(participantSenderId),
            participantSenderId,
            mediatorId,
            authorizedRecipient = active,
            mediatorGroup = Some(group),
          )
        }
      }

      Seq(true, false).foreach { includeObserver =>
        val testName =
          if (includeObserver) "deliver to SequencersOfSynchronizer including observer members"
          else "deliver to SequencersOfSynchronizer when all group members are active"
        testName in {
          val topology = authorizationTopology
            .copy(sequencerGroup =
              authorizationTopology.sequencerGroup.copy(
                passive = if (includeObserver) Seq(observerSequencer) else Seq.empty
              )
            )
            .withSimpleParticipants(participantSenderId)
          checkDelivery(
            topology,
            participantSenderId,
            if (includeObserver) observerSequencer else sequencerId,
            authorizedRecipient = true,
            useSequencerGroup = true,
          )
        }
      }

      Seq[(String, Member, Option[ParticipantAttributes], Boolean)](
        (
          "deliver to an active participant",
          participantRecipientId,
          Some(ParticipantAttributes(ParticipantPermission.Submission, None)),
          true,
        ),
        (
          "deliver to an active participant unable to log in",
          participantRecipientId,
          Some(
            ParticipantAttributes(
              ParticipantPermission.Submission,
              Some(sequencingTs.immediateSuccessor),
            )
          ),
          true,
        ),
        (
          "reject a registered participant absent from topology",
          participantRecipientId,
          None,
          false,
        ),
        //
        ("deliver to an active mediator", mediatorId, None, true),
        ("deliver to an observer mediator", observerMediator, None, true),
        ("reject a registered mediator absent from topology", absentMediator, None, false),
        //
        ("deliver to an active sequencer", sequencerId, None, true),
        ("deliver to a registered observer sequencer", observerSequencer, None, true),
        ("reject a registered sequencer absent from topology", absentSequencer, None, false),
      ).foreach { case (testName, recipient, attributes, authorized) =>
        testName onlyRunWhen (authorized || testedProtocolVersion > ProtocolVersion.v37) in {
          val topology = authorizationTopology.withParticipants(
            (Seq(
              participantSenderId -> ParticipantAttributes(ParticipantPermission.Submission, None)
            ) ++
              attributes.toList.map(participantRecipientId -> _))*
          )
          checkDelivery(topology, participantSenderId, recipient, authorized)
        }
      }

      "deliver to a registered recipient absent from topology through PV 37" onlyRunWithOrLessThan ProtocolVersion.v37 in {
        checkDelivery(
          authorizationTopology.withSimpleParticipants(participantSenderId),
          participantSenderId,
          absentMediator,
          authorizedRecipient = false,
        )
      }
    }

    def withMediatorGroup(
        originalCryptoApi: SynchronizerCryptoClient,
        group: MediatorGroup,
    ): SynchronizerCryptoClient = {
      val originalSnapshot = originalCryptoApi.snapshot(sequencingTs).futureValueUS
      // Valid topology transactions cannot encode a group below threshold.
      val snapshot = spy(originalSnapshot.ipsSnapshot)
      doReturn(sequencingTs).when(snapshot).timestamp
      val _ = doReturn(FutureUnlessShutdown.pure(Seq(group)))
        .when(snapshot)
        .mediatorGroups()(anyTraceContext)
      val cryptoSnapshot = new SynchronizerSnapshotSyncCryptoApi(
        originalSnapshot.psid,
        snapshot,
        originalSnapshot.crypto,
        originalSnapshot.syncCryptoSigner,
        originalSnapshot.syncCryptoVerifier,
        loggerFactory,
      )
      val client = spy(originalCryptoApi)
      val _ = doReturn(FutureUnlessShutdown.pure(cryptoSnapshot))
        .when(client)
        .snapshot(eqTo(sequencingTs))(anyTraceContext)
      val _ = doReturn(FutureUnlessShutdown.pure(cryptoSnapshot))
        .when(client)
        .hypotheticalSnapshot(any[CantonTimestamp], eqTo(sequencingTs))(anyTraceContext)
      client
    }

    def checkDelivery(
        testingTopology: TestingTopology,
        requestSender: Member,
        recipient: Member,
        authorizedRecipient: Boolean,
        useSequencerGroup: Boolean = false,
        mediatorGroup: Option[MediatorGroup] = None,
        registeredRecipients: Boolean = true,
        authorizedSender: Boolean = true,
    ): Future[Assertion] = {
      val topology = testingTopology.build(loggerFactory)
      val memberValidator = mock[SequencerMemberValidator]
      when(memberValidator.isMemberRegisteredAt(requestSender, sequencingTs))
        .thenReturn(FutureUnlessShutdown.pure(true))
      val expandedRecipients: Set[Member] =
        if (useSequencerGroup)
          (testingTopology.sequencerGroup.active.forgetNE ++
            testingTopology.sequencerGroup.passive).toSet[Member]
        else mediatorGroup.fold(Set(recipient))(_.all.toSet[Member])
      // Ensure that the recipient is registered, so that the test focuses on the authorization
      //  check.
      when(
        memberValidator.areMembersRegisteredAt(
          argThat[Seq[Member]](_.toSet == expandedRecipients),
          eqTo(sequencingTs),
        )(anyTraceContext)
      ).thenReturn(
        FutureUnlessShutdown.pure(expandedRecipients.map(_ -> registeredRecipients).toMap)
      )
      if (useSequencerGroup || mediatorGroup.isDefined)
        // Group-only requests have no direct members before expansion.
        when(memberValidator.areMembersRegisteredAt(Seq.empty, sequencingTs))
          .thenReturn(FutureUnlessShutdown.pure(Map.empty[Member, Boolean]))
      val rateLimitManager = mock[SequencerRateLimitManager]
      when(
        rateLimitManager.validateRequestAndConsumeTraffic(
          any[SubmissionRequest],
          any[CantonTimestamp],
          any[com.digitalasset.canton.topology.client.TopologySnapshot],
          any[Option[CantonTimestamp]],
          any[Option[CantonTimestamp]],
          anyBoolean,
          any[com.digitalasset.canton.topology.SequencerId],
        )(anyTraceContext, any[CloseContext])
      ).thenReturn(
        EitherT.rightT[FutureUnlessShutdown, SequencerRateLimitError](None)
      )
      val originalCryptoApi =
        topology.forOwnerAndSynchronizer(sequencerId, physicalSynchronizerId, sequencingTs)
      val syncCryptoApi = mediatorGroup.fold(originalCryptoApi) { group =>
        withMediatorGroup(originalCryptoApi, group)
      }
      syncCryptoApi
        .ipsSnapshot(sequencingTs)(TraceContext.empty)
        .flatMap { snapshot =>
          requestSender match {
            case participant: ParticipantId =>
              snapshot.isParticipantActiveAndCanLoginAt(participant, sequencingTs)
            case mediator: MediatorId => snapshot.isMediatorActive(mediator)
            case sequencer: SequencerId => snapshot.isSequencerActive(sequencer)
          }
        }
        .futureValueUS shouldBe authorizedSender
      syncCryptoApi
        .snapshot(sequencingTs)(TraceContext.empty)
        .flatMap { snapshot =>
          recipient match {
            case participant: ParticipantId => snapshot.ipsSnapshot.isParticipantActive(participant)
            case mediator: MediatorId => snapshot.ipsSnapshot.isMediatorActive(mediator)
            case sequencer: SequencerId =>
              snapshot.ipsSnapshot
                .sequencerGroup()
                .map(
                  _.exists(group =>
                    group.active.contains(sequencer) || group.passive.contains(sequencer)
                  )
                )
          }
        }
        .futureValueUS shouldBe authorizedRecipient
      val senderSigningKeyName =
        KeyName.tryCreate(s"${requestSender.toProtoPrimitive}-${SigningKeyUsage.Protocol}")
      val senderSigningKey = topology.crypto.cryptoPublicStore
        .findSigningKeyIdByName(senderSigningKeyName)
        .value
        .futureValueUS
        .getOrElse(fail(s"No protocol signing key found for $requestSender"))
      val blockUpdateGenerator =
        new BlockUpdateGeneratorImpl(
          syncCryptoApi,
          sequencerId,
          rateLimitManager,
          drSequencingTimeUpperBound = None,
          getAnnouncedLsu = None,
          consistencyChecks = true, // Not relevant for this test
          parameters = parameters,
          metrics,
          memberValidator,
          loggerFactory,
        )
      val state = BlockUpdateGenerator.AccumulatedStateProcessingBlocks(
        lastBlockTs = lastBUGStateTs,
        lastChunkTs = lastBUGStateTs,
        latestSequencerEventTimestamp = None,
        latestPendingTopologyTransactionTimestamp = None,
        inFlightAggregations = InFlightAggregations.empty,
      )
      val envelope =
        ClosedUncompressedEnvelope.create(
          ByteString.EMPTY,
          if (useSequencerGroup) Recipients.cc(SequencersOfSynchronizer)
          else
            mediatorGroup.fold(Recipients.cc(recipient))(group =>
              Recipients.cc(MediatorGroupRecipient(group.index))
            ),
          Seq.empty,
          testedProtocolVersion,
        )
      for {
        envelopeSignature <- topology.crypto.privateCrypto
          .sign(
            topology.crypto.pureCrypto.digest(
              HashPurpose.SignedProtocolMessageSignature,
              envelope.bytes,
            ),
            senderSigningKey.fingerprint,
            SigningKeyUsage.ProtocolOnly,
          )
          .valueOrFailShutdown("Failed to sign test envelope")
        request = SubmissionRequest.tryCreate(
          requestSender,
          MessageId.randomMessageId(),
          Batch[ClosedEnvelope](
            List(envelope.copy(signatures = Seq(envelopeSignature))),
            testedProtocolVersion,
          ),
          CantonTimestamp.MaxValue,
          None,
          None,
          testedProtocolVersion,
        )
        requestSignature <- topology.crypto.privateCrypto
          .sign(
            topology.crypto.pureCrypto.digest(
              HashPurpose.SubmissionRequestSignature,
              request.getCryptographicEvidence,
            ),
            senderSigningKey.fingerprint,
            SigningKeyUsage.ProtocolOnly,
          )
          .valueOrFailShutdown("Failed to sign test submission request")
        signedRequest = SignedContent(
          request,
          requestSignature,
          Some(ts1),
          testedProtocolVersion,
        )
        result <- loggerFactory
          .assertLogsSeqString(
            SuppressionRule.LevelAndAbove(Level.WARN),
            requestSender match {
              case _: SequencerId =>
                Seq(s"Unexpected message from $requestSender to participants")
              case _: ParticipantId if useSequencerGroup || recipient.isInstanceOf[SequencerId] =>
                Seq(s"Unexpected message from $requestSender to sequencers")
              case _ => Seq.empty
            },
          )(
            blockUpdateGenerator.processBlockChunk(
              state,
              NextChunk(
                1L,
                0,
                NonEmpty(
                  Seq,
                  TracedPossiblyPrevalidated.notValidated[LedgerBlockEvent](
                    Send(sequencingTs, signedRequest, sequencerId)
                  )(TraceContext.empty),
                ),
              ),
            )
          )
          .failOnShutdown
      } yield {
        if (useSequencerGroup || mediatorGroup.isDefined) request.batch.allMembers shouldBe empty
        val registrationRequired = !useSequencerGroup || testedProtocolVersion > ProtocolVersion.v37
        result._1.lastChunkTs shouldBe sequencingTs
        result._2 match {
          case update: ChunkUpdate =>
            update.submissionsOutcomes should have size 1
            update.submissionsOutcomes.head match {
              case deliver: SubmissionOutcome.Deliver
                  if (authorizedRecipient || testedProtocolVersion <= ProtocolVersion.v37) &&
                    (registeredRecipients || !registrationRequired) =>
                deliver.submission shouldBe request
                deliver.recipients shouldBe (expandedRecipients + requestSender).map(
                  com.digitalasset.canton.sequencing.protocol.MemberRecipient(_)
                )
              case reject: SubmissionOutcome.Reject if !registeredRecipients =>
                if (mediatorGroup.isDefined && testedProtocolVersion <= ProtocolVersion.v37)
                  SequencerErrors.SubmissionRequestRefused.unapply(reject.error) shouldBe defined
                else SequencerErrors.UnknownRecipients.unapply(reject.error) shouldBe defined
              case reject: SubmissionOutcome.Reject if !authorizedRecipient =>
                SequencerErrors.UnauthorizedRecipients.unapply(reject.error) shouldBe defined
              case other =>
                fail(s"Unexpected submission outcome for $requestSender to $recipient: $other")
            }
          case other => fail(s"Expected a chunk update, got $other")
        }
        val _ = verify(memberValidator, if (registrationRequired) times(1) else never)
          .areMembersRegisteredAt(
            argThat[Seq[Member]](_.toSet == expandedRecipients),
            eqTo(sequencingTs),
          )(anyTraceContext)
        succeed
      }
    }
  }

  "BlockUpdateGeneratorImpl.extractBlockEvents" should {
    "filter out events" when {
      "the sequencing time is before or at the minimum sequencing time" in {
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            physicalSynchronizerId,
            aTimestamp,
          )
        val sequencingTimeLowerBoundExclusive = CantonTimestamp.Epoch.plusSeconds(10)

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            drSequencingTimeUpperBound = None,
            getAnnouncedLsu = None,
            consistencyChecks = true,
            parameters = BlockProcessingParameters(
              OrderingTimeFixMode.ValidateOnly,
              lsuSequencingBounds = Some(
                LsuSequencingBounds
                  .unsafeCreate(
                    sequencingTimeLowerBoundExclusive,
                    sequencingTimeLowerBoundExclusive,
                  )
              ),
              parallelism = PositiveInt.two,
              enablePrevalidation = true,
              enableAsyncLoggingWithOutcome = false,
            ),
            metrics,
            memberValidatorMock,
            loggerFactory,
          )

        val signedSubmissionRequest = senderSignedSubmissionRequest(
          topologyTransactionFactory.participant1,
          Recipients.cc(AllMembersOfSynchronizer),
        ).futureValue
        val acknowledgeRequest =
          senderSignedAcknowledgeRequest(topologyTransactionFactory.participant1).futureValue

        val blockBaseSequencingTime1 = sequencingTimeLowerBoundExclusive.minusSeconds(5)
        val blockBaseSequencingTime1MicrosFromEpoch = blockBaseSequencingTime1.toMicros
        blockUpdateGenerator
          .extractBlockEvents(
            Traced(
              RawLedgerBlock(
                1L,
                blockBaseSequencingTime1MicrosFromEpoch,
                Seq(
                  RawBlockEvent.Send(
                    signedSubmissionRequest.toByteString,
                    blockBaseSequencingTime1MicrosFromEpoch,
                    sequencerId.toProtoPrimitive,
                  ),
                  RawBlockEvent
                    .Acknowledgment(
                      acknowledgeRequest.toByteString,
                      sequencingTimeLowerBoundExclusive.minusSeconds(4).toMicros,
                    ),
                ).map(Traced(_)(TraceContext.empty)),
                tickTopologyAtMicrosFromEpoch = None,
              )
            )(TraceContext.empty)
          )
          .value shouldBe BlockEvents(1L, blockBaseSequencingTime1, Seq.empty, None)

        val blockBaseSequencingTime2 = sequencingTimeLowerBoundExclusive.immediatePredecessor
        val blockBaseSequencingTime2MicrosFromEpoch = blockBaseSequencingTime2.toMicros
        assertBlockEventsEqual(
          blockUpdateGenerator
            .extractBlockEvents(
              Traced(
                RawLedgerBlock(
                  1L,
                  blockBaseSequencingTime2MicrosFromEpoch,
                  Seq(
                    RawBlockEvent
                      .Acknowledgment(
                        acknowledgeRequest.toByteString,
                        blockBaseSequencingTime2MicrosFromEpoch,
                      ),
                    RawBlockEvent
                      .Send(
                        signedSubmissionRequest.toByteString,
                        sequencingTimeLowerBoundExclusive.immediateSuccessor.toMicros,
                        sequencerId.toProtoPrimitive,
                      ),
                    RawBlockEvent
                      .Acknowledgment(
                        acknowledgeRequest.toByteString,
                        sequencingTimeLowerBoundExclusive.immediateSuccessor.immediateSuccessor.toMicros,
                      ),
                  ).map(Traced(_)(TraceContext.empty)),
                  None,
                )
              )
            )
            .value,
          BlockEvents(
            1L,
            blockBaseSequencingTime2,
            Seq(
              Send(
                sequencingTimeLowerBoundExclusive.immediateSuccessor,
                signedSubmissionRequest,
                sequencerId,
                signedSubmissionRequest.toByteString.size(),
              ),
              Acknowledgment(
                sequencingTimeLowerBoundExclusive.immediateSuccessor.immediateSuccessor,
                acknowledgeRequest,
              ),
            ).map(
              Traced(_)(TraceContext.empty)
            ),
            None,
          ),
        )

      }
    }

    "append a topology tick event only" when {
      "the block requires one" in {
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            physicalSynchronizerId,
            aTimestamp,
          )

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            drSequencingTimeUpperBound = None,
            getAnnouncedLsu = None,
            consistencyChecks = true,
            parameters = parameters,
            metrics,
            memberValidatorMock,
            loggerFactory,
          )

        blockUpdateGenerator
          .extractBlockEvents(
            Traced(
              RawLedgerBlock(
                1L,
                aTimestamp.toMicros,
                Seq.empty,
                tickTopologyAtMicrosFromEpoch = Some(aTimestamp.toMicros),
              )
            )
          )
          .value shouldBe BlockEvents(
          1L,
          aTimestamp,
          Seq.empty,
          Some(aTimestamp),
        )

        blockUpdateGenerator
          .extractBlockEvents(
            Traced(RawLedgerBlock(1L, aTimestamp.toMicros, Seq.empty, None))
          )
          .value shouldBe BlockEvents(1L, aTimestamp, Seq.empty, None)
      }
    }
  }

  "BlockUpdateGeneratorImpl.chunkBlock" should {
    "append a tick chunk" when {
      "the block requires one" in {
        val sequencerAddressedEventTimestamp = aTimestamp.immediateSuccessor
        val topologyTickEventTimestamp = sequencerAddressedEventTimestamp.immediateSuccessor

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            synchronizerSyncCryptoApi =
              TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
                sequencerId,
                physicalSynchronizerId,
                topologyTickEventTimestamp,
              ),
            sequencerId,
            mock[SequencerRateLimitManager],
            drSequencingTimeUpperBound = None,
            getAnnouncedLsu = None,
            consistencyChecks = true,
            parameters = parameters,
            metrics,
            mock[SequencerMemberValidator],
            loggerFactory,
          )

        for {
          signedSubmissionRequest <- FutureUnlessShutdown.outcomeF(
            senderSignedSubmissionRequest(
              topologyTransactionFactory.participant1,
              Recipients.cc(AllMembersOfSynchronizer),
            )
          )
          events = Seq(
            Traced(
              LedgerBlockEvent.Send(
                sequencerAddressedEventTimestamp,
                signedSubmissionRequest,
                sequencerId,
              )
            )(TraceContext.empty)
          )

          chunkBlock = () =>
            blockUpdateGenerator.chunkBlock(
              BlockEvents(
                height = 1L,
                aTimestamp,
                events,
                tickTopologyAtLeastAt = Some(topologyTickEventTimestamp),
              )
            )

          chunks = chunkBlock()
        } yield {
          chunks should matchPattern {
            case Seq(
                  NextChunk(
                    1L,
                    0,
                    Seq(
                      TracedPossiblyPrevalidated(
                        LedgerBlockEvent.Send(`sequencerAddressedEventTimestamp`, _, _, _),
                        _,
                      )
                    ),
                  ),
                  MaybeTopologyTickChunk(
                    1L,
                    `aTimestamp`,
                    Some(`topologyTickEventTimestamp`),
                  ),
                  EndOfBlock(1L),
                ) =>
          }
        }
      }.failOnShutdown
    }

    "append a maybe tick chunk" when {
      "getting to the end of the block and producePostOrderingTopologyTicks is true" in {
        val sequencerAddressedEventTimestamp = aTimestamp.immediateSuccessor
        val topologyTickEventTimestamp = sequencerAddressedEventTimestamp.immediateSuccessor

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            synchronizerSyncCryptoApi =
              TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
                sequencerId,
                physicalSynchronizerId,
                topologyTickEventTimestamp,
              ),
            sequencerId,
            mock[SequencerRateLimitManager],
            drSequencingTimeUpperBound = None,
            getAnnouncedLsu = None,
            consistencyChecks = true,
            parameters = parameters,
            metrics,
            mock[SequencerMemberValidator],
            loggerFactory,
          )

        for {
          signedSubmissionRequest <- FutureUnlessShutdown.outcomeF(
            senderSignedSubmissionRequest(
              topologyTransactionFactory.participant1,
              Recipients.cc(AllMembersOfSynchronizer),
            )
          )
          events = Seq(
            Traced(
              LedgerBlockEvent.Send(
                sequencerAddressedEventTimestamp,
                signedSubmissionRequest,
                sequencerId,
              )
            )(TraceContext.empty)
          )

          chunks1 = blockUpdateGenerator.chunkBlock(
            BlockEvents(
              height = 1L,
              aTimestamp,
              events,
            )
          )
        } yield {
          chunks1 should matchPattern {
            case Seq(
                  NextChunk(
                    1L,
                    0,
                    Seq(
                      TracedPossiblyPrevalidated(
                        LedgerBlockEvent.Send(`sequencerAddressedEventTimestamp`, _, _, _),
                        _,
                      )
                    ),
                  ),
                  MaybeTopologyTickChunk(
                    1L,
                    `aTimestamp`,
                    None,
                  ),
                  EndOfBlock(1L),
                ) =>
          }
        }
      }.failOnShutdown
    }

    "process a maybe tick chunk" when {
      "receiving the MaybeTopologyTickChunk at the end of the block" in {
        val epsilon = defaultStaticSynchronizerParameters.topologyChangeDelay.duration
        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            synchronizerSyncCryptoApi =
              TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
                sequencerId,
                physicalSynchronizerId,
                aTimestamp,
              ),
            sequencerId,
            mock[SequencerRateLimitManager],
            drSequencingTimeUpperBound = None,
            getAnnouncedLsu = None,
            consistencyChecks = true,
            parameters = parameters,
            metrics,
            mock[SequencerMemberValidator],
            loggerFactory,
          )

        val state = BlockUpdateGenerator.AccumulatedStateProcessingBlocks(
          lastBlockTs = aTimestamp.immediatePredecessor,
          lastChunkTs = aTimestamp,
          latestSequencerEventTimestamp = None,
          inFlightAggregations = InFlightAggregations.empty,
          latestPendingTopologyTransactionTimestamp = None,
        )

        val t1 = aTimestamp.minus(epsilon)
        val t2 = t1.immediateSuccessor
        val t3 = aTimestamp.immediateSuccessor.immediateSuccessor

        for {
          noOpResult <- blockUpdateGenerator.processBlockChunk(
            state,
            MaybeTopologyTickChunk(1L, aTimestamp, None),
          )

          result2 <- blockUpdateGenerator.processBlockChunk(
            state.copy(latestPendingTopologyTransactionTimestamp = Some(t1)),
            MaybeTopologyTickChunk(1L, aTimestamp, None),
          )

          result3 <- blockUpdateGenerator.processBlockChunk(
            state.copy(latestPendingTopologyTransactionTimestamp = Some(t2)),
            MaybeTopologyTickChunk(1L, aTimestamp, None),
          )

          result4 <- blockUpdateGenerator.processBlockChunk(
            state.copy(latestPendingTopologyTransactionTimestamp = Some(t2)),
            MaybeTopologyTickChunk(1L, t3, None),
          )

          result5 <- blockUpdateGenerator.processBlockChunk(
            state.copy(latestPendingTopologyTransactionTimestamp = Some(t1)),
            MaybeTopologyTickChunk(
              1L,
              aTimestamp,
              Some(aTimestamp.immediateSuccessor.immediateSuccessor),
            ),
          )

          result6 <- blockUpdateGenerator.processBlockChunk(
            state.copy(latestPendingTopologyTransactionTimestamp = Some(t2)),
            MaybeTopologyTickChunk(1L, aTimestamp, Some(aTimestamp.immediateSuccessor)),
          )
        } yield {
          // no pending topology transaction timestamps, so nothing to do
          noOpResult shouldBe (state, ChunkUpdate.noop)

          // in this case t1 is considered the highest newly effective topology timestamp, so a tick is created
          result2._1 shouldBe state.copy(
            lastChunkTs = aTimestamp.immediateSuccessor,
            latestSequencerEventTimestamp = Some(aTimestamp.immediateSuccessor),
            latestPendingTopologyTransactionTimestamp = None,
          )
          result2._2 should matchPattern {
            // the tick is created
            case c: ChunkUpdate if c.submissionsOutcomes.sizeIs == 1 =>
          }

          // in this case t2 is not yet effective (by a microsecond), so no tick is created
          result3 shouldBe (state.copy(
            lastChunkTs = aTimestamp,
            latestSequencerEventTimestamp = None,
            latestPendingTopologyTransactionTimestamp = Some(t2),
          ), ChunkUpdate.noop)

          // in this case, the block is empty and the baseBlockSequencingTime was taken into account
          // to conclude that t2 is effective.
          result4._1 shouldBe state.copy(
            lastChunkTs = t3,
            latestSequencerEventTimestamp = Some(t3),
            latestPendingTopologyTransactionTimestamp = None,
          )
          result4._2 should matchPattern {
            // the tick is created
            case c: ChunkUpdate if c.submissionsOutcomes.sizeIs == 1 =>
          }

          // in this case, DABFT is requesting a later tick and this later timestamp wins over t1
          result5._1 shouldBe state.copy(
            lastChunkTs = aTimestamp.immediateSuccessor.immediateSuccessor,
            latestSequencerEventTimestamp = Some(aTimestamp.immediateSuccessor.immediateSuccessor),
            latestPendingTopologyTransactionTimestamp = None,
          )
          result5._2 should matchPattern {
            // the tick is created
            case c: ChunkUpdate if c.submissionsOutcomes.sizeIs == 1 =>
          }

          // in this case, DABFT requests a tick exactly at t2's effective time, which is also the block end:
          //  the tick is broadcast and t2 is no longer pending, rather than the tick going to the sequencers only
          //  and t2 never getting a broadcast tick
          result6._1 shouldBe state.copy(
            lastChunkTs = aTimestamp.immediateSuccessor,
            latestSequencerEventTimestamp = Some(aTimestamp.immediateSuccessor),
            latestPendingTopologyTransactionTimestamp = None,
          )
          result6._2 should matchPattern {
            // the tick is created
            case c: ChunkUpdate if c.submissionsOutcomes.sizeIs == 1 =>
          }
        }
      }.failOnShutdown
    }

  }
}
