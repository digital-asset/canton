// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.channel
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelMetadata,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationStore,
  StoredAuthenticationToken,
}
import com.digitalasset.canton.synchronizer.sequencing.service.channel.SequencerChannelInternalError.{
  PermissionDenied,
  PoolCapacityExhausted,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config, time}
import io.grpc.StatusRuntimeException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*

final class GrpcSequencerChannelPoolTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private class Env(
      maxPoolSize: PositiveInt = PositiveInt.MaxValue,
      channelTtl: time.PositiveFiniteDuration,
      maxMetadataWaitingTime: time.PositiveFiniteDuration,
  ) extends AutoCloseable {

    val protocolVersion = testedProtocolVersion

    val member1 = ParticipantId("member-1")
    val member2 = ParticipantId("member-2")
    val member3 = ParticipantId("member-3")
    val channelId1 = SequencerChannelId.tryCreate("channel-1")
    val channelId2 = SequencerChannelId.tryCreate("channel-2")
    val channelId3 = SequencerChannelId.tryCreate("channel-3")

    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory = loggerFactory)
    val crypto = new SymbolicPureCrypto
    val authenticationStore =
      spy(new MemberAuthenticationStore(PositiveInt.MaxValue, loggerFactory))
    // Keep the test from hanging for long if pool.close() fails
    val timeouts = ProcessingTimeout().copy(network = config.NonNegativeDuration.ofSeconds(5))

    val channelPool = new GrpcSequencerChannelPool(
      clock,
      authenticationStore,
      protocolVersion,
      timeouts,
      maxPoolSize,
      channelTtl,
      maxMetadataWaitingTime,
      loggerFactory,
    )

    override def close(): Unit = channelPool.close()

    def createUninitializedChannel(
        memberTokenExpiresAt: Option[CantonTimestamp] = None,
        authenticatedMemberO: Option[Member] = None,
    ): Either[
      SequencerChannelInternalError,
      (
          ServerCallStreamObserver[ConnectToSequencerChannelResponse],
          StreamObserver[ConnectToSequencerChannelRequest],
      ),
    ] = {
      val authenticationTokenO = memberTokenExpiresAt.map { expiresAt =>
        val member = authenticatedMemberO.getOrElse(member1)
        val token = StoredAuthenticationToken(
          member,
          expiresAt,
          AuthenticationToken.generate(crypto),
          member.fingerprint,
        )
        authenticationStore.saveToken(token)
        token
      }
      val responseObserver = mock[ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse]]
      channelPool
        .createUninitializedChannel(
          responseObserver,
          authenticatedMemberO.orElse(authenticationTokenO.map(_.member)),
          authenticationTokenO,
          _ => Right(()),
          timeouts,
          loggerFactory,
        )
        .map(responseObserver -> _)
    }

    def sendRequest(
        requestObserver: StreamObserver[v30.ConnectToSequencerChannelRequest]
    )(request: v30.ConnectToSequencerChannelRequest.Request): Unit = {
      val req = v30.ConnectToSequencerChannelRequest(
        request,
        Some(SerializableTraceContext(traceContext).toProtoV30),
      )
      requestObserver.onNext(req)
    }

    def initializeChannel(
        channelId: SequencerChannelId,
        initiatingMember: Member = member1,
        receivingMember: Member = member2,
        expectSuccess: Boolean = true,
    )(
        responseObserverMock: ServerCallStreamObserver[ConnectToSequencerChannelResponse],
        requestObserver: StreamObserver[ConnectToSequencerChannelRequest],
    ): ServerCallStreamObserver[ConnectToSequencerChannelResponse] = {
      sendRequest(requestObserver) {
        channel.ConnectToSequencerChannelRequest
          .Metadata(
            SequencerChannelMetadata(
              channelId,
              initiatingMember,
              receivingMember,
              protocolVersion,
            )
          )
          .toProtoV30
      }
      if (expectSuccess) {
        verify(responseObserverMock, never).onError(any[Throwable])
        verify(responseObserverMock, never).onCompleted()
      }
      responseObserverMock
    }

    def createAndInitializeChannel(
        sequencerChannelId: SequencerChannelId,
        initiatingMember: Member = member1,
        receivingMember: Member = member2,
        memberTokenExpiresAt: Option[CantonTimestamp] = None,
    ): (
        ServerCallStreamObserver[ConnectToSequencerChannelResponse],
        StreamObserver[ConnectToSequencerChannelRequest],
    ) = {
      val res = createUninitializedChannel(memberTokenExpiresAt, Some(initiatingMember)).value
      initializeChannel(sequencerChannelId, initiatingMember, receivingMember).tupled(res)
      res
    }

    def checkMetrics(pending: Int = 0, waiting: Int = 0, active: Int = 0): Unit =
      eventually(2.seconds) {
        channelPool.numOfPendingConnections shouldBe pending
        channelPool.numOfWaitingChannels shouldBe waiting
        channelPool.numOfActiveChannels shouldBe active
        // Sanity check against available slots counter skew
        val expectedNumOfSlots = 2L * maxPoolSize.value.toLong - 2L * active - waiting - pending
        channelPool.numOfAvailableSlots shouldBe expectedNumOfSlots
      }

    def assertNumOfScheduledTasks(expected: Int): Unit =
      eventually(2.seconds) {
        clock.numberOfScheduledTasks shouldBe expected
      }

    def expectMaxPoolSizeReachedError(res: Either[SequencerChannelInternalError, Any]): Unit =
      res.left.value shouldBe PoolCapacityExhausted

    /** Makes `observer.onCompleted()` block (up to 5s) until `release()` is called. Returns a latch
      * that is counted down once the call has started. Used to force interleavings in
      * multi-threaded tests.
      */
    def blockOnCompleted(
        observer: ServerCallStreamObserver[ConnectToSequencerChannelResponse]
    ): (CountDownLatch, () => Unit) = {
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      doAnswer { () =>
        entered.countDown()
        release.await(5, TimeUnit.SECONDS)
      }
        .when(observer)
        .onCompleted()
      (entered, () => release.countDown())
    }

  }

  val ExtremelyLongDuration = config.PositiveFiniteDuration(365.days)

  private def withEnv[A](
      maxPoolSize: PositiveInt = PositiveInt.MaxValue,
      channelTtl: config.PositiveFiniteDuration = ExtremelyLongDuration,
      maxMetadataWaitingTime: config.PositiveFiniteDuration = ExtremelyLongDuration,
  )(test: Env => A): A =
    ResourceUtil
      .withResource(
        new Env(
          maxPoolSize,
          time.PositiveFiniteDuration.fromConfig(channelTtl),
          time.PositiveFiniteDuration.fromConfig(maxMetadataWaitingTime),
        )
      )(test)

  "GrpcSequencerChannelPool size limits" should {

    "report an empty pool with full capacity when created" in withEnv(PositiveInt.four) { env =>
      import env.*

      checkMetrics(pending = 0, waiting = 0, active = 0)
      channelPool.size shouldBe 0

      // 8 members can connect and establish 4 channels
      channelPool.numOfAvailableSlots shouldBe 8L
    }

    "limit the number of pending connections" in withEnv(PositiveInt.two) { env =>
      import env.*

      // An uninitialized channel represents a potential channel member connection (a pending connection).
      // An active channel needs two members, so the number of pending connections is limited to
      // (2 * maximum pool size) - number of already connected members.
      // In this case it's 4.
      (1 to 4).foreach { _ =>
        createUninitializedChannel().value
        // Uninitialized channels don't count towards size.
        channelPool.size shouldBe 0
      }

      expectMaxPoolSizeReachedError(createUninitializedChannel())
      channelPool.size shouldBe 0
    }

    "reject new connections once the size limit is reached" in withEnv(PositiveInt.two) { env =>
      import env.*

      channelPool.numOfAvailableSlots shouldBe 4
      channelPool.size shouldBe 0

      // Create a new channel (channel-1), waiting for the other member to join
      createAndInitializeChannel(channelId1, member1, member2)
      checkMetrics(waiting = 1)
      channelPool.numOfAvailableSlots shouldBe 3
      channelPool.size shouldBe 1

      // Create another new channel (channel-2), waiting for the other member to join
      createAndInitializeChannel(channelId2, member1, member2)
      checkMetrics(waiting = 2)
      channelPool.numOfAvailableSlots shouldBe 2
      channelPool.size shouldBe 2

      // Create pending channel
      val pendingChannel1 = createUninitializedChannel().value
      channelPool.numOfAvailableSlots shouldBe 1
      channelPool.size shouldBe 2
      val pendingChannel2 = createUninitializedChannel().value
      channelPool.numOfAvailableSlots shouldBe 0
      channelPool.size shouldBe 2
      expectMaxPoolSizeReachedError(createUninitializedChannel())
      checkMetrics(
        pending = 2,
        // Pending connections are limited only by free slots (2 * maxPoolSize),
        // so waiting channels can still receive their second member.
        waiting = 2,
      )

      // Connect the pending connection to the waiting channel, making it active.
      initializeChannel(channelId1, member2, member1).tupled(pendingChannel1)
      checkMetrics(pending = 1, waiting = 1, active = 1)
      channelPool.numOfAvailableSlots shouldBe 0
      channelPool.size shouldBe 2

      // Still no slots available: 1 active channel (2 slots), 1 waiting channel (1 slot) and 1 pending connection
      // that may belong to the member the waiting channel expects.
      expectMaxPoolSizeReachedError(createUninitializedChannel())

      channelPool.numOfAvailableSlots shouldBe 0

      // Try to initialize 3rd channel.
      // Request should be rejected because the pool already has maxPoolSize channels, and its slot should be released.
      val respObserver = initializeChannel(channelId3, member2, member1, expectSuccess = false)
        .tupled(pendingChannel2)
      eventually(2.seconds) {
        verify(respObserver).onError(
          argThat[io.grpc.StatusRuntimeException](
            (_.getMessage.contains(PoolCapacityExhausted.message))
          )
        )
      }
      checkMetrics(waiting = 1, active = 1)

      // Initialization failure should complete and remove the uninitialized channel, releasing its slot.
      channelPool.numOfAvailableSlots shouldBe 1
      channelPool.size shouldBe 2

      val pendingChannel3 = createUninitializedChannel().value
      checkMetrics(pending = 1, waiting = 1, active = 1)
      channelPool.numOfAvailableSlots shouldBe 0
      channelPool.size shouldBe 2

      // Finally the last waiting channel gets its second member
      initializeChannel(channelId2, member2, member1).tupled(pendingChannel3)
      checkMetrics(active = 2)
      channelPool.numOfAvailableSlots shouldBe 0
      channelPool.size shouldBe 2

      // Now the pool is at its capacity and will reject new connections until a slot gets released
      expectMaxPoolSizeReachedError(createUninitializedChannel())
    }

    "allow both members to connect before either sends metadata" in withEnv(PositiveInt.one) {
      env =>
        import env.*

        val pending1 = createUninitializedChannel().value
        val pending2 = createUninitializedChannel().value
        checkMetrics(pending = 2)

        initializeChannel(channelId1, member1, member2).tupled(pending1)
        initializeChannel(channelId1, member2, member1).tupled(pending2)
        checkMetrics(active = 1)
        channelPool.size shouldBe 1
    }

    "allow second member to connect after the first member sends the metadata" in withEnv(
      PositiveInt.one
    ) { env =>
      import env.*

      createAndInitializeChannel(channelId1, member1, member2)
      checkMetrics(waiting = 1)
      channelPool.size shouldBe 1

      createAndInitializeChannel(channelId1, member2, member1)
      checkMetrics(active = 1)
      channelPool.size shouldBe 1
    }

    "not create more channels than maxPoolSize" in withEnv(PositiveInt.two) { env =>
      import env.*

      createAndInitializeChannel(channelId1, member1, member2)
      createAndInitializeChannel(channelId2, member1, member3)
      checkMetrics(waiting = 2)

      // a free connection slot is still available, but no channel slot.
      val pending = createUninitializedChannel().value
      checkMetrics(pending = 1, waiting = 2)
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
        initializeChannel(channelId3, member2, member3, expectSuccess = false).tupled(pending),
        forExactly(1, _)(_.infoMessage should include(PoolCapacityExhausted.message)),
      )
      checkMetrics(pending = 0, waiting = 2) // rejected connection's slot released
      channelPool.size shouldBe 2
    }

    "handle size limits close to Int.MaxValue without overflow" in withEnv(
      PositiveInt.tryCreate(Int.MaxValue - 1)
    ) { env =>
      import env.*

      val numOfSlots = (Int.MaxValue.toLong - 1L) * 2L

      channelPool.numOfAvailableSlots shouldBe numOfSlots
      createUninitializedChannel().value
      channelPool.numOfAvailableSlots shouldBe numOfSlots - 1

      channelPool.closeChannels()
      channelPool.numOfAvailableSlots shouldBe numOfSlots
    }

    "release the slot of a channel whose initialization fails" in withEnv() { env =>
      import env.*

      val (responseObserver, requestObserver) = createUninitializedChannel().value

      checkMetrics(pending = 1)

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
        // Uninitialized channel expects channel metadata - empty request would cause a failure
        sendRequest(requestObserver) {
          v30.ConnectToSequencerChannelRequest.Request.Empty
        },
        forExactly(1, _)(
          _.infoMessage should include regex "Sequencer channel initialization error"
        ),
      )

      eventually(2.seconds) {
        verify(responseObserver).onError(any[Throwable])
      }
      checkMetrics()

      requestObserver.onCompleted()
      checkMetrics()
    }

    "release the slot when the token expires (waiting channel)" in withEnv(PositiveInt.one) { env =>
      import env.*

      val (responseObserver, _) =
        createAndInitializeChannel(
          channelId1,
          memberTokenExpiresAt = Some(clock.now.add(10.minutes)),
        )

      checkMetrics(waiting = 1)
      channelPool.size shouldBe 1
      channelPool.numOfAvailableSlots shouldBe 1

      clock.advance(11.minutes.toJava)

      eventually(2.seconds) {
        verify(responseObserver).onCompleted()
      }

      checkMetrics()
      channelPool.size shouldBe 0
      channelPool.numOfAvailableSlots shouldBe 2
    }

    "release both slots when a member's token expires (active channel)" in withEnv(
      PositiveInt.one
    ) { env =>
      import env.*

      val (responseObserver1, _) = createAndInitializeChannel(
        channelId1,
        member1,
        member2,
        memberTokenExpiresAt = Some(clock.now.add(10.minutes)),
      )
      val (responseObserver2, _) = createAndInitializeChannel(
        channelId1,
        member2,
        member1,
        memberTokenExpiresAt = Some(clock.now.add(5.minutes)),
      )

      checkMetrics(active = 1)
      channelPool.size shouldBe 1
      channelPool.numOfAvailableSlots shouldBe 0

      clock.advance(6.minutes.toJava)

      checkMetrics()
      channelPool.size shouldBe 0
      channelPool.numOfAvailableSlots shouldBe 2

      clock.advance(20.minutes.toJava)

      checkMetrics()
      channelPool.size shouldBe 0
      channelPool.numOfAvailableSlots shouldBe 2

      verify(responseObserver1, times(1)).onCompleted()
      verify(responseObserver2, times(1)).onCompleted()
    }

    "reject an already expired token without retaining its slot" in withEnv(
      PositiveInt.one
    ) { env =>
      import env.*

      val result = createUninitializedChannel(
        memberTokenExpiresAt = Some(clock.now.minusSeconds(1))
      )

      result.left.value shouldBe PermissionDenied(
        "Authentication token has expired or been revoked."
      )
      checkMetrics()
      assertNumOfScheduledTasks(0)
    }

    "close a pending connection when its token expires before the metadata timeout" in withEnv(
      maxPoolSize = PositiveInt.one,
      maxMetadataWaitingTime = PositiveFiniteDuration(2.minutes),
    ) { env =>
      import env.*

      val tokenExpiresAt = clock.now.add(10.seconds)
      val (responseObserver, requestObserver) = createUninitializedChannel(
        // Token expires after now before the metadata timeout
        memberTokenExpiresAt = Some(tokenExpiresAt)
      ).value

      checkMetrics(pending = 1)
      assertNumOfScheduledTasks(1)

      clock.advanceTo(tokenExpiresAt.minusSeconds(1))

      checkMetrics(pending = 1)
      verify(responseObserver, never).onCompleted()

      clock.advanceTo(tokenExpiresAt)

      checkMetrics()
      initializeChannel(channelId1, expectSuccess = false)(responseObserver, requestObserver)
      verify(responseObserver, times(1)).onCompleted()
      verify(responseObserver, never).onError(any[Throwable])
      assertNumOfScheduledTasks(0)

      clock.advance(2.minutes.toJava)

      checkMetrics()
      verify(responseObserver, times(1)).onCompleted()
      assertNumOfScheduledTasks(0)
    }

    "release slots of a member's channels on closeChannels(member)" in withEnv() { env =>
      import env.*

      createUninitializedChannel().value
      val (channel1Observer, _) =
        createAndInitializeChannel(channelId1, member1, member2)
      // Make 1st channel active
      val (channel1ObserverForJoiningMember, _) =
        createAndInitializeChannel(channelId1, member2, member1)
      val (channel2Observer, _) =
        createAndInitializeChannel(channelId2, member1, member3)
      val (channel3Observer, _) =
        createAndInitializeChannel(channelId3, member2, member3)

      checkMetrics(pending = 1, waiting = 2, active = 1)

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      eventually(2.seconds) {
        verify(channel1Observer).onCompleted()
        verify(channel1ObserverForJoiningMember).onCompleted()
        verify(channel2Observer).onCompleted()
        verify(channel3Observer, never).onCompleted()
      }
      checkMetrics(pending = 1, waiting = 1)
    }

    "close every pending connection owned by a revoked member and cancel its timer" in withEnv(
      PositiveInt.two
    ) { env =>
      import env.*

      val (revoked1, _) =
        createUninitializedChannel(authenticatedMemberO = Some(member1)).value
      val (revoked2, _) =
        createUninitializedChannel(authenticatedMemberO = Some(member1)).value
      val (otherMember, _) =
        createUninitializedChannel(authenticatedMemberO = Some(member2)).value
      val (unauthenticated, _) = createUninitializedChannel().value
      checkMetrics(pending = 4)
      assertNumOfScheduledTasks(4)

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      verify(revoked1).onCompleted()
      verify(revoked2).onCompleted()
      verifyNoMoreInteractions(otherMember, unauthenticated)
      checkMetrics(pending = 2)
      assertNumOfScheduledTasks(2)

      channelPool.closeChannels(waitForClosed = true)

      verify(otherMember).onCompleted()
      verify(unauthenticated).onCompleted()
      verifyNoMoreInteractions(revoked1, revoked2)
      checkMetrics()
      assertNumOfScheduledTasks(0)
    }

    "release slots when a second member is revoked via closeChannels(member)" in withEnv() { env =>
      import env.*

      createUninitializedChannel().value
      val (channel1Observer, _) =
        createAndInitializeChannel(channelId1, member2, member1)
      // Make 1st channel active, member1 is a secondMemeber
      val (channel1ObserverForJoiningMember, _) =
        createAndInitializeChannel(channelId1, member1, member2)
      val (channel2Observer, _) =
        createAndInitializeChannel(channelId2, member2, member1)
      val (channel3Observer, _) =
        createAndInitializeChannel(channelId3, member2, member3)

      checkMetrics(pending = 1, waiting = 2, active = 1)

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      eventually(2.seconds) {
        verify(channel1Observer).onCompleted()
        verify(channel1ObserverForJoiningMember).onCompleted()
        verify(channel2Observer).onCompleted()
        verify(channel3Observer, never).onCompleted()
      }
      checkMetrics(pending = 1, waiting = 1)
    }

    "handle repeated closeChannels(member) idempotently" in withEnv() { env =>
      import env.*

      val (channel1Observer, _) =
        createAndInitializeChannel(channelId1)
      val (channel2Observer, _) =
        createAndInitializeChannel(channelId2)
      checkMetrics(waiting = 2)

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      eventually(2.seconds) {
        verify(channel1Observer).onCompleted()
        verify(channel2Observer).onCompleted()
      }
      checkMetrics()

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      verifyNoMoreInteractions(channel1Observer)
      verifyNoMoreInteractions(channel2Observer)

      checkMetrics()
    }

    "release slots on closeChannels(None)" in withEnv() { env =>
      import env.*

      createUninitializedChannel().value
      val (channel1Observer, _) =
        createAndInitializeChannel(channelId1, member1, member2)
      // Make 1st channel active
      val (channel1ObserverForJoiningMember, _) =
        createAndInitializeChannel(channelId1, member2, member1)
      val (channel2Observer, _) =
        createAndInitializeChannel(channelId2, member1, member3)
      val (channel3Observer, _) =
        createAndInitializeChannel(channelId3, member2, member3)

      checkMetrics(pending = 1, waiting = 2, active = 1)
      channelPool.size shouldBe 3

      channelPool.closeChannels(None, waitForClosed = true)

      eventually(2.seconds) {
        verify(channel1Observer).onCompleted()
        verify(channel1ObserverForJoiningMember).onCompleted()
        verify(channel2Observer).onCompleted()
        verify(channel3Observer).onCompleted()
      }
      checkMetrics()
      channelPool.size shouldBe 0
    }

    "release the slot when the request stream completes" in withEnv() { env =>
      import env.*

      val (responseObserver, requestObserver) = createAndInitializeChannel(channelId1)

      checkMetrics(waiting = 1)

      requestObserver.onCompleted()

      eventually(2.seconds) {
        verify(responseObserver).onCompleted()
      }
      checkMetrics()
    }
  }

  "GrpcSequencerChannelPool thread safety" should {

    "not break when the pending connection is closed while its metadata is being processed" in withEnv(
      PositiveInt.one
    ) { env =>
      import env.*

      val inAuthCheck = new CountDownLatch(1)
      val resume = new CountDownLatch(1)
      val resObs = mock[ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse]]
      val reqObs = channelPool
        .createUninitializedChannel(
          resObs,
          authenticatedMemberO = None,
          None,
          // Used to park one of the threads by pausing the auth check. Eventually returns Right(()).
          _ => { inAuthCheck.countDown(); resume.await(5, TimeUnit.SECONDS); Right(()) },
          ProcessingTimeout(),
          loggerFactory,
        )
        .value

      val onNextF = Future(initializeChannel(channelId1, expectSuccess = false)(resObs, reqObs))
      // Thread T1 is now just before ensureChannelAndConnect
      inAuthCheck.await(5, TimeUnit.SECONDS) shouldBe true
      // thread T2: close channels and release the slots
      channelPool.closeChannels()
      resume.countDown()

      onNextF.futureValue
      channelPool.size shouldBe 0
      // slots back to 2 * maxPoolSize, nothing pending/waiting
      checkMetrics()
    }

    "not initialize a pending connection removed by member revocation while processing metadata" in
      withEnv(PositiveInt.one) { env =>
        import env.*

        val inAuthCheck = new CountDownLatch(1)
        val resume = new CountDownLatch(1)
        val resObs = mock[ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse]]
        val reqObs = channelPool
          .createUninitializedChannel(
            resObs,
            Some(member1),
            None,
            // Pause metadata processing before it acquires the pool lock.
            _ => { inAuthCheck.countDown(); resume.await(5, TimeUnit.SECONDS); Right(()) },
            ProcessingTimeout(),
            loggerFactory,
          )
          .value

        val onNextF = Future(initializeChannel(channelId1, expectSuccess = false)(resObs, reqObs))
        try {
          inAuthCheck.await(5, TimeUnit.SECONDS) shouldBe true
          channelPool.closeChannels(Some(member1))
        } finally resume.countDown()

        onNextF.futureValue
        verify(resObs).onCompleted()
        verifyNoMoreInteractions(resObs)
        checkMetrics()
        assertNumOfScheduledTasks(0)
      }

    "not miss revocation between token validation and pending connection registration" in
      withEnv(PositiveInt.one) { env =>
        import env.*

        val inTokenCheck = new CountDownLatch(1)
        val resume = new CountDownLatch(1)
        val revoked = new CountDownLatch(1)
        org.mockito.Mockito
          .doAnswer { (invocation: InvocationOnMock) =>
            val validToken = invocation.callRealMethod()
            inTokenCheck.countDown()
            resume.await(5, TimeUnit.SECONDS) shouldBe true
            validToken
          }
          .when(authenticationStore)
          .tokenForMemberAt(any[Member], any[AuthenticationToken], any[CantonTimestamp])

        val connectionF = Future(
          createUninitializedChannel(
            memberTokenExpiresAt = Some(clock.now.plusSeconds(30)),
            authenticatedMemberO = Some(member1),
          ).value
        )

        val disconnectF =
          try {
            inTokenCheck.await(5, TimeUnit.SECONDS) shouldBe true
            val disconnect = Future {
              authenticationStore.invalidateMember(member1)
              revoked.countDown()
              channelPool.closeChannels(Some(member1), waitForClosed = true)
            }
            revoked.await(5, TimeUnit.SECONDS) shouldBe true
            disconnect
          } finally resume.countDown()

        val (response, request) = connectionF.futureValue
        disconnectF.futureValue
        verify(response).onCompleted()
        checkMetrics()
        assertNumOfScheduledTasks(0)

        initializeChannel(channelId1, expectSuccess = false)(response, request)
        verifyNoMoreInteractions(response)
        checkMetrics()
        assertNumOfScheduledTasks(0)
      }

    "not deadlock when a pending connection completes while closeChannels runs" in withEnv() {
      env =>
        import env.*
        val (resObs, reqObs) = createUninitializedChannel().value
        val (closing, finishClosing) = blockOnCompleted(resObs)

        // Thread T1: complete() takes connection lock, parks in onCompleted
        val clientF = Future(reqObs.onCompleted())
        closing.await(5, TimeUnit.SECONDS) shouldBe true
        // Thread T2: takes pool lock, then blocks on connection lock in close()
        val closeF = Future(channelPool.closeChannels())
        eventually(2.seconds) {
          channelPool.numOfPendingConnections shouldBe 0
        }
        // T1 continues to notifyOnComplete → needs pool lock
        finishClosing()

        clientF.futureValue(timeout(5.seconds))
        closeF.futureValue(timeout(5.seconds))
        checkMetrics()
    }
  }

  "GrpcSequencerChannelPool channel removal" should {

    // Channel ids are reused when a member reconnects

    "not drop a reconnected channel when the old channel with the same id finishes closing" in
      withEnv(PositiveInt.two) { env =>
        import env.*

        // aka 'oldChannel'
        val (oldObs, _) = createAndInitializeChannel(channelId1, member1, member2)
        checkMetrics(waiting = 1)

        // The thread will be parked on oldChannel.close()
        val (closing, finishClosing) = blockOnCompleted(oldObs)
        val closeF = Future(channelPool.closeChannels())
        closing.await(5, TimeUnit.SECONDS) shouldBe true
        // the old channel is already detached and its slot released
        checkMetrics()

        // member1 reconnects with the same channel id while the old channel is still closing.
        val (newObs, _) = createAndInitializeChannel(channelId1, member1, member2)
        checkMetrics(waiting = 1)

        finishClosing()
        closeF.futureValue

        // closeChannels() should not remove the new channel.
        always(500.millis)(channelPool.size shouldBe 1)
        checkMetrics(waiting = 1)
        verify(newObs, never).onCompleted()

        // member2 must still be able to join the reconnected channel.
        createAndInitializeChannel(channelId1, member2, member1)
        checkMetrics(active = 1)
      }

    "not let a member join a channel that is being closed by its TTL job" in
      withEnv(PositiveInt.two, channelTtl = PositiveFiniteDuration(10.minutes)) { env =>
        import env.*

        val start = clock.now
        // aka 'firstChannel'
        val (firstObs, _) = createAndInitializeChannel(channelId1, member1, member2)

        // Create the second member's pending connection 1 minute later. Its own pending-connection
        // TTL (start+11m) then doesn't fire together with the channel TTL.
        clock.advance(1.minute.toJava)
        val (secondObs, secondReqObs) = createUninitializedChannel().value
        checkMetrics(pending = 1, waiting = 1)

        // The thread will be parked on firstChannel.close()
        val (closing, finishClosing) = blockOnCompleted(firstObs)
        val ttlF = Future(clock.advanceTo(start.add(10.minutes + 1.second)))
        closing.await(5, TimeUnit.SECONDS) shouldBe true

        // The second member sends its metadata while the channel is closing.
        initializeChannel(channelId1, member2, member1)(secondObs, secondReqObs)

        // It must not be attached to the closing channel.
        verify(secondObs, never).onNext(any[ConnectToSequencerChannelResponse])
        // we should end up with a new channel with member2 as its first member
        checkMetrics(waiting = 1)

        finishClosing()
        ttlF.futureValue
        verify(firstObs).onCompleted()

        always(500.millis)(channelPool.size shouldBe 1)
        checkMetrics(waiting = 1)
        verify(secondObs, never).onCompleted()
      }
  }

  "GrpcSequencerChannelPool scheduled jobs" should {

    val channelTtl = PositiveFiniteDuration(10.minutes)

    "leave no scheduled jobs after a regular pool close" in withEnv(channelTtl = channelTtl) {
      env =>
        import env.*

        createUninitializedChannel().value
        createAndInitializeChannel(
          channelId1,
          member1,
          member2,
          memberTokenExpiresAt = Some(clock.now.add(5.minutes)),
        )
        createAndInitializeChannel(
          channelId1,
          member2,
          member1,
          memberTokenExpiresAt = Some(clock.now.add(5.minutes)),
        )
        checkMetrics(pending = 1, active = 1)
        // pending channel TTL + active channel TTL + 2 x member token expiry
        assertNumOfScheduledTasks(4)

        channelPool.close()

        // All channel jobs should be cancelled
        assertNumOfScheduledTasks(0)
    }

    "cancel the jobs of a channel whose close fails when the pool is closed" in withEnv(
      channelTtl = channelTtl
    ) { env =>
      import env.*

      val (resObs, _) = createAndInitializeChannel(
        channelId1,
        member1,
        member2,
        memberTokenExpiresAt = Some(clock.now.add(5.minutes)),
      )
      checkMetrics(waiting = 1)
      // channel TTL + member token expiry
      assertNumOfScheduledTasks(2)

      // Completing the member's response stream fails, so the channel never reaches notifyClosed()
      // and the onClosed callbacks that cancel its jobs never run.
      doThrow(new IllegalStateException("Test exception")).when(resObs).onCompleted()

      loggerFactory.assertLogs(
        channelPool.close(),
        _.warningMessage should include(s"Failed to close channel $channelId1"),
      )

      // All channel jobs should be cancelled
      assertNumOfScheduledTasks(0)
    }

    "close every channel even if closing a pending connection fails" in withEnv(
      channelTtl = channelTtl
    ) { env =>
      import env.*

      val (waitingObs, _) = createAndInitializeChannel(channelId1, member1, member2)
      val (pendingObs1, _) = createUninitializedChannel().value
      val (pendingObs2, _) = createUninitializedChannel().value
      checkMetrics(pending = 2, waiting = 1)
      // 2x pending channel TTL + waiting channel TTL
      assertNumOfScheduledTasks(3)

      doThrow(new IllegalStateException("Test exception")).when(pendingObs1).onCompleted()

      loggerFactory.assertLogs(
        channelPool.closeChannels(),
        _.warningMessage should include("Failed to close pending connection"),
      )

      // Other channels' streams should get completed
      verify(pendingObs2).onCompleted()
      verify(waitingObs).onCompleted()

      // All the channels were removed
      checkMetrics()

      // Because of the failure, the scheduled job for first pending channel was not cancelled
      assertNumOfScheduledTasks(1)

      // Closing the pool should remove the remaining orphaned job
      channelPool.close()
      assertNumOfScheduledTasks(0)
    }
  }

  "GrpcSequencerChannelPool TTL limits" should {

    val channelTtl = PositiveFiniteDuration(10.minutes)
    val pendingConnectionTtl = PositiveFiniteDuration(5.minutes)

    "close a pending connection once the pending connection TTL expires" in withEnv(
      maxMetadataWaitingTime = pendingConnectionTtl,
      channelTtl = channelTtl,
    ) { env =>
      import env.*

      val (resObs, _) = createUninitializedChannel().value
      checkMetrics(pending = 1)
      // job should be scheduled
      assertNumOfScheduledTasks(1)

      clock.advance(pendingConnectionTtl.underlying.plus(10.seconds).toJava)

      eventually(2.seconds) {
        // the member stream should be completed exactly once
        verify(resObs, times(1)).onCompleted()
      }
      // job should be removed from the queue
      assertNumOfScheduledTasks(0)
      // slot should be released
      checkMetrics(pending = 0)
    }

    "not close channel initialized before pending connection TTL timeout" in withEnv(
      maxMetadataWaitingTime = pendingConnectionTtl,
      channelTtl = channelTtl,
    ) { env =>
      import env.*

      val (resObs, reqObs) =
        createUninitializedChannel().value
      checkMetrics(pending = 1)
      assertNumOfScheduledTasks(1)

      val cantonNow = clock.now

      // 20s before pending connection expiration time
      clock.advanceTo(cantonNow.add(pendingConnectionTtl.underlying.minus(20.second)))
      checkMetrics(pending = 1)
      // pending connection TTL job should be added
      assertNumOfScheduledTasks(1)

      initializeChannel(channelId1)(resObs, reqObs)
      checkMetrics(waiting = 1)
      // Pending connection TTL job should be replaced with channel TTL job
      assertNumOfScheduledTasks(1)

      // 10s past pending connection expiration time while the channel is no longer in the pending state
      clock.advanceTo(cantonNow.add(pendingConnectionTtl.underlying.plus(10.second)))

      checkMetrics(waiting = 1)
      assertNumOfScheduledTasks(1)
      // member stream should not be completed
      verify(resObs, never).onCompleted()
    }

    "not attempt to close already completed pending channel if pending connection TTL is exceeded" in withEnv(
      maxMetadataWaitingTime = pendingConnectionTtl,
      channelTtl = channelTtl,
    ) { env =>
      import env.*

      val (resObs, reqObs) = createUninitializedChannel().value
      checkMetrics(pending = 1)
      assertNumOfScheduledTasks(1)

      reqObs.onCompleted()

      eventually(2.seconds) {
        verify(resObs, times(1)).onCompleted()
      }
      checkMetrics(pending = 0)
      // channel completion should cancel owned jobs
      assertNumOfScheduledTasks(0)
    }

    "not attempt to close already completed waiting channel if channel TTL is exceeded" in withEnv(
      channelTtl = channelTtl
    ) { env =>
      import env.*

      val (resObs, reqObs) =
        createAndInitializeChannel(channelId1)
      checkMetrics(waiting = 1)
      // channel TTL
      assertNumOfScheduledTasks(1)

      reqObs.onCompleted()

      eventually(2.seconds) {
        verify(resObs, times(1)).onCompleted()
      }
      checkMetrics(waiting = 0)
      // channel completion should cancel owned jobs
      assertNumOfScheduledTasks(0)
    }

    "not attempt to close already completed active channel if channel TTL is exceeded" in withEnv(
      channelTtl = channelTtl
    ) { env =>
      import env.*

      val (resObs1, reqObs1) = createAndInitializeChannel(
        channelId1,
        member1,
        member2,
      )
      val (resObs2, _) = createAndInitializeChannel(
        channelId1,
        member2,
        member1,
      )
      eventually(2.seconds) {
        channelPool.size shouldBe 1
      }
      checkMetrics(active = 1)
      // channel TTL
      assertNumOfScheduledTasks(1)

      reqObs1.onCompleted()

      eventually(2.seconds) {
        verify(resObs1, times(1)).onCompleted()
        verify(resObs2, times(1)).onCompleted()
      }
      checkMetrics(active = 0)
      channelPool.size shouldBe 0

      // channel completion should cancel owned jobs
      assertNumOfScheduledTasks(0)
    }

    "not attempt to close channel whose initialization fails if pending connection TTL is exceeded" in withEnv(
      maxPoolSize = PositiveInt.one,
      maxMetadataWaitingTime = pendingConnectionTtl,
      channelTtl = channelTtl,
    ) { env =>
      import env.*

      val (_, initializedReqObs) = createAndInitializeChannel(channelId1, member1, member2)
      checkMetrics(waiting = 1)
      val (responseObserver, requestObserver) = createUninitializedChannel().value
      checkMetrics(pending = 1, waiting = 1)

      initializeChannel(channelId2, member1, member2, expectSuccess = false)(
        responseObserver,
        requestObserver,
      )
      eventually(2.seconds) {
        verify(responseObserver).onError(any[StatusRuntimeException])
      }
      checkMetrics(pending = 0, waiting = 1)

      initializedReqObs.onCompleted()
      checkMetrics(pending = 0, waiting = 0)

      assertNumOfScheduledTasks(0)
      clock.advance(pendingConnectionTtl.underlying.plus(1.second).toJava)

      verify(responseObserver, never).onCompleted()
    }

    "not attempt to close already closed pending channel if pending connection TTL is exceeded" in withEnv(
      maxMetadataWaitingTime = pendingConnectionTtl,
      channelTtl = channelTtl,
    ) { env =>
      import env.*

      val (resObs, _) = createUninitializedChannel().value
      checkMetrics(pending = 1)

      channelPool.closeChannels(waitForClosed = true)

      eventually(2.seconds) {
        verify(resObs, times(1)).onCompleted()
      }
      checkMetrics(pending = 0)

      reset(resObs)
      assertNumOfScheduledTasks(0)
      clock.advance(pendingConnectionTtl.underlying.plus(1.second).toJava)

      assertNumOfScheduledTasks(0)
      // member stream should not be completed for the second time
      verify(resObs, never).onCompleted()
    }

    "not attempt to close already closed waiting channel if channel TTL is exceeded" in withEnv(
      channelTtl = channelTtl
    ) { env =>
      import env.*

      val (resObs, _) = createAndInitializeChannel(channelId1, member1, member2)
      checkMetrics(waiting = 1)

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      eventually(2.seconds) {
        verify(resObs, times(1)).onCompleted()
      }
      checkMetrics(waiting = 0)

      reset(resObs)
      assertNumOfScheduledTasks(0)
      clock.advance(channelTtl.underlying.plus(1.second).toJava)

      assertNumOfScheduledTasks(0)
      // member stream should not be completed for the second time
      verify(resObs, never).onCompleted()
    }

    "not attempt to close already closed active channel if channel TTL is exceeded" in withEnv(
      channelTtl = channelTtl
    ) { env =>
      import env.*

      val (resObs1, _) = createAndInitializeChannel(channelId1, member1, member2)
      val (resObs2, _) = createAndInitializeChannel(channelId1, member2, member1)
      checkMetrics(active = 1)
      channelPool.size shouldBe 1

      channelPool.closeChannels(Some(member1), waitForClosed = true)

      eventually(2.seconds) {
        verify(resObs1, times(1)).onCompleted()
        verify(resObs2, times(1)).onCompleted()
        channelPool.size shouldBe 0
      }
      checkMetrics(active = 0)

      reset(resObs1)
      reset(resObs2)
      assertNumOfScheduledTasks(0)

      clock.advance(channelTtl.underlying.plus(1.second).toJava)

      assertNumOfScheduledTasks(0)
      // member streams should not be completed for the second time
      verify(resObs1, never).onCompleted()
      verify(resObs2, never).onCompleted()
    }

    "close waiting channel once channel TTL is exceeded" in withEnv(channelTtl = channelTtl) {
      env =>
        import env.*

        val (resObs, _) = createAndInitializeChannel(channelId1, member1, member2)
        channelPool.size shouldBe 1

        clock.advance(channelTtl.underlying.plus(1.second).toJava)

        // The slot should be released
        checkMetrics()
        eventually(2.seconds) {
          // the member stream should be completed exactly once
          verify(resObs, times(1)).onCompleted()
          channelPool.size shouldBe 0
        }
    }

    "close active channel once channel TTL is exceeded" in withEnv(channelTtl = channelTtl) { env =>
      import env.*

      val (resObs1, _) = createAndInitializeChannel(channelId1, member1, member2)
      val (resObs2, _) = createAndInitializeChannel(channelId1, member2, member1)
      channelPool.size shouldBe 1

      clock.advance(channelTtl.underlying.plus(1.second).toJava)

      // The slot should be released
      checkMetrics()
      eventually(2.seconds) {
        // both members' streams should be completed exactly once
        verify(resObs1, times(1)).onCompleted()
        verify(resObs2, times(1)).onCompleted()
        channelPool.size shouldBe 0
      }
    }

    "measure the channel TTL from the first member's arrival" in withEnv(channelTtl = channelTtl) {
      env =>
        import env.*

        val startingTime = clock.now

        val (resObs1, _) = createAndInitializeChannel(channelId1, member1, member2)
        checkMetrics(waiting = 1)
        assertNumOfScheduledTasks(1)

        clock.advanceTo(startingTime.add(channelTtl.underlying).minusSeconds(10))
        checkMetrics(waiting = 1)
        assertNumOfScheduledTasks(1)

        // second member joins the channel 10s before its expiration time
        val (resObs2, _) = createAndInitializeChannel(channelId1, member2, member1)
        checkMetrics(active = 1)
        assertNumOfScheduledTasks(1)

        // now we move to 1s past the initial channel expiration time
        clock.advanceTo(startingTime.add(channelTtl.underlying).plusSeconds(1))

        // The slot should be released and connections closed ~11s after the second member joined
        checkMetrics()
        eventually(2.seconds) {
          // both members' streams should be completed exactly once
          verify(resObs1, times(1)).onCompleted()
          verify(resObs2, times(1)).onCompleted()
          channelPool.size shouldBe 0
        }
    }

  }
}
