// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerService._
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.protocol.{DomainParametersLookup, v0 => protocolV0}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.time.{Clock, TimeProof}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.digitalasset.canton.util.{EitherTUtil, RateLimiter}
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.empty.Empty
import io.functionmeta.functionFullName
import io.grpc.Status
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Authenticate the current user can perform an operation on behalf of the given member */
trait AuthenticationCheck {

  /** Can the current user perform an action on behalf of the provided member.
    * Return a left with a user presentable error message if not.
    * Right if the operation can continue.
    */
  def authenticate(member: Member): Either[String, Unit]
}

object AuthenticationCheck {
  @VisibleForTesting
  trait MatchesAuthenticatedMember extends AuthenticationCheck {
    def lookupCurrentMember(): Option[Member]

    override def authenticate(member: Member): Either[String, Unit] = {
      val authenticatedMember = lookupCurrentMember()
      // fwiw I don't think it will be possible to reach this check for being the right member
      // if there is no member authenticated, but prepare some text for that scenario just in case.
      val authenticatedMemberText =
        authenticatedMember.map(_.toString).getOrElse("[unauthenticated]")

      Either.cond(
        authenticatedMember.contains(member),
        (),
        s"Authenticated member $authenticatedMemberText just tried to use sequencer on behalf of $member without permission",
      )
    }
  }

  /** Check the member matches member available from the GRPC context */
  object AuthenticationToken extends MatchesAuthenticatedMember {
    override def lookupCurrentMember(): Option[Member] =
      IdentityContextHelper.getCurrentStoredMember
  }

  /** No authentication check is performed */
  object Disabled extends AuthenticationCheck {
    override def authenticate(member: Member): Either[String, Unit] = Right(())
  }
}

object GrpcSequencerService {
  def apply(
      sequencer: Sequencer,
      metrics: SequencerMetrics,
      auditLogger: TracedLogger,
      authenticationCheck: AuthenticationCheck,
      clock: Clock,
      maxRatePerParticipantLookup: DomainParametersLookup[NonNegativeInt],
      maxInboundMessageSize: NonNegativeInt,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): GrpcSequencerService =
    new GrpcSequencerService(
      sequencer,
      metrics,
      loggerFactory,
      auditLogger,
      authenticationCheck,
      new SubscriptionPool[GrpcManagedSubscription](clock, metrics, timeouts, loggerFactory),
      new DirectSequencerSubscriptionFactory(sequencer, timeouts, loggerFactory),
      maxRatePerParticipantLookup,
      maxInboundMessageSize,
      timeouts,
    )

  private sealed trait WrappedSubmissionRequest[+A] extends Product with Serializable {
    def unwrap: SubmissionRequest
    def proto: v0.SubmissionRequest
    def fullRequest: A
  }
  private case class PlainSubmissionRequest(
      request: SubmissionRequest,
      override val proto: v0.SubmissionRequest,
  ) extends WrappedSubmissionRequest[SubmissionRequest] {
    override def unwrap: SubmissionRequest = request
    override def fullRequest: SubmissionRequest = request
  }
  private case class SignedSubmissionRequest(signedRequest: SignedContent[SubmissionRequest])
      extends WrappedSubmissionRequest[SignedContent[SubmissionRequest]] {
    override def unwrap: SubmissionRequest = signedRequest.content
    override lazy val proto: v0.SubmissionRequest = signedRequest.content.toProtoV0
    override def fullRequest: SignedContent[SubmissionRequest] = signedRequest
  }
}

/** Service providing a GRPC connection to the [[sequencer.Sequencer]] instance.
  *
  * @param sequencer The underlying sequencer implementation
  */
class GrpcSequencerService(
    sequencer: Sequencer,
    metrics: SequencerMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    auditLogger: TracedLogger,
    authenticationCheck: AuthenticationCheck,
    subscriptionPool: SubscriptionPool[GrpcManagedSubscription],
    directSequencerSubscriptionFactory: DirectSequencerSubscriptionFactory,
    maxRatePerParticipantLookup: DomainParametersLookup[NonNegativeInt],
    maxInBoundMessageSize: NonNegativeInt,
    override protected val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends v0.SequencerServiceGrpc.SequencerService
    with NamedLogging
    with FlagCloseable {

  private val rates = new TrieMap[ParticipantId, RateLimiter]()

  def membersWithActiveSubscriptions: Seq[Member] =
    subscriptionPool.activeSubscriptions().map(_.member)
  def disconnectMember(member: Member)(implicit traceContext: TraceContext): Unit =
    subscriptionPool.closeSubscriptions(member)
  def disconnectAllMembers()(implicit traceContext: TraceContext): Unit =
    subscriptionPool.closeAllSubscriptions()

  override def sendAsyncSigned(request: protocolV0.SignedContent): Future[v0.SendAsyncResponse] =
    send[SignedContent[SubmissionRequest]](
      SignedContent
        .fromProtoV0[SubmissionRequest](
          SubmissionRequest.fromByteString(MaxRequestSize.Limit(maxInBoundMessageSize)),
          request,
        )
        .map(request => GrpcSequencerService.SignedSubmissionRequest(request))
    )

  override def sendAsync(requestP: v0.SubmissionRequest): Future[v0.SendAsyncResponse] =
    send[SubmissionRequest](
      SubmissionRequest
        .fromProtoV0(requestP, MaxRequestSize.Limit(maxInBoundMessageSize))
        .map(request => GrpcSequencerService.PlainSubmissionRequest(request, requestP))
    )

  private def send[Req](
      deserealize: => Either[
        ProtoDeserializationError,
        WrappedSubmissionRequest[Req],
      ]
  ): Future[v0.SendAsyncResponse] = fromGrpcContext { implicit traceContext =>
    lazy val sendF = {
      val validatedRequestEither: Either[SendAsyncError, WrappedSubmissionRequest[Req]] = for {
        result <- deserealize
          .leftMap { error =>
            // Todo(i10271): add an alarm to report a deserialization error.
            logger.warn(error.toString)
            SendAsyncError.RequestInvalid(error.toString)
          }
        // validateSubmissionRequest is thread-local and therefore we need to validate the submission request
        // before we switch threads
        validatedRequest <- validateSubmissionRequest(result.proto, result.unwrap)
        _ <- checkAuthenticatedSenderPermission(validatedRequest)
      } yield result

      val validatedRequestF =
        for {
          validatedRequest <- EitherT.fromEither[Future](validatedRequestEither)
          _ <- checkRate(validatedRequest.unwrap)
        } yield validatedRequest

      sendRequestIfValid(validatedRequestF)
    }

    val sendUnlessShutdown = performUnlessClosingF(functionFullName)(sendF)
    sendUnlessShutdown.onShutdown(
      SendAsyncResponse(error = Some(SendAsyncError.ShuttingDown())).toProtoV0: v0.SendAsyncResponse
    )
  }
  private def checkAuthenticatedSenderPermission(
      submissionRequest: SubmissionRequest
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = {
    val sender = submissionRequest.sender
    sender match {
      case authMember: AuthenticatedMember =>
        checkAuthenticatedSendPermission(
          submissionRequest,
          authMember,
          sender,
        )
      case _: UnauthenticatedMemberId =>
        Left(
          refuse(submissionRequest.messageId.unwrap, sender)(
            s"Sender $sender needs to use unauthenticated send operation"
          )
        )
    }
  }

  override def sendAsyncUnauthenticated(
      requestP: v0.SubmissionRequest
  ): Future[v0.SendAsyncResponse] =
    fromGrpcContext { implicit traceContext =>
      lazy val sendF = {
        val messageIdP = requestP.messageId
        // validateSubmissionRequest is thread-local and therefore we need to validate the submission request
        // before we switch threads
        val validatedRequestEither = for {
          request <- SubmissionRequest
            .fromProtoV0(requestP, MaxRequestSize.Limit(maxInBoundMessageSize))
            .leftMap(err => SendAsyncError.RequestInvalid(err.toString))
          validatedRequest <- validateSubmissionRequest(requestP, request)
          sender = validatedRequest.sender
          _ <- sender match {
            case _: UnauthenticatedMemberId =>
              checkUnauthenticatedSendPermission(messageIdP, validatedRequest, sender)
            case _: AuthenticatedMember =>
              Left(
                refuse(messageIdP, sender)(
                  s"Sender $sender needs to use authenticated send operation"
                )
              )
          }
        } yield validatedRequest

        val validatedRequestF =
          for {
            validatedRequest <- EitherT.fromEither[Future](validatedRequestEither)
            _ <- checkRate(validatedRequest)
          } yield PlainSubmissionRequest(validatedRequest, requestP): WrappedSubmissionRequest[
            SubmissionRequest
          ]

        sendRequestIfValid(validatedRequestF)
      }

      performUnlessClosingF(functionFullName)(sendF).onShutdown(
        SendAsyncResponse(error = Some(SendAsyncError.ShuttingDown())).toProtoV0
      )
    }

  private def sendRequestIfValid[Req](
      validatedRequestEither: EitherT[Future, SendAsyncError, WrappedSubmissionRequest[Req]]
  )(implicit traceContext: TraceContext): Future[v0.SendAsyncResponse] = {
    val resultET = for {
      validatedRequest <- validatedRequestEither
      _ <- validatedRequest match {
        case p: PlainSubmissionRequest => sequencer.sendAsync(p.request)
        case s: SignedSubmissionRequest => sequencer.sendAsyncSigned(s.signedRequest)
      }
    } yield ()

    resultET
      .fold(err => Some(err.toProtoV0), _ => None) // extract the error if available
      .map(v0.SendAsyncResponse(_))
  }

  private def validateSubmissionRequest(
      requestP: v0.SubmissionRequest,
      request: SubmissionRequest,
  )(implicit traceContext: TraceContext): Either[SendAsyncError, SubmissionRequest] = {
    val messageIdP = requestP.messageId

    val requestSize = requestP.serializedSize

    // TODO(i2741) properly deal with malicious behaviour
    def refuseUnless(
        sender: Member
    )(condition: Boolean, message: => String): Either[SendAsyncError, Unit] =
      Either.cond(condition, (), refuse(messageIdP, sender)(message))

    def invalidUnless(
        sender: Member
    )(condition: Boolean, message: => String): Either[SendAsyncError, Unit] =
      Either.cond(condition, (), invalid(messageIdP, sender)(message))

    for {
      sender <- extractSender(messageIdP, requestP.sender)

      // do the security checks

      _ <- authenticationCheck // this method is thread-local.
        .authenticate(sender)
        .leftMap(err => refuse(messageIdP, sender)(s"$sender is not authorized to send: $err"))

      // TODO(i10107): remove this check because it's redundant (done by the client and by the netty channel)
      _ <- refuseUnless(sender)(
        requestSize <= maxInBoundMessageSize.unwrap,
        s"Request from '$sender' of size ($requestSize bytes) is exceeding maximum size ($maxInBoundMessageSize bytes).",
      )

      _ = {
        val envelopesCount = request.batch.envelopesCount
        auditLogger.info(
          s"'$sender' sends request with id '$messageIdP' of size $requestSize bytes with $envelopesCount envelopes."
        )
      }

      // check everything else
      _ <- invalidUnless(sender)(
        request.batch.envelopes.forall(_.recipients.allRecipients.nonEmpty),
        "Batch contains envelope without recipients.",
      )
      _ <- invalidUnless(sender)(
        request.batch.envelopes.forall(!_.bytes.isEmpty),
        "Batch contains envelope without content.",
      )
      _ <- refuseUnless(sender)(
        atMostOneMediator(sender, request.batch.envelopes),
        "Batch from participant contains multiple mediators as recipients.",
      )
      _ <- refuseUnless(sender)(
        noSigningTimestampIfUnauthenticated(
          sender,
          request.timestampOfSigningKey,
          request.batch.envelopes,
        ),
        "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key",
      )
    } yield {
      metrics.bytesProcessed.metric.mark(requestSize.toLong)
      metrics.messagesProcessed.metric.mark()
      if (TimeProof.isTimeProofSubmission(request)) metrics.timeRequests.metric.mark()

      request
    }
  }

  /** Reject requests from participants that try to send something to multiple mediators.
    * Mediators are identified by their [[com.digitalasset.canton.topology.KeyOwnerCode]]
    * rather than by the topology snapshot's [[com.digitalasset.canton.topology.client.MediatorDomainStateClient.mediators]]
    * because the submission has not yet been sequenced and we therefore do not yet know the topology snapshot.
    */
  private def atMostOneMediator(sender: Member, envelopes: Seq[ClosedEnvelope]): Boolean = {
    sender match {
      case ParticipantId(_) =>
        val allMediatorRecipients =
          envelopes.foldLeft(Set.empty[MediatorId]) { (acc, envelope) =>
            val mediatorRecipients = envelope.recipients.allRecipients.collect {
              case mediatorId: MediatorId => mediatorId
            }
            acc.union(mediatorRecipients)
          }
        allMediatorRecipients.sizeCompare(1) <= 0
      case _ => true
    }
  }

  /** Reject requests that involve unauthenticated members and specify the timestamp of the signing key.
    * This is because the unauthenticated member typically does not know the domain topology state
    * and therefore cannot validate that the requested timestamp is within the signing tolerance.
    */
  private def noSigningTimestampIfUnauthenticated(
      sender: Member,
      timestampOfSigningKey: Option[CantonTimestamp],
      envelopes: Seq[ClosedEnvelope],
  ): Boolean =
    timestampOfSigningKey.isEmpty || (sender.isAuthenticated && envelopes.forall(
      _.recipients.allRecipients.forall(_.isAuthenticated)
    ))

  private def invalid(messageIdP: String, senderPO: String)(
      message: String
  )(implicit traceContext: TraceContext): SendAsyncError = {
    val senderText = if (senderPO.isEmpty) "[sender-not-set]" else senderPO
    logger.warn(s"Request '$messageIdP' from '$senderText' is invalid: $message")
    SendAsyncError.RequestInvalid(message)
  }

  private def invalid(messageIdP: String, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SendAsyncError = {
    logger.warn(s"Request '$messageIdP' from '$sender' is invalid: $message")
    SendAsyncError.RequestInvalid(message)
  }

  private def refuse(messageIdP: String, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SendAsyncError = {
    logger.warn(s"Request '$messageIdP' from '$sender' refused: $message")
    SendAsyncError.RequestRefused(message)
  }

  private def extractSender(messageIdP: String, senderP: String)(implicit
      traceContext: TraceContext
  ): Either[SendAsyncError, Member] =
    Member
      .fromProtoPrimitive(senderP, "member")
      .leftMap(err => invalid(messageIdP, senderP)(s"Unable to parse sender: $err"))

  private def checkAuthenticatedSendPermission(
      request: SubmissionRequest,
      authMember: AuthenticatedMember,
      sender: Member,
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = authMember match {
    case _: DomainTopologyManagerId =>
      Right(())
    case _ =>
      val unauthRecipients = request.batch.envelopes
        .toSet[ClosedEnvelope]
        .flatMap(_.recipients.allRecipients)
        .collect { case unauthMember: UnauthenticatedMemberId =>
          unauthMember
        }
      Either.cond(
        unauthRecipients.isEmpty,
        (),
        refuse(request.messageId.unwrap, sender)(
          s"Member is trying to send message to unauthenticated ${unauthRecipients.mkString(" ,")}. Only domain manager can do that."
        ),
      )
  }

  private def checkUnauthenticatedSendPermission(
      messageIdP: String,
      request: SubmissionRequest,
      sender: Member,
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = sender match {
    case _: UnauthenticatedMemberId =>
      // unauthenticated member can only send messages to IDM
      val nonIdmRecipients = request.batch.envelopes
        .toSet[ClosedEnvelope]
        .flatMap(_.recipients.allRecipients)
        .filter {
          case _: DomainTopologyManagerId => false
          case _ => true
        }
      Either.cond(
        nonIdmRecipients.isEmpty,
        (),
        refuse(messageIdP, sender)(
          s"Unauthenticated member is trying to send message to members other than the domain manager: ${nonIdmRecipients
            .mkString(" ,")}."
        ),
      )
    case _ => Right(())
  }

  private def checkRate(
      request: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit] = {
    val sender = request.sender
    def checkRate(
        participantId: ParticipantId,
        maxRatePerParticipant: NonNegativeInt,
    ): Either[SendAsyncError, Unit] = {
      val limiter = getOrUpdateRateLimiter(participantId, maxRatePerParticipant)
      Either.cond(
        limiter.checkAndUpdateRate(),
        (), {
          val message = f"Submission rate exceeds rate limit of $maxRatePerParticipant/s."
          logger.info(
            f"Request '${request.messageId}' from '$sender' refused: $message"
          )
          SendAsyncError.Overloaded(message)
        },
      )
    }

    sender match {
      case participantId: ParticipantId if request.isRequest =>
        for {
          // TODO(i10191): Create a specific error type if getApproximate fails
          maxRatePerParticipant <- EitherTUtil.fromFuture(
            maxRatePerParticipantLookup.getApproximate,
            e => SendAsyncError.RequestInvalid(message = e.getMessage),
          )
          _ <- EitherT.fromEither[Future](checkRate(participantId, maxRatePerParticipant))
        } yield ()
      case _ =>
        // No rate limitation for domain entities and non-requests
        // TODO(i2898): verify that the sender is not lying about the request nature to bypass the rate limitation
        EitherT.rightT[Future, SendAsyncError](())
    }
  }
  private def getOrUpdateRateLimiter(
      participantId: ParticipantId,
      maxRatePerParticipant: NonNegativeInt,
  ): RateLimiter = {
    rates.get(participantId) match {
      case Some(rateLimiter) =>
        if (rateLimiter.maxTasksPerSecond == maxRatePerParticipant) rateLimiter
        else {
          val newRateLimiter = new RateLimiter(maxRatePerParticipant)
          rates.update(participantId, newRateLimiter)
          newRateLimiter
        }
      case None =>
        rates.getOrElseUpdate(participantId, new RateLimiter(maxRatePerParticipant))
    }
  }

  override def subscribe(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.SubscriptionResponse],
  ): Unit =
    subscribeInternal(request, responseObserver, requiresAuthentication = true)

  override def subscribeUnauthenticated(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.SubscriptionResponse],
  ): Unit =
    subscribeInternal(request, responseObserver, requiresAuthentication = false)

  private def subscribeInternal(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.SubscriptionResponse],
      requiresAuthentication: Boolean,
  ): Unit =
    fromGrpcContext { implicit traceContext =>
      withServerCallStreamObserver(responseObserver) { observer =>
        val result = for {
          subscriptionRequest <- SubscriptionRequest
            .fromProtoV0(request)
            .left
            .map(err => invalidRequest(err.toString))
          SubscriptionRequest(member, offset) = subscriptionRequest
          _ = logger.debug(s"Received subscription request from $member for offset $offset")
          _ <- Either.cond(
            !isClosing,
            (),
            Status.UNAVAILABLE.withDescription("Domain is being shutdown."),
          )
          _ <- checkSubscriptionMemberPermission(member, requiresAuthentication)
          authenticationTokenO = IdentityContextHelper.getCurrentStoredAuthenticationToken
          _ <- subscriptionPool
            .create(
              () =>
                createSubscription(
                  member,
                  authenticationTokenO.map(_.expireAt),
                  offset,
                  observer,
                ),
              member,
            )
            .leftMap { case SubscriptionPool.PoolClosed =>
              Status.UNAVAILABLE.withDescription("Subscription pool is closed.")
            }
        } yield ()
        result.fold(err => responseObserver.onError(err.asException()), identity)
      }
    }

  private def checkSubscriptionMemberPermission(member: Member, requiresAuthentication: Boolean)(
      implicit traceContext: TraceContext
  ): Either[Status, Unit] =
    (member, requiresAuthentication) match {
      case (authMember: AuthenticatedMember, true) =>
        checkAuthenticatedMemberPermission(authMember)
      case (authMember: AuthenticatedMember, false) =>
        Left(
          Status.PERMISSION_DENIED.withDescription(
            s"Member $authMember needs to use authenticated subscribe operation"
          )
        )
      case (_: UnauthenticatedMemberId, false) =>
        Right(())
      case (unauthMember: UnauthenticatedMemberId, true) =>
        Left(
          Status.PERMISSION_DENIED.withDescription(
            s"Member $unauthMember cannot use authenticated subscribe operation"
          )
        )
    }

  override def acknowledge(requestP: v0.AcknowledgeRequest): Future[Empty] = {
    fromGrpcContext { implicit traceContext =>
      // deserialize the request and check that they're authorized to perform a request on behalf of the member.
      // intentionally not using an EitherT here as we want to remain on the same thread to retain the GRPC context
      // for authorization.
      val validatedRequestE = for {
        request <- AcknowledgeRequest
          .fromProtoV0(requestP)
          .leftMap(err => invalidRequest(err.toString).asException())
        // check they are authenticated to perform actions on behalf of this member
        _ <- checkAuthenticatedMemberPermission(request.member)
          .leftMap(_.asException())
      } yield request

      validatedRequestE.fold(
        Future.failed,
        request => {
          for {
            _ <- sequencer.acknowledge(request.member, request.timestamp)
          } yield Empty()
        },
      )
    }
  }

  private def createSubscription(
      member: Member,
      expireAt: Option[CantonTimestamp],
      counter: SequencerCounter,
      observer: ServerCallStreamObserver[v0.SubscriptionResponse],
  )(implicit traceContext: TraceContext): GrpcManagedSubscription = {
    member match {
      case ParticipantId(uid) =>
        auditLogger.info(s"$uid creates subscription from $counter")
      case _ => ()
    }
    new GrpcManagedSubscription(
      handler => directSequencerSubscriptionFactory.create(counter, "direct", member, handler),
      observer,
      member,
      expireAt,
      timeouts,
      loggerFactory,
    )
  }

  /** Ensure observer is a ServerCalLStreamObserver
    *
    * @param observer underlying observer
    * @param handler  handler requiring a ServerCallStreamObserver
    */
  private def withServerCallStreamObserver[R](
      observer: StreamObserver[R]
  )(handler: ServerCallStreamObserver[R] => Unit)(implicit traceContext: TraceContext): Unit =
    observer match {
      case serverCallStreamObserver: ServerCallStreamObserver[R] =>
        handler(serverCallStreamObserver)
      case _ =>
        val statusException = internalError("Unknown stream observer request").asException()
        logger.warn(statusException.getMessage)
        observer.onError(statusException)
    }

  private def checkAuthenticatedMemberPermission(
      member: Member
  )(implicit traceContext: TraceContext): Either[Status, Unit] =
    authenticationCheck
      .authenticate(member)
      .leftMap { message =>
        logger.warn(s"Authentication check failed: $message")
        permissionDenied(message)
      }

  private def invalidRequest(message: String): Status =
    Status.INVALID_ARGUMENT.withDescription(message)

  private def internalError(message: String): Status = Status.INTERNAL.withDescription(message)

  private def permissionDenied(message: String): Status =
    Status.PERMISSION_DENIED.withDescription(message)

  override def onClosed(): Unit = {
    subscriptionPool.close()
  }

}
