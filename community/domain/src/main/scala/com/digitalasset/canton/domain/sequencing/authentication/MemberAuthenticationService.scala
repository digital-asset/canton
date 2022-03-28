// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import cats.data.{EitherT, NonEmptyList}
import cats.instances.future._
import cats.syntax.bifunctor._
import cats.syntax.list._
import cats.syntax.traverse._
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication._
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  ParticipantState,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureUtil

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

/** The authentication service issues tokens to members after successfully completed the following challenge
  * response protocol and after they have accepted the service agreement of the domain. The tokens are required for
  * connecting to the sequencer.
  *
  * In order for a member to subscribe to the sequencer, it must follow a few steps for it to authenticate.
  * Assuming the domain already has knowledge of the member's public keys, the following steps are to be taken:
  *   1. member sends request to the domain for authenticating
  *   2. domain returns a nonce (a challenge random number)
  *   3. member takes the nonce, concatenates it with the identity of the domain, signs it and sends it back
  *   4. domain checks the signature against the key of the member. if it matches, create a token and return it
  *   5. member will use the token when subscribing to the sequencer
  *
  * @param invalidateMemberCallback Called when a member is explicitly deactivated on the domain so all active subscriptions
  *                                 for this member should be terminated.
  */
class MemberAuthenticationService(
    domain: DomainId,
    cryptoApi: DomainSyncCryptoClient,
    store: MemberAuthenticationStore,
    agreementManager: Option[ServiceAgreementManager],
    clock: Clock,
    nonceExpirationTime: Duration,
    tokenExpirationTime: Duration,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: Future[Unit],
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    auditLogger: TracedLogger,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with DomainTopologyClient.Subscriber {

  private val tokenCache = new AuthenticationTokenCache(clock, store, loggerFactory)

  /** Domain generates nonce that he expects the participant to use to concatenate with the domain's id and sign
    * to proceed with the authentication (step 2).
    */
  def generateNonce(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AuthenticationError, (Nonce, NonEmptyList[Fingerprint])] = {
    for {
      _ <- EitherT.right(waitForInitialized)
      snapshot = cryptoApi.ips.currentSnapshotApproximation
      _ <- isActive(member)
      fingerprints <- EitherT(
        snapshot
          .signingKeys(member)
          .map(
            _.map(_.fingerprint).toList.toNel
              .toRight(NoKeysRegistered(member): AuthenticationError)
          )
      )
      nonce = Nonce.generate()
      storedNonce = StoredNonce(member, nonce, clock.now, nonceExpirationTime)
      _ <- EitherT.right(store.saveNonce(storedNonce))
    } yield {
      scheduleExpirations(storedNonce.expireAt)
      (nonce, fingerprints)
    }
  }

  private def waitForInitialized(implicit traceContext: TraceContext): Future[Unit] = {
    // avoid logging if we're already done
    if (isTopologyInitialized.isCompleted) isTopologyInitialized
    else {
      logger.debug(s"Waiting for topology to be initialized")

      isTopologyInitialized.map { _ =>
        logger.debug(s"Topology has been initialized")
      }
    }
  }

  /** Domain checks that the signature given by the member matches and returns a token if it does (step 4)
    * Al
    */
  def validateSignature(member: Member, signature: Signature, providedNonce: Nonce)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AuthenticationError, AuthenticationTokenWithExpiry] =
    for {
      _ <- EitherT.right(waitForInitialized)
      _ <- isActive(member)
      value <- EitherT(
        store
          .fetchAndRemoveNonce(member, providedNonce)
          .map(ignoreExpired)
          .map(_.toRight(MissingNonce(member): AuthenticationError))
      )
      StoredNonce(_, nonce, generatedAt, _expireAt) = value
      agreementId = agreementManager.map(_.agreement.id)
      authentication <- EitherT.fromEither(MemberAuthentication(member))
      hash = authentication.hashDomainNonce(nonce, domain, agreementId, cryptoApi.pureCrypto)
      snapshot = cryptoApi.currentSnapshotApproximation

      _ <- snapshot.verifySignature(hash, member, signature).leftMap { err =>
        logger.warn(s"Member $member provided invalid signature: $err")
        InvalidSignature(member)
      }
      // If an agreement manager is set, we store the acceptance
      _ <- agreementManager.fold(EitherT.rightT[Future, AuthenticationError](())) { manager =>
        storeAcceptedAgreement(member, manager, manager.agreement.id, signature, generatedAt)
      }
      token = AuthenticationToken.generate()
      tokenExpiry = clock.now.add(tokenExpirationTime)
      storedToken = StoredAuthenticationToken(member, tokenExpiry, token)
      _ <- EitherT.right(tokenCache.saveToken(storedToken))
    } yield {
      auditLogger.info(
        s"$member authenticated new token based on agreement $agreementId on $domain"
      )
      AuthenticationTokenWithExpiry(token, tokenExpiry)
    }

  /** Domain checks if the token given by the participant is the one previously assigned to it for authentication.
    * The participant also provides the domain id for which they think they are connecting to. If this id does not match
    * this domain's id, it means the participant was previously connected to a different domain on the same address and
    * now should be informed that this address now hosts a different domain.
    */
  def validateToken(intendedDomain: DomainId, member: Member, token: AuthenticationToken)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AuthenticationError, StoredAuthenticationToken] =
    for {
      _ <- EitherT.fromEither[Future](correctDomain(member, intendedDomain))
      validTokenO <- EitherT.right(tokenCache.lookupMatchingToken(member, token))
      validToken <- EitherT
        .fromEither[Future](validTokenO.toRight(MissingToken(member)))
        .leftWiden[AuthenticationError]
    } yield validToken

  private def ignoreExpired[A <: HasExpiry](itemO: Option[A]): Option[A] =
    itemO.filter(_.expireAt > clock.now)

  private def scheduleExpirations(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit = {
    def run(): Unit = FutureUtil.doNotAwait(
      performUnlessClosingF {
        val now = clock.now
        logger.debug(s"Expiring nonces and tokens up to $now")
        store.expireNoncesAndTokens(now)
      }.unwrap,
      "Expiring nonces and tokens failed",
    )
    clock.scheduleAt(_ => run(), timestamp).discard
  }

  private def isActive(
      member: Member
  )(implicit traceContext: TraceContext): EitherT[Future, AuthenticationError, Unit] =
    member match {
      case participant: ParticipantId =>
        EitherT(isParticipantActive(participant).map {
          if (_) Right(()) else Left(ParticipantDisabled(participant))
        })
      // consider all types of members always active
      case _ => EitherT.pure[Future, AuthenticationError](())
    }

  private def storeAcceptedAgreement(
      member: Member,
      agreementManager: ServiceAgreementManager,
      agreementId: ServiceAgreementId,
      signature: Signature,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, AuthenticationError, Unit] =
    member match {
      case participant: ParticipantId =>
        agreementManager
          .insertAcceptance(agreementId, participant, signature, timestamp)
          .leftMap(err => ServiceAgreementAcceptanceError(member, err.toString))
      case _ => EitherT.rightT(())
    }

  private def correctDomain(
      member: Member,
      intendedDomain: DomainId,
  ): Either[AuthenticationError, Unit] =
    Either.cond(intendedDomain == domain, (), NonMatchingDomainId(member, intendedDomain))

  protected def isParticipantActive(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = {
    cryptoApi.snapshot(cryptoApi.topologyKnownUntilTimestamp).flatMap { snapshot =>
      // we are a bit more conservative here. a participant needs to be active NOW and the head state (i.e. effective in the future)
      Seq(snapshot.ipsSnapshot, cryptoApi.currentSnapshotApproximation.ipsSnapshot)
        .traverse(
          _.isParticipantActive(participant)
        )
        .map(_.forall(identity))
    }
  }

  /** domain topology client subscriber used to remove member tokens if they get disabled */
  override def observed(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Unit = {
    transactions.map(_.transaction.element.mapping).foreach {
      case ParticipantState(_, _, participant, ParticipantPermission.Disabled, _) =>
        def invalidateAndExpire: Future[Unit] = {
          isParticipantActive(participant).flatMap { isActive =>
            if (!isActive) {
              logger.debug(s"Expiring all auth-tokens of ${participant}")
              tokenCache
                // first, remove all auth tokens
                .invalidAllTokensForMember(participant)
                // second, ensure the sequencer client gets disconnected
                .map(_ => invalidateMemberCallback(Traced(participant)))
            } else Future.unit
          }
        }
        FutureUtil.doNotAwait(
          invalidateAndExpire,
          s"Invalidating participant authentication for $participant",
        )
      case _ =>
    }
  }

  override def onClosed(): Unit = Lifecycle.close(store)(logger)
}
