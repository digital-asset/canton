// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, Nonce}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PriorityBlockingQueueUtil
import com.google.common.annotations.VisibleForTesting

import java.time.Duration
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

sealed trait HasExpiry {
  val expireAt: CantonTimestamp
}

final case class StoredNonce(
    member: Member,
    nonce: Nonce,
    generatedAt: CantonTimestamp,
    expireAt: CantonTimestamp,
) extends HasExpiry

object StoredNonce {
  def apply(
      member: Member,
      nonce: Nonce,
      generatedAt: CantonTimestamp,
      expirationDuration: Duration,
  ): StoredNonce =
    StoredNonce(member, nonce, generatedAt, generatedAt.add(expirationDuration))
}
final case class StoredAuthenticationToken(
    member: Member,
    expireAt: CantonTimestamp,
    token: AuthenticationToken,
    signingKeyFingerprint: Fingerprint, // track the key used to allow for per-key token revocations
) extends HasExpiry

class MemberAuthenticationStore(
    maxTokensPerMember: PositiveInt,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private implicit val hasExpiryOrdering: Ordering[HasExpiry] = Ordering.by(_.expireAt)
  // we store exactly one nonce per member and return it for each request until it is consumed or expired
  private val nonces = new AtomicReference[Map[Member, StoredNonce]](Map.empty)
  // note we only need to remember the tokens by member so we can invalidate them on request
  private val tokens = new TrieMap[Member, List[StoredAuthenticationToken]]()
  private val tokenLookup = new TrieMap[AuthenticationToken, StoredAuthenticationToken]()
  private val expiryQueue = new PriorityBlockingQueue[HasExpiry](
    PriorityBlockingQueueUtil.DefaultInitialCapacity,
    Ordering[HasExpiry],
  )

  // ==============================================================================================
  // NONCE OPERATIONS
  // ==============================================================================================

  /** If a nonce exists for the member, return it. If not, generate it using the passed generation
    * logic in the by-name parameter.
    */
  def fetchOrGenerateNonce(member: Member, freshNonce: => StoredNonce): StoredNonce =
    nonces.get().get(member) match {
      case Some(existing) => existing
      // case where the member doesn't exist as a key in the map
      case None =>
        // The nonce is generated only if it doesn't already exist
        val generated = freshNonce

        val finalMap = nonces.updateAndGet { currentMap =>
          // Check again if the member exists as a key in the map.
          // This prevents race conditions between threads calling the
          // method concurrently, by ensuring the insertion is done
          // only once by the winning thread.
          if (currentMap.contains(member)) currentMap
          // Insert the nonce only if the member is not in the current map
          else currentMap + (member -> generated)
        }

        val activeNonce = finalMap.getOrElse(member, generated)

        // Only queue for expiration if the generated nonce is still the active one
        // The additional check is to mitigate race conditions
        // if another thread generated and saved a nonce before this one, in which
        // case we just take the existing, active nonce and discard the generated one.
        if (activeNonce == generated) {
          expiryQueue.put(generated)
        }

        activeNonce
    }

  /** Matches the provided nonce with the existing unique nonce stored for the given member and
    * returns the stored nonce if it matches. If the nonce in the store does not match the provided
    * nonce or exist, returns None.
    */
  def fetchNonceIfMatches(member: Member, nonce: Nonce)(implicit
      traceContext: TraceContext
  ): Option[StoredNonce] =
    nonces.get().get(member) match {
      case Some(storedNonce) if storedNonce.nonce == nonce =>
        Some(storedNonce)
      case Some(storedNonce) =>
        logger.info(s"Received the wrong nonce $nonce for member $member. Stored: $storedNonce")
        None
      case None =>
        None
    }

  /** Matches the provided nonce with the existing unique nonce stored for the given member and
    * removes it if they are equal. If the nonce in the store does not match the provided nonce or
    * exist, changes nothing. Returns true if the nonce was removed, and false otherwise.
    */
  def removeNonceIfMatches(member: Member, expectedNonce: Nonce): Boolean = {
    val oldMap = nonces.getAndUpdate(_.updatedWith(member) {
      // remove the nonce from the store if it exists
      case Some(stored) if stored.nonce == expectedNonce => None
      // no match, leave as is
      case existing => existing
    })

    // Inspect the old snapshot to see if the present thread did the removing
    // return true if the nonce was removed, false otherwise
    oldMap.get(member).exists(_.nonce == expectedNonce)
  }

  // ==============================================================================================
  // TOKEN OPERATIONS
  // ==============================================================================================

  def saveToken(token: StoredAuthenticationToken)(implicit traceContext: TraceContext): Unit = {
    tokens
      .updateWith(token.member) {
        case Some(tokens) =>
          // remove excess tokens
          // this is fine if called multiple times as it will subsequently just be a no-op
          if (tokens.sizeIs > maxTokensPerMember.value - 1)
            tokens.drop(maxTokensPerMember.value - 1).foreach { stored =>
              tokenLookup.remove(stored.token).discard
            }
          val limitedTokens = tokens.take(maxTokensPerMember.value - 1)
          if (tokens.sizeIs > maxTokensPerMember.value - 1) {
            logger.info(
              s"Dropping excess auth token for ${token.member} as max per member is ${maxTokensPerMember.value}"
            )
          }
          Some(token :: limitedTokens)
        case None => Some(List(token))
      }
      .discard
    // this is fine racy wise. we only need this entry to invalidate the member
    tokenLookup.put(token.token, token).discard
    // same as above, we only need to remember the member for which we need to schedule a cleanup
    // by adding the token to the queue after it was added to the data structures, we'll make sure
    // that they get cleaned up when the cleanup process picks up
    expiryQueue.put(token)
  }

  def tokenForMemberAt(
      member: Member,
      token: AuthenticationToken,
      timestamp: CantonTimestamp,
  ): Option[StoredAuthenticationToken] =
    tokenLookup.get(token).filter(stored => stored.member == member && stored.expireAt > timestamp)

  def fetchMemberOfTokenForInvalidation(token: AuthenticationToken): Option[Member] =
    tokenLookup.get(token).map(_.member)

  // ==============================================================================================
  // EXPIRATION & INVALIDATION
  // ==============================================================================================

  def expireNoncesAndTokens(timestamp: CantonTimestamp): Unit = {
    // figure out which members need clean up
    val members = mutable.HashSet[Member]()
    @tailrec
    def go(): Unit = if (!expiryQueue.isEmpty && expiryQueue.peek().expireAt <= timestamp) {
      expiryQueue.poll() match {
        case StoredNonce(member, _, _, _) => members.add(member).discard
        case StoredAuthenticationToken(member, _, _, _) => members.add(member).discard
      }
      go()
    }
    go()
    members.foreach { member =>
      nonces
        .updateAndGet(_.updatedWith(member) {
          // keep the nonces that expire in the future
          case Some(nonce) if nonce.expireAt > timestamp => Some(nonce)
          case _ => None
        })
        .discard
      // iterate over the tokens which will expire and remove them from the lookup
      tokens.getOrElse(member, List.empty).filterNot(_.expireAt > timestamp).foreach { toRemove =>
        tokenLookup.remove(toRemove.token).discard
      }
      tokens
        .updateWith(member) {
          // keep tokens that expire in the future
          case Some(tokens) => noneIfEmpty(tokens.filter(_.expireAt > timestamp))
          case None => None
        }
        .discard
    }
  }

  def invalidateMember(member: Member): Unit = {
    nonces.getAndUpdate(_.removed(member)).discard
    // this is fine racy wise as the auth token itself is unique
    // while at the same time, the tokenLookup use always makes the self-consistency check
    tokens.remove(member).foreach(_.foreach(stored => tokenLookup.remove(stored.token).discard))
  }

  def invalidateTokensByFingerprint(fingerprint: Fingerprint): Unit = {
    val tokensForFingerprint =
      tokenLookup.values.filter(_.signingKeyFingerprint == fingerprint).toList
    tokensForFingerprint.foreach { stored =>
      tokenLookup.remove(stored.token).discard
      tokens
        .updateWith(stored.member) {
          case Some(memberTokens) =>
            noneIfEmpty(memberTokens.filterNot(_.token == stored.token))
          case None => None
        }
        .discard
    }
  }
  // ==============================================================================================
  // INTERNAL HELPERS
  // ==============================================================================================

  private def noneIfEmpty[T](lst: List[T]): Option[List[T]] =
    Option.when(lst.nonEmpty)(lst)

  // ==============================================================================================
  // METHODS ONLY FOR TESTING
  // ==============================================================================================

  @VisibleForTesting
  def fetchTokens(member: Member): Seq[StoredAuthenticationToken] =
    tokens.getOrElse(member, List.empty)

  /** Return whatever nonce exists in the store for the member.
    */
  @VisibleForTesting
  def fetchNonce(member: Member): Option[StoredNonce] =
    nonces.get().get(member)

  /** If a nonce exists for the member, return it. If not, save the provided nonce.
    */
  @VisibleForTesting
  def fetchOrSaveNonce(nonce: StoredNonce): StoredNonce = {
    val finalNonceMap = nonces.updateAndGet { currentMap =>
      // if there is already a nonce for the member, keep it as is
      if (currentMap.contains(nonce.member)) currentMap
      // if not, save the new nonce
      else currentMap + (nonce.member -> nonce)
    }
    // retrieve the active nonce from the map
    // (covers race conditions where another thread have modified the map in the meanwhile)
    val activeNonce = finalNonceMap.getOrElse(nonce.member, nonce)

    if (activeNonce == nonce) {
      // fine to add subsequently. we only need this to avoid doing a full-pass
      // over all tokens when we clean them up, so we eventually remove older tokens from memory
      expiryQueue.put(nonce)
    }

    // Return the active nonce
    activeNonce
  }

}
