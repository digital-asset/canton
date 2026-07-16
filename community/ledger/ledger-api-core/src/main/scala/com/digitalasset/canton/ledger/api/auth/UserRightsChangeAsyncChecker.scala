// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.auth.{ClaimSet, UserBasedClaimResolver}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.user.store.{IdentityProviderConfigStore, UserManagementStore}
import com.digitalasset.canton.user.{IdentityProviderConfig, IdentityProviderId, User, UserRight}
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.actor.Scheduler

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[auth] final class UserRightsChangeAsyncChecker(
    lastUserRightsCheckTime: AtomicReference[Instant],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    userManagementStore: UserManagementStore,
    identityProviderConfigStore: IdentityProviderConfigStore,
    userRightsCheckIntervalInSeconds: Int,
    pekkoScheduler: Scheduler,
)(implicit ec: ExecutionContext) {

  /** Schedules an asynchronous and periodic task to check for user rights' state changes
    * @param userClaimsMismatchCallback
    *   called when user rights' state change has been detected.
    * @return
    *   a function to cancel the scheduled task
    */
  def schedule(
      userClaimsMismatchCallback: () => Unit
  )(implicit loggingContext: LoggingContextWithTrace): () => Unit = {
    val delay = userRightsCheckIntervalInSeconds.seconds
    val identityProviderId = originalClaims.identityProviderId
    val userId = originalClaims.userId.fold[Ref.UserId](
      throw new RuntimeException(
        "Claims were resolved from a user but userId is missing in the claims."
      )
    )(Ref.UserId.assertFromString)
    assert(
      originalClaims.resolvedFromUser,
      "The claims were not resolved from a user. Expected claims resolved from a user.",
    )
    // Note: https://doc.akka.io/docs/akka/2.6.13/scheduler.html states that:
    // "All scheduled task will be executed when the ActorSystem is terminated, i.e. the task may execute before its timeout."
    val cancellable =
      pekkoScheduler.scheduleWithFixedDelay(initialDelay = delay, delay = delay) { () =>
        val idpId = IdentityProviderId.fromOptionalLedgerString(identityProviderId)
        val userState: Future[
          Either[String, (User, Set[UserRight], Option[IdentityProviderConfig])]
        ] =
          for {
            userRightsResult <- userManagementStore.listUserRights(userId, idpId)
            userResult <- userManagementStore.getUser(userId, idpId)
            idpConfigOResult <- idpId.toDb.traverse(id =>
              identityProviderConfigStore.getIdentityProviderConfig(id)
            )
          } yield {
            for {
              userRights <- userRightsResult.leftMap(_.toString)
              user <- userResult.leftMap(_.toString)
              idpConfigO <- idpConfigOResult.sequence.leftMap(_.toString)
            } yield (user, userRights, idpConfigO)
          }
        userState
          .onComplete {
            case Failure(_) | Success(Left(_)) =>
              userClaimsMismatchCallback()
            case Success(Right((user, userRights, idpConfig))) =>
              val updatedClaims =
                UserBasedClaimResolver.convertUserRightsToClaims(userRights, idpId)
              if (
                idpConfig.exists(_.isDeactivated) ||
                user.isDeactivated ||
                updatedClaims.toSet != originalClaims.claims.toSet
              ) {
                userClaimsMismatchCallback()
              }
              lastUserRightsCheckTime.set(nowF())
          }
      }
    () => (cancellable.cancel(): Unit)
  }

}
