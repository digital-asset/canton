// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import java.util.concurrent.{CompletableFuture, CompletionStage}
import com.daml.ledger.api.auth.{AuthService, ClaimSet}
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.util.{HexString, NoCopy}
import io.grpc.Metadata

case class CantonAdminToken private (secret: String) extends NoCopy
object CantonAdminToken {
  private[this] def apply(secret: String): CantonAdminToken =
    throw new UnsupportedOperationException("Use the create method instead.")

  def create(): CantonAdminToken = {
    val secret = HexString.toHexString(SecureRandomness.randomByteString(64))
    new CantonAdminToken(secret)
  }
}

/** AuthService interceptor used for internal canton services
  *
  * Internal Canton services such as the PingService or the DarService require access to the
  * Ledger-Api server. However, if the Ledger-Api server is configured with JWT, they will fail.
  * But we can't expect that Canton obtains an oauth token from a third party service during startup.
  *
  * Therefore, we create on each startup a master token which is only ever shared internally.
  */
class CantonAdminTokenAuthService(adminToken: CantonAdminToken, parent: Seq[AuthService])
    extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] = {
    val bearerTokenRegex = "Bearer (.*)".r
    val authToken = for {
      authKey <- Option(headers.get(AUTHORIZATION_KEY))
      token <- bearerTokenRegex.findFirstMatchIn(authKey).map(_.group(1))
      _ <- if (token == adminToken.secret) Some(()) else None
    } yield ()
    authToken
      .map(_ => wildcard)
      .getOrElse(if (parent.isEmpty) wildcard else decodeMetadataParent(headers))
  }

  private val wildcard = CompletableFuture.completedFuture(ClaimSet.Claims.Wildcard: ClaimSet)
  private val deny = CompletableFuture.completedFuture(ClaimSet.Unauthenticated: ClaimSet)

  private def decodeMetadataParent(headers: Metadata): CompletionStage[ClaimSet] = {
    // iterate until we find one claim set which is not unauthenticated
    parent.foldLeft(deny) { case (acc, elem) =>
      acc.thenCompose { prevClaims =>
        if (prevClaims != ClaimSet.Unauthenticated)
          CompletableFuture.completedFuture(prevClaims)
        else
          elem.decodeMetadata(headers)
      }
    }
  }

}
