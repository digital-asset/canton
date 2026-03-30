// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import io.circe.parser

import scala.util.Try

private[extension] final case class HttpExtensionOAuthAccessToken(
    value: String,
    expiresAtMillis: Long,
)

private[extension] final class HttpExtensionOAuthTokenResponseParser {

  def parse(
      response: HttpExtensionClientResponse,
      requestId: String,
      nowMillis: Long,
  ): Either[ExtensionCallError, HttpExtensionOAuthAccessToken] =
    for {
      json <- parser.parse(response.body).left.map(error =>
        malformedResponse(requestId, s"invalid JSON: ${error.message}")
      )
      cursor = json.hcursor
      accessToken <- requiredStringField(cursor, "access_token", requestId)
      tokenType <- requiredStringField(cursor, "token_type", requestId)
      expiresIn <- requiredLongField(cursor, "expires_in", requestId)
      _ <- Either.cond(
        tokenType.equalsIgnoreCase("Bearer"),
        (),
        malformedResponse(requestId, s"unsupported token_type: $tokenType"),
      )
      _ <- Either.cond(
        expiresIn >= 0L,
        (),
        malformedResponse(requestId, s"invalid expires_in: $expiresIn"),
      )
      expiresAtMillis <- computeExpiry(nowMillis, expiresIn, requestId)
    } yield HttpExtensionOAuthAccessToken(accessToken, expiresAtMillis)

  private def computeExpiry(
      nowMillis: Long,
      expiresInSeconds: Long,
      requestId: String,
  ): Either[ExtensionCallError, Long] =
    Try(Math.addExact(nowMillis, Math.multiplyExact(expiresInSeconds, 1000L)))
      .toEither
      .left
      .map(_ => malformedResponse(requestId, s"invalid expires_in: $expiresInSeconds"))

  private def requiredStringField(
      cursor: io.circe.HCursor,
      fieldName: String,
      requestId: String,
  ): Either[ExtensionCallError, String] =
    cursor.downField(fieldName).focus.flatMap(_.asString).toRight(
      malformedResponse(requestId, s"missing or malformed $fieldName")
    )

  private def requiredLongField(
      cursor: io.circe.HCursor,
      fieldName: String,
      requestId: String,
  ): Either[ExtensionCallError, Long] =
    cursor.downField(fieldName).focus.flatMap(_.asNumber).flatMap(_.toLong).toRight(
      malformedResponse(requestId, s"missing or malformed $fieldName")
    )

  private def malformedResponse(
      requestId: String,
      details: String,
  ): ExtensionCallError =
    ExtensionCallError(502, s"Malformed OAuth token response: $details", Some(requestId))
}
