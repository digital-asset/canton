// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.http

import cats.syntax.either.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

import java.net.{MalformedURLException, URL}

/** Urls for talking to a HTTP backend.
  * Allows specifying different locations for read and write requests which is required by the Confidential Consortium
  * Framework application where end to end TLS prevents a level 7 balancer being able to see the path and appropriately
  * directing the request.
  */
case class HttpSequencerEndpoints(read: URL, write: URL) extends PrettyPrinting {
  override def pretty: Pretty[HttpSequencerEndpoints] = prettyOfClass(
    param("read", _.read.toURI),
    param("write", _.write.toURI),
  )
}

object HttpSequencerEndpoints {

  val endpointVersion = "v0"

  private def parseUrl(url: String): Either[String, URL] =
    Either
      .catchOnly[MalformedURLException](new URL(url))
      .leftMap(err => s"Failed to parse given URL $url: $err")

  def create(writeUrl: String, readUrl: String): Either[String, HttpSequencerEndpoints] =
    for {
      parsedWriteUrl <- parseUrl(writeUrl)
      parsedReadUrl <- parseUrl(readUrl)
    } yield HttpSequencerEndpoints(parsedWriteUrl, parsedReadUrl)

  /** Constructor for where there is no separation between read and writes */
  def create(url: String): Either[String, HttpSequencerEndpoints] = create(url, url)
}
