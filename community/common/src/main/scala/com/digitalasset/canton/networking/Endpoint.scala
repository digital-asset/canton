// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking

import cats.syntax.either._
import cats.syntax.reducible._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances._
import com.digitalasset.canton.config.RequireTypes.Port

import java.net.URI

/** Networking endpoint where host could be a hostname or ip address. */
case class Endpoint(host: String, port: Port) {
  override def toString: String = s"$host:$port"

  def toURI(useTls: Boolean) = new URI(s"${if (useTls) "https" else "http"}://$toString")
}

object Endpoint {
  private val defaultHttpPort = 80
  private val defaultHttpsPort = 443
  private def defaultPort(useTls: Boolean): Int = if (useTls) defaultHttpsPort else defaultHttpPort

  /** Extracts from a list of URIs the endpoint configuration (host and port), as well as a flag indicating
    *  whether they all use TLS or all don't. Will return an error if endpoints are not consistent in their usage
    * of TLS.
    */
  def fromUris(
      connections: NonEmpty[Seq[URI]]
  ): Either[String, (NonEmpty[Seq[Endpoint]], Boolean)] =
    for {
      endpointsWithTlsFlag <- connections.toNEF.traverse(fromUri)
      (endpoints, tlsFlags) = (endpointsWithTlsFlag.map(_._1), endpointsWithTlsFlag.map(_._2))
      // check that they all are using TLS, or all aren't
      useTls <- tlsFlags.toNEF.reduceLeftM(Right(_): Either[String, Boolean])((a, b) =>
        Either.cond[String, Boolean](
          a == b,
          b,
          s"All domain connections must either use TLS or all not use TLS",
        )
      )
    } yield (endpoints, useTls)

  private def fromUri(uri: URI): Either[String, (Endpoint, Boolean)] = {
    val (scheme, host, portO) = (
      // default to https if the connection scheme is not supplied
      Option(uri.getScheme).getOrElse("https"),
      // `staging.canton.global` is assumed to be a path rather than a host
      Option(uri.getHost).getOrElse(uri.getPath),
      // java.net.URI will return -1 if no port is set in the URI string
      Option(uri.getPort).filter(_ >= 0),
    )
    for {
      useTls <- scheme match {
        case "https" => Right(true)
        case "http" => Right(false)
        case unknownScheme =>
          Left(s"Domain connection url [$uri] has unknown scheme: $unknownScheme")
      }
      port <- Port
        .create(portO.getOrElse(defaultPort(useTls)))
        .leftMap(err => s"Domain connection url [$uri] has an invalid port: $err")
    } yield (Endpoint(host, port), useTls)
  }

}
