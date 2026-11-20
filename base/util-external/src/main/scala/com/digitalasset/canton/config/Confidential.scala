// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory}
import pureconfig.{ConfigReader, ConfigWriter}

/** Wrapper for confidential config values (secrets, passwords, tokens, ...).
  *
  * The value is masked in `toString`, and when the config is written with a
  * [[ConfidentialConfigWriter]] in confidential mode.
  */
final case class Confidential[+A](unwrap: A) {
  override def toString: String = Confidential.Mask
}

object Confidential {

  /** The value that confidential values are replaced with. */
  val Mask: String = "****"

  /* Reading confidential values just applies the wrapper.
   * There is intentionally no [[ConfigWriter]] here, we force users to construct
   * a [[ConfidentialConfigWriter]] for that. */
  implicit def confidentialValueReader[A](implicit
      reader: ConfigReader[A]
  ): ConfigReader[Confidential[A]] = reader.map(Confidential(_))

  object Implicits {
    import scala.language.implicitConversions

    /** Implicit that allows passing plain value where a `Confidential` one is expected. */
    implicit def toConfidential[A](value: A): Confidential[A] = Confidential(value)
  }

  /** Helper type to support typical HTTP request header configurations, where headers that contain
    * tokens are masked.
    */
  final case class HttpRequestHeaders(
      exposed: Map[String, String],
      confidential: Map[String, Confidential[String]],
  ) {
    def all: Map[String, String] = exposed ++ confidential.view.mapValues(_.unwrap)
  }

  object HttpRequestHeaders {
    val empty: HttpRequestHeaders = HttpRequestHeaders(Map.empty, Map.empty)

    def fromMap(headers: Map[String, String]): HttpRequestHeaders = {
      val (confidential, exposed) =
        headers.partition { case (name, _) => isConfidential(name.toLowerCase) }
      HttpRequestHeaders(
        exposed = exposed,
        confidential = confidential.view.mapValues(Confidential(_)).toMap,
      )
    }

    private def isConfidential(key: String): Boolean = {
      val patterns =
        List("auth", "token", "key", "secret", "password", "credential", "cookie", "session")
      patterns.exists(key.contains(_))
    }

    // Reads a flat `request-headers { name = value }` object.
    implicit val httpRequestHeadersReader: ConfigReader[HttpRequestHeaders] =
      ConfigReader[Map[String, String]].map(fromMap)
  }
}

/** Provides writers that mask confidential values if `confidential` is set. */
class ConfidentialConfigWriter(confidential: Boolean) {
  implicit def confidentialValueWriter[A](implicit
      writer: ConfigWriter[A]
  ): ConfigWriter[Confidential[A]] =
    if (confidential)
      ConfigWriter.fromFunction(_ => ConfigValueFactory.fromAnyRef(Confidential.Mask))
    else
      writer.contramap(_.unwrap)

  /** Writer for raw [[com.typesafe.config.Config]] blobs, which can't wrap their confidential
    * values in [[Confidential]]. The motivating example for this is [[DbConfig]]. These types need
    * to supply a custom `mask` function.
    */
  def rawConfigWriter(mask: Config => Config): ConfigWriter[Config] =
    ConfigWriter.fromFunction(config => (if (confidential) mask(config) else config).root())

  /** Writer for confidential request headers. */
  implicit def httpRequestHeadersWriter: ConfigWriter[Confidential.HttpRequestHeaders] =
    ConfigWriter[Map[String, ConfigValue]].contramap { headers =>
      headers.exposed.view.mapValues(ConfigValueFactory.fromAnyRef(_)).toMap ++
        headers.confidential.view.mapValues(confidentialValueWriter[String].to).toMap
    }
}
