// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  HttpSequencerConnection,
  SequencerConnection,
}
import com.digitalasset.canton.sequencing.client.http.HttpSequencerEndpoints
import com.google.protobuf.ByteString

/** Definition provided by the domain node to members with details on how to connect to the domain sequencer. * */
sealed trait SequencerConnectionConfig {
  def toConnection: Either[String, SequencerConnection]
}

object SequencerConnectionConfig {

  // TODO(i3804) consolidate with TlsClientCertificate
  sealed trait CertificateConfig extends Product with Serializable {
    def pem: X509CertificatePem
  }

  /** Throws an exception if the file does not exist or cannot be loaded. */
  case class CertificateFile(pemFile: ExistingFile) extends CertificateConfig {
    override val pem: X509CertificatePem = X509CertificatePem.tryFromFile(pemFile.unwrap.toScala)
  }

  /** Throws an exception if the string containing the PEM certificate cannot be loaded. */
  case class CertificateString(pemString: String) extends CertificateConfig {
    override val pem: X509CertificatePem = X509CertificatePem.tryFromString(pemString)
  }

  object CertificateConfig {
    def apply(bytes: ByteString): CertificateConfig =
      CertificateString(bytes.toStringUtf8)
  }

  /** Connection specification for a HTTP Sequencer
    * @param address Host for writes (and reads if read host is not specified).
    * @param port Port for writes (and reads if read port is not specified).
    * @param certificate server certificate of the sequencer in PEM format as either a file or a string.
    * @param readHost Optional host override for directing reads
    * @param readPort Optional port override for directing reads
    */
  case class Http(
      address: String,
      port: Port,
      certificateConfig: CertificateConfig,
      readHost: Option[String] = None,
      readPort: Option[Port] = None,
  ) extends SequencerConnectionConfig {

    // https can be safely assumed
    private def url(host: String, port: Port) = s"https://$host:$port"

    def toConnection: Either[String, HttpSequencerConnection] =
      for {
        urls <- HttpSequencerEndpoints.create(
          writeUrl = url(address, port),
          readUrl = url(readHost.getOrElse(address), readPort.getOrElse(port)),
        )
      } yield HttpSequencerConnection(urls, certificateConfig.pem)
  }

  /** Grpc connection using a real grpc channel.
    */
  case class Grpc(
      address: String,
      port: Port,
      transportSecurity: Boolean = false,
      customTrustCertificates: Option[CertificateFile] = None,
  ) extends SequencerConnectionConfig {

    def toConnection: Either[String, GrpcSequencerConnection] =
      for {
        pem <- customTrustCertificates.traverse(file =>
          X509CertificatePem.fromFile(file.pemFile.unwrap.toScala)
        )
      } yield GrpcSequencerConnection(
        NonEmpty(Seq, Endpoint(address, port)),
        transportSecurity,
        pem.map(_.unwrap),
      )
  }
}
