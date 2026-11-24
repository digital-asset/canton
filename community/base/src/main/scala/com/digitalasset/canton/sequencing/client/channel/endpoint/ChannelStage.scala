// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.Encrypted
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.ConnectToSequencerChannelResponse.Response
import com.digitalasset.canton.sequencing.channel.ConnectToSequencerChannelRequest
import com.digitalasset.canton.sequencing.channel.errors.SequencerChannelErrors
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.sequencing.client.channel.endpoint.ChannelStage.{
  InternalData,
  StageTransition,
}
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelSecurity.SessionKeyRejection
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelSessionKey
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ByteStringUtil, MaxBytesToDecompress}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Represents a sequencer channel client endpoint stage.
  *
  * @param data
  *   Parameter object containing configuration and APIs.
  * @param name
  *   The stage name.
  */
private[endpoint] sealed abstract class ChannelStage(
    data: InternalData,
    val name: String,
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  override protected def loggerFactory: NamedLoggerFactory = data.loggerFactory

  protected def wrongMessageError(
      response: v30.ConnectToSequencerChannelResponse.Response
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = EitherT.leftT(
    s"In stage $name, cannot process message ${ChannelStage.getName(response)}"
  )

  /** Sequencer channel client endpoint stage dependent response message handling.
    */
  def handleMessage(response: v30.ConnectToSequencerChannelResponse.Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition]

  def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

private[endpoint] object ChannelStage {
  type StageTransition = (List[(String, ConnectToSequencerChannelRequest)], ChannelStage)

  def getName(response: v30.ConnectToSequencerChannelResponse.Response): String =
    response match {
      case Response.Empty => "Empty"
      case _: Response.Connected => "Connected"
      case _: Response.SessionKey => "SessionKey"
      case _: Response.SessionKeyAcknowledgement => "SessionKeyAcknowledgement"
      case _: Response.Payload => "Payload"
    }

  // Data that need to be known by all the stages
  final case class InternalData(
      security: SequencerChannelSecurity,
      protocolVersion: ProtocolVersion,
      processor: SequencerChannelProtocolProcessor,
      maxBytesToDecompress: MaxBytesToDecompress,
      loggerFactory: NamedLoggerFactory,
  )
}

private[endpoint] class ChannelStageBootstrap(
    sessionKeyOwner: Boolean,
    data: InternalData,
)(implicit executionContext: ExecutionContext)
    extends ChannelStage(data, "Bootstrap")(
      executionContext: ExecutionContext
    ) {

  override def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.rightT(())

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case _: Response.Connected => handleConnected()
    case other => wrongMessageError(other)
  }

  private def handleConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] =
    if (sessionKeyOwner) {
      for {
        sessionKey <- data.security.generateSessionKey()
        m = ConnectToSequencerChannelRequest.sessionKey(sessionKey, data.protocolVersion)
      } yield (List(("session key", m)), new ChannelStageAwaitingSessionKeyAck(data))
    } else {
      EitherT.rightT[FutureUnlessShutdown, String]((Nil, new ChannelStageAwaitingSessionKey(data)))
    }
}

/** Stage of the endpoint that does not own the session key. It accepts only the session key, so
  * that it cannot become securely connected without one.
  */
private[endpoint] class ChannelStageAwaitingSessionKey(data: InternalData)(implicit
    executionContext: ExecutionContext
) extends ChannelStage(data, "AwaitingSessionKey")(
      executionContext
    ) {
  private def newStage = new ChannelStageSecurelyConnected(data)

  override def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.rightT(())

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case sessionKey: Response.SessionKey => handleSessionKey(sessionKey)
    case other => wrongMessageError(other)
  }

  private def handleSessionKey(message: Response.SessionKey)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] =
    (for {
      sessionKey <- EitherT.fromEither[FutureUnlessShutdown](
        SequencerChannelSessionKey
          .fromProtoV30(message.value)
          .leftMap(error => SessionKeyRejection.Incompatible(error.message): SessionKeyRejection)
      )
      _ <- data.security.registerSessionKey(sessionKey)
    } yield {
      val messages = List(
        ("session key ack", ConnectToSequencerChannelRequest.sessionKeyAck(data.protocolVersion))
      )
      (messages, newStage): StageTransition
    }).leftMap { rejection =>
      report(rejection)
      s"Rejected session key: ${rejection.reason}"
    }

  /** Raises the alarm only for tampering, and warns only if an operator can act. */
  private def report(rejection: SessionKeyRejection)(implicit traceContext: TraceContext): Unit =
    rejection match {
      case SessionKeyRejection.Tampered(reason) =>
        SequencerChannelErrors.SessionKeyRejected.Warn(reason).report()
      case SessionKeyRejection.Incompatible(reason) =>
        SequencerChannelErrors.SessionKeyNotAccepted.Warn(reason).discard
      case SessionKeyRejection.MaybeTransient(reason) =>
        // TODO(#38123) Closing the endpoint after this rejection still logs a WARN.
        logger.info(s"Rejected session key: $reason")
    }
}

/** Stage of the endpoint that owns the session key. It accepts only the acknowledgement, so that it
  * never takes a session key from the other member.
  */
private[endpoint] class ChannelStageAwaitingSessionKeyAck(data: InternalData)(implicit
    executionContext: ExecutionContext
) extends ChannelStage(data, "AwaitingSessionKeyAck")(
      executionContext
    ) {
  private def newStage = new ChannelStageSecurelyConnected(data)

  override def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.rightT(())

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case _: Response.SessionKeyAcknowledgement =>
      EitherT.rightT[FutureUnlessShutdown, String]((Nil, newStage))
    case other => wrongMessageError(other)
  }
}

private[endpoint] class ChannelStageSecurelyConnected(data: InternalData)(implicit
    executionContext: ExecutionContext
) extends ChannelStage(data, "SecurelyConnected")(
      executionContext
    ) {
  private def newStage = this

  override def initialization()(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    Unit,
  ] = processOnChannelReadyForProcessor()(traceContext)

  /** Notify the processor that the channel is ready for use, in particular for sending payloads.
    */
  private def processOnChannelReadyForProcessor()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    data.processor.onConnected()

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case payload: Response.Payload => handlePayload(payload)
    case other => wrongMessageError(other)
  }

  private def handlePayload(payload: Response.Payload)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = {
    val encryptedPayload = Encrypted.fromByteString(payload.value)
    for {
      decrypted <- decrypt(encryptedPayload)(Right(_))
      decompressed <- EitherT.fromEither[FutureUnlessShutdown](
        ByteStringUtil
          .decompressZstd(decrypted, data.maxBytesToDecompress)
          .leftMap(err => s"Failed to decompress payload: ${err.message}")
      )
      _ <- data.processor.handlePayload(decompressed)(traceContext)
    } yield (Nil, newStage)
  }

  private def decrypt[M](encryptedMessage: Encrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[FutureUnlessShutdown, String, M] =
    data.security.decrypt(encryptedMessage)(deserialize).mapK(FutureUnlessShutdown.outcomeK)

}
