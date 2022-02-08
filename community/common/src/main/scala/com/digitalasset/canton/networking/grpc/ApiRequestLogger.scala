// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.logging.pretty.Pretty.{
  DefaultEscapeUnicode,
  DefaultIndent,
  DefaultShowFieldNames,
  DefaultWidth,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.ShowUtil._
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Status.Code._
import io.grpc._
import pprint.{PPrinter, Tree}

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/** Server side interceptor that logs incoming and outgoing traffic.
  *
  * @param logMessagePayloads Indicates whether to log message payloads. (To be disabled in production!)
  *                           Also applies to metadata.
  * @param maxMethodLength indicates how much to abbreviate the name of the called method.
  *                        E.g. "com.digitalasset.canton.MyMethod" may get abbreviated to "c.d.c.MyMethod".
  *                        The last token will never get abbreviated.
  * @param maxMessageLines maximum number of lines to log for a message
  * @param maxStringLength maximum number of characters to log for a string within a message
  * @param maxMetadataSize maximum size of metadata
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class ApiRequestLogger(
    override protected val loggerFactory: NamedLoggerFactory,
    logMessagePayloads: Boolean,
    maxMethodLength: Int = 30,
    maxMessageLines: Int = 10,
    maxStringLength: Int = 20,
    maxMetadataSize: Int = 200,
) extends ServerInterceptor
    with NamedLogging {

  @VisibleForTesting
  private[networking] val cancelled: AtomicBoolean = new AtomicBoolean(false)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val requestTraceContext: TraceContext = inferRequestTraceContext

    val sender = Option(call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString)
      .getOrElse("unknown sender")
    val method = call.getMethodDescriptor.getFullMethodName

    def createLogMessage(message: String): String =
      show"Request ${method.readableLoggerName(maxMethodLength)} by ${sender.unquoted}: ${message.unquoted}"

    logger.trace(createLogMessage(s"received headers ${stringOfMetadata(headers)}"))(
      requestTraceContext
    )

    val loggingServerCall = new LoggingServerCall(call, createLogMessage, requestTraceContext)
    val serverCallListener = next.startCall(loggingServerCall, headers)
    new LoggingServerCallListener(serverCallListener, createLogMessage, requestTraceContext)
  }

  /** Intercepts events sent by the client.
    */
  class LoggingServerCallListener[ReqT, RespT](
      delegate: ServerCall.Listener[ReqT],
      createLogMessage: String => String,
      requestTraceContext: TraceContext,
  ) extends SimpleForwardingServerCallListener[ReqT](delegate) {

    /** Called when the server receives the request. */
    override def onMessage(message: ReqT): Unit = {
      val traceContext = traceContextOfMessage(message).getOrElse(requestTraceContext)
      logger.debug(
        createLogMessage(
          show"received a message ${cutMessage(message).unquoted}\n  Request ${requestTraceContext.showTraceId}"
        )
      )(traceContext)
      logThrowable(delegate.onMessage(message))(traceContext)
    }

    /** Called when the client completed all message sending (except for cancellation). */
    override def onHalfClose(): Unit = {
      logger.trace(createLogMessage(s"finished receiving messages"))(requestTraceContext)
      logThrowable(delegate.onHalfClose())(requestTraceContext)
    }

    /** Called when the client cancels the call. */
    override def onCancel(): Unit = {
      logger.info(createLogMessage("cancelled"))(requestTraceContext)
      logThrowable(delegate.onCancel())(requestTraceContext)
      cancelled.set(true)
    }

    /** Called when the server considers the call completed. */
    override def onComplete(): Unit = {
      logger.trace(createLogMessage("completed"))(requestTraceContext)
      logThrowable(delegate.onComplete())(requestTraceContext)
    }

    override def onReady(): Unit = {
      // This call is "just a suggestion" according to the docs and turns out to be quite flaky, even in simple scenarios.
      // Not logging therefore.
      logThrowable(delegate.onReady())(requestTraceContext)
    }

    private def logThrowable(within: => Unit)(traceContext: TraceContext): Unit = {
      try {
        within
      } catch {
        // If the server implementation fails, the server method must return a failed future or call StreamObserver.onError.
        // This handler is invoked, when an internal GRPC error occurs or the server implementation throws.
        case t: Throwable =>
          logger.error(createLogMessage("failed with an unexpected throwable"), t)(traceContext)
          t match {
            case _: RuntimeException =>
              throw t
            case _: Exception =>
              // Convert to a RuntimeException, because GRPC is unable to handle undeclared checked exceptions.
              throw new RuntimeException(t)
            case _: Throwable =>
              throw t
          }
      }
    }
  }

  /** Intercepts events sent by the server.
    */
  class LoggingServerCall[ReqT, RespT](
      delegate: ServerCall[ReqT, RespT],
      createLogMessage: String => String,
      requestTraceContext: TraceContext,
  ) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {

    /** Called when the server sends the response headers. */
    override def sendHeaders(headers: Metadata): Unit = {
      logger.trace(createLogMessage(s"sending response headers ${cutMessage(headers)}"))(
        requestTraceContext
      )
      delegate.sendHeaders(headers)
    }

    /** Called when the server sends a response. */
    override def sendMessage(message: RespT): Unit = {
      val traceContext = traceContextOfMessage(message).getOrElse(requestTraceContext)
      logger.debug(
        createLogMessage(
          s"sending response ${cutMessage(message)}\n  Request ${requestTraceContext.showTraceId}"
        )
      )(traceContext)
      delegate.sendMessage(message)
    }

    /** Called when the server closes the call. */
    override def close(status: Status, trailers: Metadata): Unit = {
      implicit val traceContext: TraceContext = requestTraceContext
      val enhancedStatus = enhance(status)

      val statusString = Option(enhancedStatus.getDescription).filterNot(_.isEmpty) match {
        case Some(d) => s"${enhancedStatus.getCode}/$d"
        case None => enhancedStatus.getCode.toString
      }

      val trailersString = stringOfTrailers(trailers)

      if (enhancedStatus.getCode == Status.OK.getCode) {
        logger.debug(
          createLogMessage(s"succeeded($statusString)$trailersString"),
          enhancedStatus.getCause,
        )
      } else {
        val message = createLogMessage(s"failed with $statusString$trailersString")
        if (enhancedStatus.getCode == UNKNOWN || enhancedStatus.getCode == DATA_LOSS) {
          logger.error(message, enhancedStatus.getCause)
        } else if (enhancedStatus.getCode == INTERNAL) {
          if (enhancedStatus.getDescription == "Half-closed without a request") {
            // If a call is cancelled, GRPC may half-close the call before the first message has been delivered.
            // The result is this status.
            // Logging with INFO to not confuse the user.
            // The status is still delivered to the client, to facilitate troubleshooting if there is a deeper problem.
            logger.info(message, enhancedStatus.getCause)
          } else {
            logger.error(message, enhancedStatus.getCause)
          }
        } else if (enhancedStatus.getCode == UNAUTHENTICATED) {
          logger.debug(message, enhancedStatus.getCause)
        } else {
          logger.info(message, enhancedStatus.getCause)
        }
      }
      delegate.close(enhancedStatus, trailers)
    }
  }

  private lazy val pprinter: PPrinter = PPrinter.BlackWhite.copy(
    defaultWidth = DefaultWidth,
    defaultHeight = maxMessageLines,
    defaultIndent = DefaultIndent,
    defaultEscapeUnicode = DefaultEscapeUnicode,
    defaultShowFieldNames = DefaultShowFieldNames,
    additionalHandlers = {
      case _: ByteString => Tree.Literal("ByteString")
      case s: String =>
        import com.digitalasset.canton.logging.pretty.Pretty._
        s.limit(maxStringLength).toTree
      case Some(p) =>
        pprinter.treeify(
          p,
          escapeUnicode = DefaultEscapeUnicode,
          showFieldNames = DefaultShowFieldNames,
        )
      case Seq(single) =>
        pprinter.treeify(
          single,
          escapeUnicode = DefaultEscapeUnicode,
          showFieldNames = DefaultShowFieldNames,
        )
    },
  )

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  private def cutMessage(message: Any): String =
    if (logMessagePayloads) {
      message match {
        case null => ""
        case product: Product =>
          pprinter(product).toString
        case _: Any =>
          import com.digitalasset.canton.logging.pretty.Pretty._
          message.toString.limit(maxStringLength).toString
      }
    } else {
      ""
    }

  private def stringOfTrailers(trailers: Metadata): String =
    if (!logMessagePayloads || trailers == null || trailers.keys().isEmpty) {
      ""
    } else {
      s"\n  Trailers: ${stringOfMetadata(trailers)}"
    }

  private def stringOfMetadata(metadata: Metadata): String =
    if (!logMessagePayloads || metadata == null) {
      ""
    } else {
      metadata.toString.limit(maxMetadataSize).toString
    }

  private def enhance(status: Status): Status = {
    if (status.getDescription == null && status.getCause != null) {
      // Copy the exception message to the status in order to transmit it to the client.
      // If you consider this a security risk:
      // - Exceptions are logged. Therefore, exception messages must not contain confidential data anyway.
      // - Note that scalapb.grpc.Grpc.completeObserver also copies exception messages into the status description.
      //   So removing this method would not mitigate the risk.
      status.withDescription(status.getCause.getLocalizedMessage)
    } else {
      status
    }
  }

  private def inferRequestTraceContext: TraceContext = {
    val grpcTraceContext = TraceContextGrpc.fromGrpcContext
    if (grpcTraceContext.traceId.isDefined) {
      grpcTraceContext
    } else {
      TraceContext.withNewTraceContext(identity)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def traceContextOfMessage[A](message: Any): Option[TraceContext] = {
    import scala.language.reflectiveCalls
    for {
      maybeTraceContextP <- Try(
        message
          .asInstanceOf[{ def traceContext: Option[com.digitalasset.canton.v0.TraceContext] }]
          .traceContext
      ).toOption
      tc <- ProtoConverter.required("traceContextOfMessage", maybeTraceContextP).toOption
      traceContext <- TraceContext.fromProtoV0(tc).toOption
    } yield traceContext
  }
}
