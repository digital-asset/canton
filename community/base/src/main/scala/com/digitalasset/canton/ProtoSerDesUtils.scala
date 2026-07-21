// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.ProtoSerializationError.ProtoSerializationFailure
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.concurrent.{ExecutionContext, Future}

object ProtoSerDesUtils {

  /** Transform a left to a failed future */
  def toFuture[T](
      res: ParsingResult[T]
  )(implicit errorLoggingContext: ErrorLoggingContext, ec: ExecutionContext): Future[T] =
    res.fold(
      err =>
        Future.failed(
          ProtoDeserializationFailure.WrapNoLogging(err).toCantonRpcError.asGrpcError
        ),
      res => Future(res),
    )

  /** Transform a left to a failed future unless shutdown */
  def toFutureUnlessShutdown[T](
      res: ParsingResult[T]
  )(implicit errorLoggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] =
    res.fold(
      err =>
        FutureUnlessShutdown.failed(
          ProtoDeserializationFailure.WrapNoLogging(err).toCantonRpcError.asGrpcError
        ),
      res => FutureUnlessShutdown.pure(res),
    )

  /** Transform a serialization error to a failed future */
  def serializationErrorToFuture[T](
      res: Either[String, T]
  )(implicit errorLoggingContext: ErrorLoggingContext, ec: ExecutionContext): Future[T] =
    res.fold(
      err =>
        Future.failed(
          ProtoSerializationFailure.Wrap(err).toCantonRpcError.asGrpcError
        ),
      res => Future(res),
    )

  /** Transform a serialization error to a failed future unless shutdown */
  def serializationErrorToFutureUnlessShutdown[T](
      res: Either[String, T]
  )(implicit errorLoggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] =
    res.fold(
      err =>
        FutureUnlessShutdown.failed(
          ProtoSerializationFailure.Wrap(err).toCantonRpcError.asGrpcError
        ),
      res => FutureUnlessShutdown.pure(res),
    )
}
