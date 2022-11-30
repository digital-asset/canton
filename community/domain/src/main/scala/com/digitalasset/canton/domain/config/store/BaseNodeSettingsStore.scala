// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Base trait for individual node configuration stores
  *
  * Used by DomainManager, Sequencer and Mediator nodes.
  */
trait BaseNodeSettingsStore[T] extends AutoCloseable {
  this: NamedLogging =>
  // TODO(#11052) extend this to ParticipantSettings and move node_id table into this class
  //          also, replace the fetchConfiguration and saveConfiguration with initConfiguration
  //          and update configuration. We might also want to cache the configuration directly in memory
  //          (as we do it for participant settings).
  //          also, the update configuration should be atomic. right now, we do fetch / save, which is racy
  //          also, update this to mediator and sequencer. right now, we only do this for domain manager and domain nodes
  def fetchSettings(implicit
      traceContext: TraceContext
  ): EitherT[Future, BaseNodeSettingsStoreError, Option[T]]

  def saveSettings(settings: T)(implicit
      traceContext: TraceContext
  ): EitherT[Future, BaseNodeSettingsStoreError, Unit]

  // TODO(#9014) remove once we can assume that static domain parameters are persiste
  protected def fixPreviousSettings(resetToConfig: Boolean, timeout: NonNegativeDuration)(
      update: Option[T] => EitherT[Future, BaseNodeSettingsStoreError, Unit]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Unit = {

    val eitherT = fetchSettings.flatMap {
      // if a value is stored, don't do anything
      case Some(_) if !resetToConfig =>
        EitherT.rightT[Future, BaseNodeSettingsStoreError](())
      case Some(value) =>
        noTracingLogger.warn(
          "Resetting static domain parameters to the ones defined in the config! Please disable this again."
        )
        update(Some(value))
      case None => update(None)
    }
    // wait until setting of static domain parameters completed
    timeout
      .await("Setting static domain parameters")(eitherT.value)
      .valueOr { err =>
        logger.error(s"Failed to updating static domain parameters during initialization: $err")
        throw new RuntimeException(err.toString)
      }
  }

}

object BaseNodeSettingsStore {
  def factory[T](
      storage: Storage,
      dbFactory: DbStorage => BaseNodeSettingsStore[T],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): BaseNodeSettingsStore[T] =
    storage match {
      case _: MemoryStorage => new InMemoryBaseNodeConfigStore[T](loggerFactory)
      case storage: DbStorage => dbFactory(storage)
    }

}

sealed trait BaseNodeSettingsStoreError
object BaseNodeSettingsStoreError {
  case class DbError(exception: Throwable) extends BaseNodeSettingsStoreError
  case class DeserializationError(deserializationError: ProtoDeserializationError)
      extends BaseNodeSettingsStoreError
}

class InMemoryBaseNodeConfigStore[T](val loggerFactory: NamedLoggerFactory)(implicit
    executionContext: ExecutionContext
) extends BaseNodeSettingsStore[T]
    with NamedLogging {

  private val currentSettings = new AtomicReference[Option[T]](None)

  override def fetchSettings(implicit
      traceContext: TraceContext
  ): EitherT[Future, BaseNodeSettingsStoreError, Option[T]] =
    EitherT.pure(currentSettings.get())

  override def saveSettings(settings: T)(implicit
      traceContext: TraceContext
  ): EitherT[Future, BaseNodeSettingsStoreError, Unit] = {
    currentSettings.set(Some(settings))
    EitherT.pure(())
  }

  override def close(): Unit = ()

}
