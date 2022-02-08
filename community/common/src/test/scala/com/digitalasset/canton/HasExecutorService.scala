// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  ExecutionContextMonitor,
  ExecutorServiceExtensions,
  Threading,
}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

/** Mixin that provides an executor for tests.
  * The executor supports blocking operations, provided they are wrapped in [[scala.concurrent.blocking]]
  * or [[scala.concurrent.Await]].
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
trait HasExecutorService extends BeforeAndAfterAll { this: Suite with NamedLogging =>

  private case class ExecutorState(
      scheduler: ScheduledExecutorService,
      executor: ExecutionContextIdlenessExecutorService,
      monitor: ExecutionContextMonitor,
  ) extends AutoCloseable {
    override def close(): Unit = {
      monitor.close()
      ExecutorServiceExtensions(scheduler)(logger, DefaultProcessingTimeouts.testing).close()
      ExecutorServiceExtensions(executor)(logger, DefaultProcessingTimeouts.testing).close()
    }
  }

  private def createScheduler(): ScheduledExecutorService = {
    Threading.singleThreadScheduledExecutor(
      loggerFactory.threadName + "-test-execution-context-monitor",
      logger,
    )
  }

  private def createExecutor(
      scheduler: ScheduledExecutorService
  ): (ExecutionContextIdlenessExecutorService, ExecutionContextMonitor) = {
    val service = Threading.newExecutionContext(
      executionContextName,
      logger,
      Threading.detectNumberOfThreads(logger)(TraceContext.empty),
      exitOnFatal = exitOnFatal,
    )
    val monitor =
      new ExecutionContextMonitor(
        loggerFactory,
        NonNegativeFiniteDuration.ofSeconds(3),
        10,
        reportAsWarnings = true,
        DefaultProcessingTimeouts.testing,
      )(scheduler)
    monitor.monitor(service)
    (service, monitor)
  }

  private def createExecutorState(): ExecutorState = {
    // Monitor the execution context to get useful information on deadlocks.
    val scheduler = createScheduler()
    val (executor, monitor) = createExecutor(scheduler)
    ExecutorState(scheduler, executor, monitor)
  }

  private def getOrCreateExecutor(): ExecutionContextIdlenessExecutorService =
    executorStateRef
      .updateAndGet(_.orElse(Some(createExecutorState())))
      .map(_.executor)
      .getOrElse(fail("Executor was not created"))

  private lazy val executorStateRef: AtomicReference[Option[ExecutorState]] =
    new AtomicReference[Option[ExecutorState]](None)

  protected def executionContextName: String = loggerFactory.threadName + "-test-execution-context"

  protected def exitOnFatal: Boolean = true

  protected def executorService: ExecutionContextIdlenessExecutorService = getOrCreateExecutor()

  override def afterAll(): Unit = {
    try super.afterAll()
    finally {
      val executorStateClose: AutoCloseable = () => {
        executorStateRef.updateAndGet { executorStateO =>
          executorStateO.foreach(_.close())
          None
        }
      }

      Lifecycle.close(executorStateClose)(logger)
    }
  }
}
