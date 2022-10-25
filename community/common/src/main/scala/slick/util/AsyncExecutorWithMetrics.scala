// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slick.util

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.QueryCostMonitoringConfig
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.DbQueueMetrics
import com.digitalasset.canton.util.{LoggerUtil, MonadUtil}
import com.typesafe.scalalogging.Logger
import slick.util.AsyncExecutor.{PrioritizedRunnable, Priority, WithConnection}

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{TimeUnit, *}
import javax.management.{InstanceNotFoundException, ObjectName}
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.*
import scala.util.control.NonFatal

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.IsInstanceOf",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.StringPlusAny",
    "org.wartremover.warts.Var",
  )
)
class AsyncExecutorWithMetrics(
    name: String,
    minThreads: Int,
    maxThreads: Int,
    queueSize: Int,
    maxConnections: Int = Integer.MAX_VALUE,
    keepAliveTime: FiniteDuration = 1.minute,
    registerMbeans: Boolean = false,
    logQueryCost: Option[QueryCostMonitoringConfig],
    metrics: DbQueueMetrics,
    val logger: Logger,
) extends AsyncExecutor {

  @volatile private[this] lazy val mbeanName = new ObjectName(
    s"slick:type=AsyncExecutor,name=$name"
  );

  // Before init: 0, during init: 1, after init: 2, during/after shutdown: 3
  private[this] val state = new AtomicInteger(0)

  @volatile private[this] var executor: ThreadPoolExecutor = _

  if (maxConnections > maxThreads) {
    // NOTE: when using transactions or DB locks, it may happen that a task has a lock on the database but no thread
    // to complete its action, while other tasks may have all the threads but are waiting for the first task to
    // complete. This creates a deadlock.
    logger.warn(
      "Having maxConnection > maxThreads can result in deadlocks if transactions or database locks are used."
    )
  }

  lazy val executionContext = {
    if (!state.compareAndSet(0, 1))
      throw new IllegalStateException(
        "Cannot initialize ExecutionContext; AsyncExecutor already shut down"
      )
    val queue: BlockingQueue[Runnable] = queueSize match {
      case 0 =>
        // NOTE: SynchronousQueue does not schedule high-priority tasks before others and so it cannot be used when
        // the number of connections is limited (lest high-priority tasks may be holding all connections and low/mid
        // priority tasks all threads -- resulting in a deadlock).
        require(
          maxConnections == Integer.MAX_VALUE,
          "When using queueSize == 0 (direct hand-off), maxConnections must be Integer.MAX_VALUE.",
        )

        new SynchronousQueue[Runnable]
      case -1 =>
        // NOTE: LinkedBlockingQueue does not schedule high-priority tasks before others and so it cannot be used when
        // the number of connections is limited (lest high-priority tasks may be holding all connections and low/mid
        // priority tasks all threads -- resulting in a deadlock).
        require(
          maxConnections == Integer.MAX_VALUE,
          "When using queueSize == -1 (unlimited), maxConnections must be Integer.MAX_VALUE.",
        )

        new LinkedBlockingQueue[Runnable]
      case n =>
        // NOTE: The current implementation of ManagedArrayBlockingQueue is flawed. It makes the assumption that all
        // tasks go through the queue (which is responsible for scheduling high-priority tasks first). However, that
        // assumption is wrong since the ThreadPoolExecutor bypasses the queue when it creates new threads. This
        // happens whenever it creates a new thread to run a task, i.e. when minThreads < maxThreads and the number
        // of existing threads is < maxThreads.
        //
        // The only way to prevent problems is to have minThreads == maxThreads when using the
        // ManagedArrayBlockingQueue.
        require(
          minThreads == maxThreads,
          "When using queueSize > 0, minThreads == maxThreads is required.",
        )

        // NOTE: The current implementation of ManagedArrayBlockingQueue.increaseInUseCount implicitly `require`s that
        // maxThreads <= maxConnections.
        require(
          maxThreads <= maxConnections,
          "When using queueSize > 0, maxThreads <= maxConnections is required.",
        )

        // NOTE: Adding up the above rules
        // - maxThreads >= maxConnections, to prevent database locking issues when using transactions
        // - maxThreads <= maxConnections, required by ManagedArrayBlockingQueue
        // - maxThreads == minThreads, ManagedArrayBlockingQueue
        //
        // We have maxThreads == minThreads == maxConnections as the only working configuration

        new ManagedArrayBlockingQueue(maxConnections, n).asInstanceOf[BlockingQueue[Runnable]]
    }

    // canton change begin
    object QueryCostTracker {

      /** count / total time */
      private val cost = new AtomicReference[Map[String, (Long, Long)]](Map())
      private val lastReport = new AtomicReference(CantonTimestamp.now())
      def track(trace: String, runningTime: Long): Unit = {
        if (logger.underlying.isInfoEnabled) {
          logQueryCost.foreach { case QueryCostMonitoringConfig(frequency, resetOnOutput) =>
            val updated = cost.updateAndGet { tmp =>
              val (count, total): (Long, Long) = tmp.getOrElse(trace, (0, 0))
              tmp + (trace -> ((count + 1, total + runningTime)))
            }
            val now = CantonTimestamp.now()
            val upd = lastReport.updateAndGet(rp =>
              if (rp.plusMillis(frequency.unwrap.toMillis) < now) {
                // Reset cost tracking
                if (resetOnOutput) cost.set(Map())
                now
              } else rp
            )
            if (upd == now) {
              val items = updated.toSeq
                .sortBy(x => -x._2._2)
                .take(15)
                .map { case (name, (count, nanos)) =>
                  f"count=$count%7d mean=${nanos / (Math.max(count, 1) * 1e6)}%7.2f ms total=${nanos / 1e9}%5.1f s $name%s"
                }
                .mkString("\n  ")
              logger.info(
                s"Here is our list of the 15 most expensive database queries for ${metrics.prefix}:\n  " + items
              )
            }
          }
        }
      }
    }

    case class QueryInfo(callsite: String, added: Long, scheduled: Option[Long]) {

      def created(): QueryInfo = {
        metrics.queue.inc()
        this
      }

      def updateScheduled(): QueryInfo = {
        metrics.queue.dec()
        metrics.running.inc()
        val tm = System.nanoTime()
        metrics.waitTimer.update(tm - added, TimeUnit.NANOSECONDS)
        QueryInfo(callsite, added, Some(tm))
      }

      def completed(): Unit = {
        val tm = System.nanoTime()
        scheduled match {
          case Some(st) =>
            metrics.running.dec()
            QueryCostTracker.track(callsite, tm - st)
          case None =>
            QueryCostTracker.track(s"$callsite - missing start time", tm - added)
        }
      }
    }
    // canton change end

    val stats = TrieMap[Runnable, QueryInfo]()
    val tf = new DaemonThreadFactory(name + "-")
    executor = new ThreadPoolExecutor(
      minThreads,
      maxThreads,
      keepAliveTime.toMillis,
      TimeUnit.MILLISECONDS,
      queue,
      tf,
    ) {

      /** If the runnable/task is a low/medium priority item, we increase the items in use count, because first thing it will do
        * is open a Jdbc connection from the pool.
        */
      override def beforeExecute(t: Thread, r: Runnable): Unit = {
        (r, queue) match {
          case (pr: PrioritizedRunnable, q: ManagedArrayBlockingQueue[Runnable])
              if pr.priority != WithConnection =>
            q.increaseInUseCount(pr)
          case _ =>
        }
        // canton change begin
        // update stats
        stats.get(r) match {
          case Some(cur) =>
            stats.update(r, cur.updateScheduled())
          case _ =>
        }
        // canton change end
        super.beforeExecute(t, r)
      }

      // canton change begin
      private val ignore =
        Seq(
          "slick",
          "java.",
          "scala.",
          "cats.",
          "com.daml.metrics",
          "com.digitalasset.canton.resource",
          "com.digitalasset.canton.resource.DbStorageMulti",
          "com.digitalasset.canton.util.retry",
          "com.digitalasset.canton.metrics",
          LoggerUtil.getClass.getName.dropRight(1), // Drop Scala's trailing $
          MonadUtil.getClass.getName.dropRight(1), // Drop Scala's trailing $
        )
      override def execute(command: Runnable): Unit = {
        val tr = if (logQueryCost.nonEmpty) {
          // find call site
          Thread
            .currentThread()
            .getStackTrace
            .find { e =>
              ignore.forall(pack => !e.getClassName.startsWith(pack))
            }
            .map(_.toString)
            .getOrElse("<unknown>")
        } else "query-tracking-disabled"
        // initialize statistics gathering
        stats.put(command, QueryInfo(tr, added = System.nanoTime(), None).created()).discard
        try {
          super.execute(command)
        } catch {
          // if we throw here, the task will never be executed. therefore, we'll have to remove the task statistics
          // again to not leak memory
          case NonFatal(e) =>
            stats.remove(command).discard
            throw e
        }
      }
      // canton change end

      /** If the runnable/task has released the Jdbc connection we decrease the counter again
        */
      override def afterExecute(r: Runnable, t: Throwable): Unit = {
        try {
          super.afterExecute(r, t)
          (r, queue) match {
            case (pr: PrioritizedRunnable, q: ManagedArrayBlockingQueue[Runnable])
                if pr.connectionReleased =>
              q.decreaseInUseCount()
            case _ =>
          }
          // canton change begin
        } finally {
          stats.remove(r).foreach(_.completed())
          // canton change end
        }
      }

    }
    if (registerMbeans) {
      try {
        val mbeanServer = ManagementFactory.getPlatformMBeanServer
        if (mbeanServer.isRegistered(mbeanName))
          logger.warn(s"MBean $mbeanName already registered (AsyncExecutor names should be unique)")
        else {
          logger.debug(s"Registering MBean $mbeanName")
          mbeanServer.registerMBean(
            new AsyncExecutorMXBean {
              def getMaxQueueSize = queueSize
              def getQueueSize = queue.size()
              def getMaxThreads = maxThreads
              def getActiveThreads = executor.getActiveCount
            },
            mbeanName,
          )
        }
      } catch { case NonFatal(ex) => logger.error("Error registering MBean", ex) }
    }
    if (!state.compareAndSet(1, 2)) {
      unregisterMbeans()
      executor.shutdownNow()
      throw new IllegalStateException(
        "Cannot initialize ExecutionContext; AsyncExecutor shut down during initialization"
      )
    }
    new ExecutionContextExecutor {

      override def reportFailure(t: Throwable): Unit =
        logger.error("Async executor failed with exception", t)

      override def execute(command: Runnable): Unit = {
        if (command.isInstanceOf[PrioritizedRunnable]) {
          executor.execute(command)
        } else {
          executor.execute(new PrioritizedRunnable {
            override val priority: Priority = WithConnection
            override def run(): Unit = command.run()
          })
        }
      }
    }
  }

  private[this] def unregisterMbeans(): Unit = if (registerMbeans) {
    try {
      val mbeanServer = ManagementFactory.getPlatformMBeanServer
      logger.debug(s"Unregistering MBean $mbeanName")
      try mbeanServer.unregisterMBean(mbeanName)
      catch { case _: InstanceNotFoundException => }
    } catch { case NonFatal(ex) => logger.error("Error unregistering MBean", ex) }
  }

  def close(): Unit = if (state.getAndSet(3) == 2) {
    unregisterMbeans()
    executor.shutdownNow()
    if (!executor.awaitTermination(30, TimeUnit.SECONDS))
      logger.warn("Abandoning ThreadPoolExecutor (not yet destroyed after 30 seconds)")
  }

  private class DaemonThreadFactory(namePrefix: String) extends ThreadFactory {
    private[this] val group =
      Option(System.getSecurityManager).fold(Thread.currentThread.getThreadGroup)(_.getThreadGroup)
    private[this] val threadNumber = new AtomicInteger(1)

    def newThread(r: Runnable): Thread = {
      val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
      if (!t.isDaemon) t.setDaemon(true)
      if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
      t
    }
  }

}
