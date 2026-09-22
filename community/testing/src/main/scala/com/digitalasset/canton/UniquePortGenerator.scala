// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.util.Mutex
import com.google.common.annotations.VisibleForTesting
import org.slf4j.{Logger, LoggerFactory}

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.channels.{FileLock, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import java.time.Duration
import scala.annotation.tailrec
import scala.concurrent.blocking
import scala.util.*

/** Generates host-wide unique network ports for Canton tests, guaranteeing low probability of port
  * collisions.
  *
  * Synchronization across multiple processes is managed via a shared state file and an exclusive
  * file lock.
  *
  * As an additional safety check, each candidate port is probed to skip ports that are actively
  * bound. This reduces the likelihood of port collisions with non-Canton processes.
  *
  * Probing cannot eliminate all race conditions. A candidate port that was returned by a previous
  * call to `next` but has not yet been bound by its caller will pass probing, as it is not yet
  * detected as occupied by the OS.
  *
  * @param portRangeStart
  *   first port of range (inclusive)
  * @param portRangeEnd
  *   last port of range (inclusive)
  * @param maxProbeAttempts
  *   how many counter values to skip at most before giving up on finding a bindable port
  * @param sharedPortNumFile
  *   file used for synchronization
  */
class UniquePortGenerator(
    val portRangeStart: Int,
    val portRangeEnd: Int,
    val maxProbeAttempts: Int,
    sharedPortNumFile: File,
) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  sharedPortNumFile.createFileIfNotExists(createParents = true)
  logger.debug(s"Initialized port file: ${sharedPortNumFile.path.toString}")

  private val counter = new UniqueBoundedCounter(
    dataFile = sharedPortNumFile.path,
    startValue = portRangeStart,
    maxValue = portRangeEnd,
  )(logger)

  def next: Port = {
    logger.trace("Attempting to find unique port ...")
    val start = System.nanoTime()

    @tailrec def nextBindable(attempt: Int): Port = {
      val candidate = Port.tryCreate(counter.incrementAndGet().fold(throw _, identity))
      if (isBindable(candidate)) candidate
      else if (attempt >= maxProbeAttempts)
        throw new IllegalStateException(
          s"Failed to find a bindable port after $maxProbeAttempts attempts in the range " +
            s"[$portRangeStart, $portRangeEnd]. Every candidate was already bound on this host."
        )
      else {
        // Logged at INFO because a skipped port means the counter has wrapped onto a port that is
        // still in use, or has walked into the ephemeral range: both are worth seeing in a test log.
        logger.info(s"Port $candidate is already bound, skipping it.")
        nextBindable(attempt + 1)
      }
    }

    val port = nextBindable(1)
    logger.debug(
      s"Found unique port $port after ${Duration.ofNanos(System.nanoTime() - start).toMillis} [ms]"
    )
    port
  }

  /** Checks whether `port` can currently be bound on this host.
    *
    * Binds the wildcard address, which fails if anything holds the port on any local address. That
    * is the conservative answer, given that callers bind a mix of the loopback address and the
    * wildcard address. `SO_REUSEADDR` mirrors what the servers under test do, so that a port merely
    * lingering in `TIME_WAIT` is not rejected here.
    */
  private def isBindable(port: Port): Boolean =
    Using(new ServerSocket()) { socket =>
      socket.setReuseAddress(true)
      socket.bind(new InetSocketAddress(port.unwrap))
    }.isSuccess

}

object UniquePortGenerator {

  /** Deliberately hard-coded because alternatives such as File.temp can differ from run to run. If
    * different runs use different folders for storing the state of UniquePortGenerator, the state
    * will fork and port collisions can occur as a result.
    */
  private val TmpDir: File = File("/tmp")

  /** Used for integration tests in general.
    */
  lazy val generatorForCantonTests: UniquePortGenerator =
    new UniquePortGenerator(
      30000,
      65500,
      30000,
      TmpDir / "canton_tests_unique_port_generator.dat",
    )

  /** Used for unit testing UniquePortGenerator. Note the port range is disjoint from
    * generatorForCantonTests.
    */
  @VisibleForTesting
  private[canton] lazy val generatorForUniquePortGeneratorTest: UniquePortGenerator =
    new UniquePortGenerator(
      65501,
      65535,
      30,
      TmpDir / "unique_port_generator_test_port_generator.dat",
    )

  /** Finds the next network port for use in canton tests.
    *
    * @return
    *   unique port for canton tests
    * @throws java.lang.IllegalStateException
    *   if the maximum number of probe attempts has been exhausted
    * @throws java.nio.channels.OverlappingFileLockException
    *   when failing to get an exclusive file lock after exhausting retries. (See
    *   [[com.digitalasset.canton.UniqueBoundedCounter]].maxRetries)
    */
  def next: Port = generatorForCantonTests.next
}

/** A counter implementation, reading and writing the integer value to file.
  *
  * Synchronization uses a JVM-level synchronized block for *intra-process* thead safety, and within
  * that, an OS-level exclusive [[java.nio.channels.FileLock]] on a separate lock file for
  * inter-process safety (acquired via blocking lock()).
  *
  * Allows specifying initial/maximum values (with mandatory maximum for wrap-around) and includes
  * retry logic for the overall operation (if FileLock fails).
  *
  * Manages two files (data file and lock file).
  *
  * IMPORTANT: Consult [[java.nio.channels.FileLock]] before making changes.
  *
  * @param dataFile
  *   The path to the data file which stores the counter.
  * @param startValue
  *   The value to initialize the counter with if the data file is created or found empty/invalid.
  * @param maxValue
  *   The maximum value (inclusive). The counter wraps around to `initialValue` when its value
  *   exceeds `maximumValue`.
  * @param maxRetries
  *   Maximum times to retry the *entire* operation if certain exceptions (like
  *   OverlappingFileLockException from lock()) occur. Defaults to 100.
  * @param retryDelayMillis
  *   Delay between *entire* operation retries. Defaults to 300ms.
  * @param logger
  *   An SLF4J logger instance.
  */
class UniqueBoundedCounter(
    dataFile: File,
    startValue: Int,
    maxValue: Int,
    maxRetries: Int = 100,
    retryDelayMillis: Long = 300,
)(logger: Logger) {

  require(maxValue > startValue, s"maxValue $maxValue must be greater than startValue $startValue")

  private val lockFile: File = File(dataFile.pathAsString + ".lock")
  private val lock = new Mutex()

  Try(lockFile.createIfNotExists(createParents = true)) match {
    case Success(_) => // OK
    case Failure(e: SecurityException) =>
      logger.error(s"Permission issue creating lock file '$lockFile': ${e.getMessage}")
      throw e
    case Failure(e) =>
      logger.error(s"Unexpected error creating lock file '$lockFile': ${e.getMessage}")
      throw e
  }

  def incrementAndGet(): Try[Int] = updateCounter(_ + 1)
  def get(): Try[Int] = updateCounter(identity)
  def addAndGet(delta: Int): Try[Int] = updateCounter(_ + delta)

  private def updateCounter(updateFn: Int => Int): Try[Int] = attemptWithRetries(updateFn, 1)

  @tailrec
  private def attemptWithRetries(operation: Int => Int, attempt: Int): Try[Int] = {

    val result: Try[Int] = perform(operation)

    result match {
      case Success(value) => Success(value)
      // Retry only on OverlappingFileLockException which might may occur from lock() due to inter-process contention
      case Failure(e: OverlappingFileLockException) =>
        if (attempt <= maxRetries) {
          logger.debug(
            s"Retrying operation due to OverlappingFileLockException (Attempt $attempt/$maxRetries), sleeping ${retryDelayMillis}ms ...",
            e,
          )
          (Threading.sleep(retryDelayMillis))
          attemptWithRetries(operation, attempt + 1)
        } else {
          val retriesExhaustedErrorMessage =
            s"Operation failed after $maxRetries attempts due to OverlappingFileLockException on '$lockFile'."
          logger.error(retriesExhaustedErrorMessage, e)
          Failure(new RuntimeException(retriesExhaustedErrorMessage, e))
        }
      case Failure(other) =>
        // Other errors that are not retried, for example IO errors during mutation, potentially others from lock()
        val otherErrorMessage =
          s"Operation failed with non-retryable error. Lock file: '$lockFile' | Data file: '$dataFile'"
        logger.error(otherErrorMessage, other)
        Failure(new RuntimeException(otherErrorMessage, other))
    }
  }

  /** Performs a single attempt to acquire the file lock and executes the operation.
    *
    * It ensures
    *   - single thread attempts file locking (reduces lock file contention OS processes), and that
    *   - data file mutation is properly serialized (required as per [[java.nio.channels.FileLock]]
    *     JavaDoc).
    */
  private def perform(operation: Int => Int): Try[Int] = blocking {
    lock.exclusive {
      Using(lockFile.newFileChannel(Seq(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
        lockChannel =>
          var fileLock: FileLock = null
          try {
            // Acquire file lock using blocking lock()
            // This may block and throw OverlappingFileLockException if another OS process holds the lock.
            // This may also throw numerous other exceptions!
            logger.trace("Attempting to acquire file lock via blocking lock()...")
            fileLock = lockChannel.lock()
            logger.trace("Acquired file lock.")

            logger.trace("Mutating counter...")
            val dataAccessResult = mutateCounter(operation)
            logger.trace("Counter changed.")

            dataAccessResult.fold(throw _, identity)
          } finally {
            if (fileLock != null) {
              logger.trace("Releasing file lock.")
              Try(fileLock.release())
            } else {
              logger.trace(
                "Nothing to release. File lock was null because the attempt to acquire the file lock failed " +
                  "with an exception, most likely an OverlappingFileLockException has been thrown."
              )
            }
          }
      }
    }
  }

  /** Mutates the counter value in the data file based on the given update function.
    *
    * IMPORTANT: This method should only be called when the JVM lock (part of a synchronized block)
    * AND the file lock are held!
    */
  private def mutateCounter(updateFn: Int => Int): Try[Int] = Try {
    val currentValue = if (dataFile.isEmpty) {
      logger.info(
        s"Data file '$dataFile' is empty, starting the counter over at $startValue"
      )
      startValue
    } else {
      dataFile.contentAsString(StandardCharsets.UTF_8).toInt
    }

    val potentialNewValue = updateFn(currentValue)
    val newValue = if (potentialNewValue > maxValue) {
      logger.info(
        s"Counter in '$dataFile' wrapped around from $currentValue to $startValue (maximum $maxValue)."
      )
      startValue
    } else potentialNewValue

    dataFile.overwrite(newValue.toString)(charset = StandardCharsets.UTF_8)

    newValue
  }

}
