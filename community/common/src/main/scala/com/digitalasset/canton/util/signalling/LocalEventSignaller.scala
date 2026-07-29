// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.signalling

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.{StashOfferResult, StashSource}
import com.digitalasset.canton.util.TryUtil.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.SubscriptionWithCancelException.{
  NoMoreElementsNeeded,
  StageWasCompleted,
}
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class LocalEventSignaller[K, S](
    keyName: String,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer)
    extends EventSignaller[K, S]
    with FlagCloseableAsync
    with NamedLogging {

  private val stashesByKey =
    new TrieMap[
      K,
      // support multiple subscriptions for keys. For sequencer subscriptions, for example,
      // this shouldn't be the normal case, but during token expiry or crash recovery
      // a member could subscribe again, while the old stash might still be active.
      //
      // The actual set behind this mutable.Set interface is thread-safe.
      // The TraceContext carried along is from the `readSignals` call,
      // to correlate later errors with the initial request.
      mutable.Set[Traced[StashSource[NotificationSignal[S]]]],
    ]()

  override def notify(
      notification: Notification[K],
      signal: S,
  ): Unit = {
    val keysToNotify = notification match {
      case Notification.Keys(keys) =>
        keys.iterator.flatMap(k => stashesByKey.get(k).map(k -> _))
      case Notification.All => stashesByKey.iterator
      case Notification.NoTarget => Iterator.empty
    }

    keysToNotify.foreach { case (k, stashes) =>
      stashes.foreach { traceStash =>
        traceStash.value.offer(NotificationSignal(signal)) match {
          case StashOfferResult.Stashed =>
          // happy case
          case StashOfferResult.Replaced(_) =>
          // nothing to do, the stash is just full and the previous pending signal has been replaced by `signal`.
          case StashOfferResult.StashClosed =>
            // the stash was closed, so let's remove the entry
            stashes.remove(traceStash).discard
          case StashOfferResult.Failure(ex) =>
            ex match {
              case NoMoreElementsNeeded | StageWasCompleted =>
                logger.info(
                  s"Not stashing notification for $keyName $k, because subscription was closed."
                )(traceStash.traceContext)
              case _ =>
                logger.info(
                  s"Unable to stash signal for $keyName $k, because the stash failed with an error.",
                  ex,
                )(traceStash.traceContext)
            }
            stashes.remove(traceStash).discard
        }
      }
      if (stashes.isEmpty) {
        // if a stash was added between the if above and the updateWith call,
        // then the key doesn't get removed and we don't "lose" stash entries.
        stashesByKey.updateWith(k)(_.filter(stashes => stashes.nonEmpty)).discard
      }
    }
  }

  override def readSignals(
      k: K,
      prettyK: String,
  )(implicit traceContext: TraceContext): Source[NotificationSignal[S], NotUsed] = {
    logger.info(s"Creating signal source for $keyName $prettyK")
    val (stash, source) = PekkoUtil.stashSource[NotificationSignal[S]].preMaterialize()
    val registeredUS = synchronizeWithClosingSync(s"readSignals($prettyK)")(
      stashesByKey
        .updateWith(k)(
          // create a concurrent set based on ConcurrentHashMap, but use the scala api for it
          _.orElse(
            Some(
              ConcurrentHashMap.newKeySet[Traced[StashSource[NotificationSignal[S]]]]().asScala
            )
          )
            .map(_.addOne(Traced(stash)(traceContext)))
        )
        .discard
    )

    UnlessShutdown.failOnShutdownToAbortException(
      Success(registeredUS),
      s"readSignals($prettyK)",
    ) match {
      case Success(_) =>
        // if registration was successful, just return the Source the corresponds to the stash
        source
      case Failure(shutdown) =>
        // registration happened during shutdown: fail the stash and return a failed Source
        logger.info(s"Could not register stash for $keyName $prettyK due to an ongoing shutdown.")
        stash.fail(shutdown)
        Source.failed(shutdown)
    }
  }

  protected override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    val closables = stashesByKey.flatMap { case (k, stashes) =>
      stashes
        .filter(!_.value.isCompleted)
        .map(q =>
          SyncCloseable(
            s"stashes.complete($keyName=$k)",
            Try(q.value.complete()).forFailed(ex =>
              logger.info(s"Error while completing the stash for $keyName $k", ex)(
                q.traceContext
              )
            ),
          )
        )
    }.toSeq
    stashesByKey.clear()
    closables
  }
}
