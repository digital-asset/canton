// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

object IterableUtil {

  /** Split an iterable into a lazy Stream of consecutive elements with the same value of `f(element)`.
    */
  def spansBy[A, B](iterable: Iterable[A])(f: A => B): LazyList[(B, Iterable[A])] = {
    iterable.headOption match {
      case None => LazyList.empty
      case Some(x) =>
        val (first, rest) = iterable.span(f(_) == f(x))
        (f(x) -> first) #:: spansBy(rest)(f)
    }
  }

  /** Returns the zipping of `elems` with `seq` where members `y` of `seq` are skipped if `!by(x, y)`
    * for the current member `x` from `elems`. Zipping stops when there are no more elements in `elems` or `seq`
    */
  def subzipBy[A, B, C](elems: Iterator[A], seq: Iterator[B])(by: (A, B) => Option[C]): Seq[C] = {
    val zipped = Seq.newBuilder[C]

    @tailrec def go(headElem: A): Unit = if (seq.hasNext) {
      val headSeq = seq.next()
      by(headElem, headSeq) match {
        case Some(c) =>
          zipped += c
          if (elems.hasNext) go(elems.next())
        case None => go(headElem)
      }
    }

    if (elems.hasNext) go(elems.next())
    zipped.result()
  }

  /** @throws java.lang.IllegalArgumentException If `objects` contains more than one distinct element */
  def assertAtMostOne[T](objects: Seq[T], objName: String)(implicit
      loggingContext: ErrorLoggingContext
  ): Option[T] =
    objects.distinct match {
      case Seq() => None
      case Seq(single) => Some(single)
      case multiples =>
        ErrorUtil.internalError(
          new IllegalArgumentException(s"Multiple ${objName}s $multiples. Expected at most one.")
        )
    }

  /** Map the function `f` over a sequence and reduce the result with function `g`,
    * mapping and reducing is done in parallel given the desired `parallelism`.
    *
    * This method works best if the amount of work for computing `f` and `g` is roughly constant-time,
    * i.e., independent of the data that is being processed, because then each chunk to process takes about
    * the same time.
    *
    * @param parallelism Determines the number of chunks that are created for parallel processing.
    * @param f The mapping function.
    * @param g The reducing function. Must be associative.
    * @return The result of `xs.map(f).reduceOption(g)`. If `f` or `g` throw exceptions,
    *         the returned future contains such an exception, but it is not guaranteed
    *         that the returned exception is the "first" such exception in a fixed sequential execution order.
    */
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def mapReducePar[A, B](parallelism: PositiveNumeric[Int], xs: Seq[A])(
      f: A => B
  )(g: (B, B) => B)(implicit ec: ExecutionContext): Future[Option[B]] = {
    if (xs.isEmpty) { Future.successful(None) }
    else {
      val futureCount = parallelism.value
      val chunkSize = (xs.size + futureCount - 1) / futureCount
      Future
        .traverse(xs.grouped(chunkSize)) { chunk =>
          // Run map-reduce in parallel for each chunk
          Future {
            val mapped = chunk.map(f)
            checked(mapped.reduce(g)) // `chunk` is non-empty
          }
        }
        .map { reducedChunks =>
          // Finally reduce the reducts of the chunks
          Some(reducedChunks.reduce(g))
        }
    }
  }
}
