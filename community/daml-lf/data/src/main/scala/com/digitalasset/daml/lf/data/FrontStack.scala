// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/** A stack which allows to cons, prepend, and pop in constant time, and generate an ImmArray in
  * linear time. Very useful when needing to traverse stuff in topological order or similar
  * situations.
  */
final class FrontStack[+A] private (protected val fq: FrontStack.FQ[A], val length: Int) {

  import FrontStack.*

  /** O(n) */
  @throws[IndexOutOfBoundsException]
  def slowApply(ix: Int): A = {
    if (ix < 0) throw new IndexOutOfBoundsException(ix.toString)
    val i = iterator

    @tailrec def lp(ix: Int): A =
      if (!i.hasNext) throw new IndexOutOfBoundsException(ix.toString)
      else {
        val v = i.next()
        if (ix <= 0) v else lp(ix - 1)
      }
    lp(ix)
  }

  /** O(1) */
  def +:[B >: A](x: B): FrontStack[B] = new FrontStack(FQCons(x, fq), length + 1)

  /** O(1) */
  def ++:[B >: A](xs: ImmArray[B]): FrontStack[B] =
    if (xs.length > 0) {
      new FrontStack(FQPrepend(xs, fq), length + xs.length)
    } else {
      this
    }

  /** O(n) */
  def toImmArray: ImmArray[A] = {
    val array = new Array[Any](length)

    @tailrec
    def go(cursor: Int, fq: FQ[A]): Unit = fq match {
      case FQEmpty => ()
      case FQCons(head, tail) =>
        array(cursor) = head
        go(cursor + 1, tail)
      case FQPrepend(head, tail) =>
        for (i <- head.indices) {
          array(cursor + i) = head(i)
        }
        go(cursor + head.length, tail)
    }
    go(0, fq)

    ImmArray.unsafeFromArray[A](array)
  }

  /** O(1) */
  def pop: Option[(A, FrontStack[A])] =
    if (length > 0) {
      fq match {
        case FQEmpty => throw new RuntimeException(s"FrontStack has length $length but FQEmpty.")
        case FQCons(head, tail) => Some((head, new FrontStack(tail, length - 1)))
        case FQPrepend(head, tail) =>
          if (head.length > 1) {
            Some((head.head, new FrontStack(FQPrepend(head.tail, tail), length - 1)))
          } else {
            // NOTE: We maintain the invariant that `head` is never empty.
            Some((head.head, new FrontStack(tail, length - 1)))
          }
      }
    } else {
      None
    }

  /** O(n) */
  def map[B](f: A => B): FrontStack[B] = from(toImmArray.map(f))

  def foldLeft[B](z: B)(f: (B, A) => B): B = {
    @tailrec
    def go(acc: B, fq: FQ[A]): B = fq match {
      case FQEmpty => acc
      case FQCons(head, tail) => go(f(acc, head), tail)
      case FQPrepend(head, tail) =>
        val newAcc = head.foldLeft(acc)(f)
        go(newAcc, tail)
    }
    go(z, fq)
  }

  /** O(1) */
  def isEmpty: Boolean = length == 0

  /** O(1) */
  def nonEmpty: Boolean = length > 0

  /** O(1) */
  def iterator: Iterator[A] = {
    val that = this
    new Iterator[A] {
      var queue: FrontStack[A] = that

      override def next(): A = queue.pop match {
        case Some((head, tail)) =>
          queue = tail
          head
        case None =>
          throw new NoSuchElementException("head of empty list")
      }

      override def hasNext: Boolean = queue.nonEmpty
    }
  }

  /** O(n) */
  def toBackStack: BackStack[A] = {
    @tailrec
    def go(acc: BackStack[A], self: FQ[A]): BackStack[A] =
      self match {
        case FQCons(head, tail) => go(acc :+ head, tail)
        case FQPrepend(head, tail) => go(acc :++ head, tail)
        case FQEmpty => acc
      }
    go(BackStack.empty, fq)
  }

  /** O(n) */
  def foreach(f: A => Unit): Unit = this.iterator.foreach(f)

  /** O(n) */
  override def equals(that: Any): Boolean = that match {
    case thatQueue: FrontStack[?] =>
      this.length == thatQueue.length && this.iterator.sameElements[Any](thatQueue.iterator)
    case _ => false
  }

  override def hashCode(): Int =
    MurmurHash3.orderedHash(iterator)

  /** O(n) */
  override def toString: String = "FrontStack(" + iterator.map(_.toString).mkString(",") + ")"
}

object FrontStack extends scala.collection.IterableFactory[FrontStack] {
  val Empty: FrontStack[Nothing] = new FrontStack(FQEmpty, 0)

  def empty[A]: FrontStack[A] = FrontStack.Empty

  def from[A](xs: ImmArray[A]): FrontStack[A] =
    if (xs.isEmpty) Empty else new FrontStack(FQPrepend(xs, FQEmpty), length = xs.length)

  override def from[A](it: IterableOnce[A]): FrontStack[A] =
    FrontStack.from(ImmArray.from(it))

  def unapply[T](xs: FrontStack[T]): Boolean = xs.isEmpty

  override def newBuilder[A]: mutable.Builder[A, FrontStack[A]] =
    ImmArray.newBuilder.mapResult(arr => FrontStack.from(arr))

  protected sealed trait FQ[+A]
  protected case object FQEmpty extends FQ[Nothing]
  protected final case class FQCons[A](head: A, tail: FQ[A]) extends FQ[A]
  // INVARIANT: head is non-empty
  private final case class FQPrepend[A](head: ImmArray[A], tail: FQ[A]) extends FQ[A]

  implicit val traverseInstances: cats.Traverse[FrontStack] = new cats.Traverse[FrontStack] {
    override def traverse[G[_], A, B](fa: FrontStack[A])(f: A => G[B])(implicit
        G: cats.Applicative[G]
    ): G[FrontStack[B]] = {
      val accumulated =
        foldRight(fa, cats.Eval.now(G.pure(List.empty[B])))((a, acc) =>
          G.map2Eval(f(a), acc)(_ :: _)
        )
      G.map(accumulated.value) { elems =>
        val builder = newBuilder[B]
        builder.sizeHint(fa.length)
        builder.addAll(elems).result()
      }
    }

    override def foldLeft[A, B](fa: FrontStack[A], b: B)(f: (B, A) => B): B =
      fa.foldLeft(b)(f)

    override def foldRight[A, B](fa: FrontStack[A], lb: cats.Eval[B])(
        f: (A, cats.Eval[B]) => cats.Eval[B]
    ): cats.Eval[B] = {
      def loop(st: FQ[A], lb: cats.Eval[B]): cats.Eval[B] =
        st match {
          case FQCons(head, tail) =>
            f(head, cats.Eval.defer(loop(tail, lb)))
          case FQPrepend(head, tail) =>
            ImmArray.traverseInstance.foldRight(head, cats.Eval.defer(loop(tail, lb)))(f)
          case FQEmpty =>
            lb
        }
      loop(fa.fq, lb)
    }
  }
}

object FrontStackCons {
  def unapply[A](xs: FrontStack[A]): Option[(A, FrontStack[A])] = xs.pop
}
