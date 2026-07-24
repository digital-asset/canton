// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import cats.Id
import cats.data.Chain
import cats.syntax.either.*
import com.digitalasset.canton.logging.SuppressionRule.Level
import com.digitalasset.canton.util.Checked
import com.digitalasset.canton.util.collection.MapsUtil.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level as LogLevel

import java.util.concurrent.CyclicBarrier
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{Future, blocking}

class MapsUtilTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private case class Error(key: Int, oldValue: String, newValue: String)
  private case class Counter(value: Int)
  private case class Blocker(code: Int)
  private case class NonBlocker(message: String)

  /** Executes a given operation concurrently across multiple threads to test thread safety and
    * atomicity.
    *
    * Uses a [[java.util.concurrent.CyclicBarrier]] to ensure all worker threads start the target
    * operation at exactly the same time, maximizing contention.
    *
    * @param workerCount
    *   The number of parallel threads to spawn.
    * @param updateFn
    *   The operation each thread should execute.
    * @tparam A
    *   The result type of the operation.
    * @return
    *   A sequence containing the results from all worker threads.
    */
  private def runConcurrentMapUpdates[A](workerCount: Int)(
      updateFn: => A
  ): Seq[A] = {
    val barrier = new CyclicBarrier(workerCount)
    val updates = (1 to workerCount).map { _ =>
      Future {
        blocking(barrier.await())
        updateFn
      }
    }
    Future.sequence(updates).futureValue
  }

  /** Generates standard test scenarios for basic monadic map update operations.
    *
    * Tests both the modification of an existing key and the insertion of a new key (when missing).
    *
    * @param contextName
    *   The descriptive name for the test block (e.g., "when calling myMethod").
    * @param runUpdate
    *   A function wrapping the specific MapsUtil method to be tested.
    * @param expectedExistingReturn
    *   The expected result when updating an existing key.
    * @param expectedMissingReturn
    *   The expected result when inserting a missing key.
    * @tparam Res
    *   The return type of the update operation.
    */
  private def testBasicMapUpdates[Res](
      contextName: String,
      runUpdate: (scala.collection.concurrent.Map[String, Int], String, Int, Int => Int) => Res,
      expectedExistingReturn: Res,
      expectedMissingReturn: Res,
  ): Unit =
    contextName should {
      "atomically modify an existing key" in {
        val map = TrieMap("key" -> 1)
        val result = runUpdate(map, "key", 0, _ + 1)

        result shouldBe expectedExistingReturn // Returns the OLD value, () or None depending on the variant
        map("key") shouldBe 2 // Map is updated to the NEW value
      }

      "insert notFound value if key is missing" in {
        val map = TrieMap.empty[String, Int]
        val result = runUpdate(map, "missing", -1, _ => 0)

        result shouldBe expectedMissingReturn // No old value existed
        map("missing") shouldBe -1 // inserted the notFound value
      }
    }

  /** Generates comprehensive test scenarios for [[Checked]] monadic map update operations.
    *
    * Validates the correct propagation of aborts and non-aborts for standard insertions and
    * updates.
    *
    * @param contextName
    *   The descriptive name for the test block.
    * @param runUpdate
    *   A function wrapping the specific MapsUtil Checked method to be tested.
    */
  private def testCheckedMapUpdates(
      contextName: String,
      runUpdate: (
          scala.collection.concurrent.Map[String, Int],
          String,
          Checked[Blocker, NonBlocker, Option[Int]],
          Int => Checked[Blocker, NonBlocker, Option[Int]],
      ) => Checked[Blocker, NonBlocker, Unit],
  ): Unit =
    contextName should {
      "atomically modify an existing key" in {
        val map = TrieMap("key" -> 1)
        val result = runUpdate(
          map,
          "key",
          Checked.continueWithResult(NonBlocker("not found non-blocker"), Some(0)),
          oldValue => Checked.result(Some(oldValue + 1)),
        )
        result shouldBe Checked.result(())
        map("key") shouldBe 2 // Map is updated to the NEW value
      }

      "atomically modify an existing key and collect a non-blocker" in {
        val map = TrieMap("key" -> 1)
        val result = runUpdate(
          map,
          "key",
          Checked.continueWithResult(NonBlocker("not found non-blocker"), Some(0)),
          oldValue =>
            Checked.continueWithResult(NonBlocker("modify non-blocker"), Some(oldValue + 1)),
        )
        result shouldBe Checked.continueWithResult(NonBlocker("modify non-blocker"), ())
        map("key") shouldBe 2 // Map is updated to the NEW value
      }

      "not modify the map and collect a blocker and non-blocker" in {
        val map = TrieMap("key" -> 1)
        val result = runUpdate(
          map,
          "key",
          Checked.continueWithResult(NonBlocker("not found non-blocker"), Some(0)),
          _ => Checked.Abort(Blocker(42), Chain(NonBlocker("modify non-blocker"))),
        )
        result shouldBe Checked.Abort(Blocker(42), Chain(NonBlocker("modify non-blocker")))
        map("key") shouldBe 1 // Map not updated because the operation was aborted
      }

      "insert notFound value if key is missing" in {
        val map = TrieMap.empty[String, Int]
        val result = runUpdate(
          map,
          "missing",
          Checked.result(Some(-1)),
          _ => Checked.continueWithResult(NonBlocker("modify non-blocker"), Some(0)),
        )
        result shouldBe Checked.result(())
        map("missing") shouldBe -1 // inserted the notFound value
      }

      "insert notFound value if key is missing and collect a non-blocker" in {
        val map = TrieMap.empty[String, Int]
        val result = runUpdate(
          map,
          "missing",
          Checked.continueWithResult(NonBlocker("not found non-blocker"), Some(-1)),
          _ => Checked.continueWithResult(NonBlocker("modify non-blocker"), Some(0)),
        )
        result shouldBe Checked.continueWithResult(NonBlocker("not found non-blocker"), ())
        map("missing") shouldBe -1 // inserted the notFound value
      }

      "not insert a new value if key is missing and collect a blocker and non-blocker" in {
        val map = TrieMap.empty[String, Int]
        val result = runUpdate(
          map,
          "missing",
          Checked.Abort(Blocker(42), Chain(NonBlocker("not found non-blocker"))),
          _ => Checked.continueWithResult(NonBlocker("modify non-blocker"), Some(0)),
        )
        result shouldBe Checked.Abort(Blocker(42), Chain(NonBlocker("not found non-blocker")))
        map.get("missing") shouldBe None // nothing was inserted because the operation was aborted
      }
    }

  "MapsUtil" should {

    "correctly group by multiple values" in {
      val m = Map[String, Int]("abc" -> 1, "cde" -> 2, "def" -> 3)
      groupByMultipleM[Id, String, Char, Int](m)(s => s.toSet) shouldBe Map(
        'a' -> Set(1),
        'b' -> Set(1),
        'c' -> Set(1, 2),
        'd' -> Set(2, 3),
        'e' -> Set(2, 3),
        'f' -> Set(3),
      )
    }

    "build non conflicting maps" in {
      toNonConflictingMap(Seq(1 -> 2, 2 -> 3)) shouldBe Right(Map(1 -> 2, 2 -> 3))
      toNonConflictingMap(Seq(1 -> 2, 2 -> 3, 1 -> 2)) shouldBe Right(Map(1 -> 2, 2 -> 3))
      toNonConflictingMap(Seq(1 -> 2, 1 -> 3)) shouldBe Left(Map(1 -> Set(2, 3)))
    }

    "compute intersection based on values" in {
      val empty = Map.empty[String, Set[Int]]
      intersectValues(Map("1" -> Set(1)), empty) shouldBe Map.empty
      intersectValues(empty, Map("1" -> Set(1))) shouldBe Map.empty
      intersectValues(Map("1" -> Set(1)), Map("2" -> Set(2))) shouldBe Map.empty
      intersectValues(Map("1" -> Set(1)), Map("2" -> Set(2), "1" -> Set(1))) shouldBe Map(
        "1" -> Set(1)
      )
      intersectValues(
        Map("1" -> Set(1), "2" -> Set(20, 21, 22, 23)),
        Map("2" -> Set(20, 23, 25), "1" -> Set(1)),
      ) shouldBe Map("1" -> Set(1), "2" -> Set(20, 23))
    }

    "transpose" in {
      transpose(Map(1 -> Set("a"), 2 -> Set("a"))) shouldBe Map("a" -> Set(1, 2))
      transpose(Map(1 -> Set("a", "b"))) shouldBe Map("a" -> Set(1), "b" -> Set(1))
      transpose(Map("a" -> Set.empty[Int], "b" -> Set(1))) shouldBe Map(1 -> Set("b"))
      transpose(Map.empty[Int, Set[String]]) shouldBe Map.empty[String, Set[Int]]
    }

    "when calling mergeMapsOfSets" should {
      "merge two empty maps" in {
        mergeMapsOfSets(
          Map.empty[Int, Set[String]],
          Map.empty[Int, Set[String]],
        ) shouldBe Map.empty
      }
      "merge empty map with non-empty map" in {
        mergeMapsOfSets(Map(1 -> Set("a")), Map.empty[Int, Set[String]]) shouldBe Map(1 -> Set("a"))
        mergeMapsOfSets(Map.empty[Int, Set[String]], Map(3 -> Set("c"))) shouldBe Map(3 -> Set("c"))
      }
      "merge two non-empty maps" in {
        mergeMapsOfSets(
          Map(1 -> Set("a", "b"), 2 -> Set("x")),
          Map(1 -> Set("b", "c"), 3 -> Set("y")),
        ) shouldBe Map(1 -> Set("a", "b", "c"), 2 -> Set("x"), 3 -> Set("y"))
      }
    }

    // Using cats.Id as the Monad for synchronous testing
    testBasicMapUpdates(
      "when calling modifyWithConcurrentlyM",
      (map, k, notFound, f) =>
        modifyWithConcurrentlyM[Id, String, Int](map, k, Some(notFound), v => Some(f(v))),
      expectedExistingReturn = Some(1),
      expectedMissingReturn = None,
    )

    "when calling modifyWithConcurrentlyM (removal scenarios)" should {
      "remove key if f returns None" in {
        val map = TrieMap("key" -> 1)
        val result = modifyWithConcurrentlyM[Id, String, Int](
          map,
          "key",
          notFound = Some(0),
          f = _ => None, // Returning None signals deletion
        )
        result shouldBe Some(1)
        map.contains("key") shouldBe false
      }

      "modify map concurrently from multiple threads" in {
        val key = 1
        val workerCount = 4
        val map = TrieMap(key -> Counter(0))

        val results = runConcurrentMapUpdates(workerCount) {
          modifyWithConcurrentlyM[Id, Int, Counter](
            map,
            key,
            notFound = Some(Counter(0)),
            f = current => Some(Counter(current.value + 1)),
          )
        }

        all(results.map(_.isDefined)) shouldBe true
        map.get(key) shouldBe Some(Counter(workerCount))
      }
    }

    // Using cats.Id as the Monad for synchronous testing
    testBasicMapUpdates(
      "when calling updateWithConcurrentlyM",
      (map, k, notFound, f) => updateWithConcurrentlyM[Id, String, Int](map, k, notFound, f),
      expectedExistingReturn = Some(1),
      expectedMissingReturn = None,
    )

    // Using cats.Id as the Monad for synchronous testing
    testBasicMapUpdates(
      "when calling modifyWithConcurrentlyM_",
      (map, k, notFound, f) =>
        modifyWithConcurrentlyM_[Id, String, Int](map, k, Some(notFound), v => Some(f(v))),
      expectedExistingReturn = (),
      expectedMissingReturn = (),
    )

    "when calling modifyWithConcurrentlyM_ (removal scenarios)" should {
      "remove key if f returns None" in {
        val map = TrieMap("key" -> 1)
        val result = modifyWithConcurrentlyM_[Id, String, Int](
          map,
          "key",
          notFound = Some(0),
          f = _ => None, // Returning None signals deletion
        )
        result shouldBe ()
        map.contains("key") shouldBe false
      }
    }

    // Using cats.Id as the Monad for synchronous testing
    testBasicMapUpdates(
      "when calling updateWithConcurrentlyM_",
      (map, k, notFound, f) => updateWithConcurrentlyM_[Id, String, Int](map, k, notFound, f),
      expectedExistingReturn = (),
      expectedMissingReturn = (),
    )

    testCheckedMapUpdates(
      "when calling modifyWithConcurrentlyChecked_",
      (map, key, notFnd, f) =>
        modifyWithConcurrentlyChecked_[Blocker, NonBlocker, String, Int](
          map,
          key,
          notFnd,
          f,
        ),
    )

    "when calling modifyWithConcurrentlyChecked_ (removal scenarios)" should {
      "remove key if f returns None" in {
        val map = TrieMap("key" -> 1)
        val result = modifyWithConcurrentlyChecked_[Blocker, NonBlocker, String, Int](
          map,
          "key",
          notFound = Checked.result(Some(0)),
          f = _ => Checked.result(None), // Returning None signals deletion
        )
        result shouldBe Checked.result(())
        map.contains("key") shouldBe false
      }

      "remove key if f returns None and collect non-blocker" in {
        val map = TrieMap("key" -> 1)
        val result = modifyWithConcurrentlyChecked_[Blocker, NonBlocker, String, Int](
          map,
          "key",
          notFound = Checked.continueWithResult(NonBlocker("not found non-blocker"), Some(0)),
          f = _ =>
            Checked.continueWithResult(
              NonBlocker("modify non-blocker"),
              None,
            ), // Returning None signals deletion
        )
        result shouldBe Checked.continueWithResult(NonBlocker("modify non-blocker"), ())
        map.contains("key") shouldBe false
      }

      "not remove key if f returns None and collect a blocker and non-blocker" in {
        val map = TrieMap("key" -> 1)
        val result = modifyWithConcurrentlyChecked_[Blocker, NonBlocker, String, Int](
          map,
          "key",
          notFound = Checked.continueWithResult(NonBlocker("not found non-blocker"), Some(0)),
          f = _ => Checked.Abort(Blocker(42), Chain(NonBlocker("modify non-blocker"))),
        )
        result shouldBe Checked.Abort(Blocker(42), Chain(NonBlocker("modify non-blocker")))
        map.contains("key") shouldBe true // Map not updated because the operation was aborted
      }
    }

    testCheckedMapUpdates(
      "when calling updateWithConcurrentlyChecked_",
      (map, key, notFound, f) =>
        updateWithConcurrentlyChecked_[Blocker, NonBlocker, String, Int](
          map,
          key,
          // Adapt `Checked[..., Option[V]]` back to `Checked[..., V]` to satisfy `update` signature
          notFound = notFound.map(_.getOrElse(-1)),
          f = v => f(v).map(_.getOrElse(v)),
        ),
    )

    "when calling updateWithConcurrently" should {
      "update map concurrently from multiple threads" in {
        val key = 1
        val workerCount = 4
        val map = TrieMap(key -> Counter(0))

        val results = runConcurrentMapUpdates(workerCount) {
          updateWithConcurrently(map, key) { current =>
            Counter(current.value + 1)
          }
        }

        all(results) shouldBe true
        map.get(key).value shouldBe Counter(workerCount)
      }

      "not update map when key is missing" in {
        val map = TrieMap.empty[Int, Counter]
        val didUpdate = updateWithConcurrently(map, key = 42) { current =>
          Counter(current.value + 1)
        }

        didUpdate shouldBe false
        map shouldBe TrieMap.empty
      }

      "not modify the map if the new value is reference-equal to the old value" in {
        val existingCounter = Counter(0)
        val map = TrieMap(1 -> existingCounter)

        val didUpdate = updateWithConcurrently(map, 1) { _ =>
          existingCounter // Return the exact same reference
        }

        didUpdate shouldBe false
        map(1) shouldBe existingCounter
      }
    }

    "when calling tryPutIdempotent" should {
      "put a new value into the map and do nothing for identical value" in {
        val map = TrieMap(1 -> "Foo")
        tryPutIdempotent(map, 2, "Bar")
        map shouldBe TrieMap(1 -> "Foo", 2 -> "Bar")
        tryPutIdempotent(map, 1, "Foo")
        map shouldBe TrieMap(1 -> "Foo", 2 -> "Bar")
      }

      "throw an exception for conflicting values" in {
        val map = TrieMap(1 -> "Foo")
        loggerFactory.assertThrowsAndLogsSuppressing[IllegalStateException](Level(LogLevel.ERROR))(
          tryPutIdempotent(map, 1, "Bar"),
          logEntry => {
            logEntry.level shouldBe LogLevel.ERROR
            logEntry.throwable.value.getMessage shouldBe "Map key 1 already has value Foo assigned to it. " +
              "Cannot insert Bar."
          },
        )
        map shouldBe TrieMap(1 -> "Foo")
      }
    }

    "when calling mergeWith" should {
      "merge two maps and combine elements" in {
        mergeWith(
          Map(1 -> "a", 2 -> "b"),
          Map(2 -> "B", 3 -> "c"),
        )(_ + _) shouldBe Map(1 -> "a", 2 -> "bB", 3 -> "c")
      }
      "merge empty map with non-empty map" in {
        mergeWith(Map.empty[Int, String], Map(4 -> "d"))(_ + _) shouldBe Map(4 -> "d")
      }
    }

    "when calling extendMapWith" should {
      "merge new values with existing values in mutable map" in {
        val map = mutable.Map(1 -> "a", 2 -> "b")
        extendMapWith(map, Seq(2 -> "B", 3 -> "c", 2 -> "C"))(_ + _)
        map shouldBe mutable.Map(1 -> "a", 2 -> "bBC", 3 -> "c")
      }
      "extend empty mutable map with new values" in {
        val map = mutable.Map.empty[Int, String]
        extendMapWith(map, Seq(1 -> "a", 2 -> "b", 1 -> "A"))(_ + _)
        map shouldBe mutable.Map(1 -> "aA", 2 -> "b")
      }
    }

    "when calling extendedMapWith" should {
      "merge new values with existing values in an immutable map" in {
        val map = Map(1 -> "a", 2 -> "b")
        val result = extendedMapWith(map, Seq(2 -> "B", 3 -> "c", 2 -> "C"))(_ + _)

        result shouldBe Map(1 -> "a", 2 -> "bBC", 3 -> "c")
        // Ensure the original map was not mutated
        map shouldBe Map(1 -> "a", 2 -> "b")
      }
    }

    "when calling skipEmpty" should {
      "drop None values and keeps Some values" in {
        skipEmpty(Map(1 -> Some("a"), 2 -> None, 3 -> Some("c"))) shouldBe Map(
          1 -> "a",
          3 -> "c",
        )
      }
      "return empty map when all values are None" in {
        skipEmpty(Map(1 -> None, 2 -> None)) shouldBe Map.empty[Int, String]
      }
    }

    "when calling insertIfAbsent" should {
      "insert new key-value pair" in {
        val map = TrieMap(1 -> "Foo", 2 -> "Bar")
        insertIfAbsent(map, 3, "test", Error.apply _) shouldBe Either.unit
      }
      "do nothing when key-value already exists" in {
        val map = TrieMap(1 -> "Foo", 2 -> "Bar")
        insertIfAbsent(map, 2, "Bar", Error.apply _) shouldBe Either.unit
      }
      "fail on inserting existing key with different value" in {
        val map = TrieMap(1 -> "Foo", 2 -> "Bar")
        insertIfAbsent(map, 2, "Something else", Error.apply _).left.value shouldBe an[Error]
      }
      "fail with static error on inserting existing key with different value" in {
        val errorMessage = "Insertion failed"
        val map = TrieMap(1 -> "Foo", 2 -> "Bar")
        insertIfAbsent(map, 2, "Something else", errorMessage).left.value shouldBe errorMessage
      }
    }
  }
}
