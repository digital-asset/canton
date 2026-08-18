// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package validator

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value as V
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class ValueValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  val basicValue = V.ValueUnit
  val value1 = V.ValueText("deadbeef")
  val maxValueSize = ValueValidator.valueSize(basicValue)

  "test assumptions are met" in {
    ValueValidator.valueSize(basicValue) should be < ValueValidator.valueSize(value1)
  }

  s"limit value size to $maxValueSize bytes" - {
    val limits = Limits.Lenient.copy(valueSize = maxValueSize)

    "allow a transaction with no values" - {
      "fetch node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode()),
            ImmArray(NodeId(1)),
          )
        )

        ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    s"allow a transaction with a single value of size <= $maxValueSize" - {
      "create node" - {
        "create argument" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> createNode(arg = basicValue)),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
        }
      }

      "exercise node" - {
        "chosen value" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> exerciseNode(choiceArg = basicValue)),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
        }
      }

      "fetch node" - {
        "contract key" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> fetchNode(keyOpt = Some(globalKey(value = basicValue)))),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
        }
      }

      "query node" - {
        "contract key" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> queryNode(globalKey(value = basicValue))),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
        }
      }
    }

    s"disallow a transaction with one value of size > $maxValueSize" - {
      "create node" - {
        "create argument" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> createNode(arg = value1)),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }
      }

      "exercise node" - {
        "chosen value" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> exerciseNode(choiceArg = value1)),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }
      }

      "fetch node" - {
        "contract key" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> fetchNode(keyOpt = Some(globalKey(value = value1)))),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }
      }

      "query node" - {
        "contract key" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> queryNode(globalKey(value = value1))),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              pkgName,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }
      }
    }

    s"disallow a transaction with two values and one has size > $maxValueSize" - {
      "create node" - {
        "create argument" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(
                NodeId(1) -> createNode(arg = value1, keyOpt = Some(globalKey(value = basicValue)))
              ),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }

        "contract key" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(
                NodeId(1) -> createNode(arg = basicValue, keyOpt = Some(globalKey(value = value1)))
              ),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }
      }

      "exercise node" - {
        "chosen value" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> exerciseNode(choiceArg = value1, result = Some(basicValue))),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }

        "exercise result" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(NodeId(1) -> exerciseNode(choiceArg = basicValue, result = Some(value1))),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }

        "contract key" in {
          val tx = SubmittedTransaction(
            VersionedTransaction(
              version,
              HashMap(
                NodeId(1) -> exerciseNode(
                  choiceArg = basicValue,
                  keyOpt = Some(globalKey(value = value1)),
                )
              ),
              ImmArray(NodeId(1)),
            )
          )

          ValueValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
            Limit.ValueSize(
              coid,
              templateId,
              value1,
              limits.valueSize,
            )
          )
        }
      }
    }
  }
}
