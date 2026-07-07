// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.daml.lf.value.Value

/** Depth-aware, allocation-light bridge from an already parsed circe [[io.circe.Json]] to a
  * [[ujson.Value]].
  */
object CirceToUJson {

  /** Use default maximum nesting of Daml-LF values plus a small buffer for JSON structural
    * wrappers, so that we can decode the worst case Daml record embedded in JSON.
    */
  private val JsonDepthBuffer: Int = 10
  val DefaultMaxDepth: Int = Value.MAXIMUM_NESTING + JsonDepthBuffer

  /** Thrown when the JSON nesting exceeds the configured limit.
    */
  final class MaxNestingExceededException(val limit: Int)
      extends IllegalArgumentException(s"JSON nesting exceeds the maximum allowed depth of $limit")

  /** Transform a parsed circe value into a ujson value, rejecting inputs nested deeper than
    * `maxDepth`
    */
  def transform(json: io.circe.Json, maxDepth: Int = DefaultMaxDepth): ujson.Value =
    transform(json, depth = 1, maxDepth = maxDepth)

  // This is not tail recursive - but recursion depth is limited, we should not practically get Stack overflow
  private def transform(json: io.circe.Json, depth: Int, maxDepth: Int): ujson.Value = {
    if (depth > maxDepth) throw new MaxNestingExceededException(maxDepth)
    json.fold(
      jsonNull = ujson.Null,
      jsonBoolean = b => if (b) ujson.True else ujson.False,
      jsonNumber = n => ujson.Num(n.toDouble),
      jsonString = s => ujson.Str(s),
      jsonArray =
        arr => ujson.Arr.from(arr.iterator.map(child => transform(child, depth + 1, maxDepth))),
      jsonObject = obj =>
        ujson.Obj.from(
          obj.toIterable.iterator.map { case (k, v) => k -> transform(v, depth + 1, maxDepth) }
        ),
    )
  }
}
