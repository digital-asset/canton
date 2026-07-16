// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotated methods are treated like `synchronized` when looking at the type of their arguments
  * for the purpose of flagging future-like types. Works only for methods with signatures of the
  * form
  *
  * {{{
  *   def foo[A](body: ...): ...
  * }}}
  */
final class SynchronizedLikeMethod extends StaticAnnotation
