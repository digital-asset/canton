// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.Id
import com.digitalasset.canton.BaseTest.UnsupportedExternalPartyTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.topology.PartyKind
import com.digitalasset.canton.version.ProtocolVersion
import org.scalactic.source
import org.scalactic.source.Position
import org.scalatest.compatible.Assertion
import org.scalatest.wordspec.{
  AnyWordSpecLike,
  AsyncWordSpecLike,
  FixtureAnyWordSpecLike,
  FixtureAsyncWordSpecLike,
}

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.language.implicitConversions

/** Adds utilities for ignoring tests based on a predicate in various scalatest suites. For example:
  * {{{
  * class MyTest extends EnterpriseIntegrationTest with ProtocolVersionChecksFixtureAnyWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in { implicit env =>
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * @see
  *   com.digitalasset.canton.BaseTest.testedProtocolVersion on how to set the protocol version for
  *   tests at runtime.
  */
trait ProtocolVersionCheckString[F[_], FUS[_], A, Wrapper] {
  protected def wrapper(condition: => Boolean): Wrapper
  protected def partiesKind: PartyKind
  protected def testedProtocolVersion: ProtocolVersion

  def onlyRunWhen(condition: => Boolean): Wrapper = wrapper(condition)

  // Reason is not used but forces us to identify the reason which can be linked to a T O D O
  @nowarn("cat=unused")
  def onlyRunWithLocalParty(reason: UnsupportedExternalPartyTest): Wrapper =
    onlyRunWhen(partiesKind == PartyKind.Local)

  def onlyRunWithOrGreaterThan(minProtocolVersion: ProtocolVersion): Wrapper =
    onlyRunWhen(testedProtocolVersion >= minProtocolVersion)

  def onlyRunWith(protocolVersion: ProtocolVersion): Wrapper =
    onlyRunWhen(testedProtocolVersion == protocolVersion)

  def onlyRunWhen(condition: ProtocolVersion => Boolean): Wrapper =
    onlyRunWhen(condition(testedProtocolVersion))

  def onlyRunLessThan(pvUpperBoundExclusive: ProtocolVersion): Wrapper =
    onlyRunWhen(testedProtocolVersion < pvUpperBoundExclusive)

  def onlyRunWithOrLessThan(pvUpperBoundInclusive: ProtocolVersion): Wrapper =
    onlyRunWhen(testedProtocolVersion <= pvUpperBoundInclusive)
}

object ProtocolVersionCheckString {
  private final class Impl[Verb, F[_], FUS[_], A, Wrapper](
      verbWrapper: VerbWrapper[Verb, F, FUS, A, Wrapper],
      verb: Verb,
      override val testedProtocolVersion: ProtocolVersion,
      override val partiesKind: PartyKind,
  ) extends ProtocolVersionCheckString[F, FUS, A, Wrapper] {
    override protected def wrapper(condition: => Boolean): Wrapper =
      verbWrapper.wrapper(verb, condition)
  }

  def apply[V, F[_], FUS[_], A, W](verbWrapper: VerbWrapper[V, F, FUS, A, W])(
      verb: V,
      protocolVersion: ProtocolVersion,
      partiesKind: PartyKind,
  ): ProtocolVersionCheckString[F, FUS, A, W] =
    new Impl[V, F, FUS, A, W](verbWrapper, verb, protocolVersion, partiesKind)
}

/** Defines the test case methods to be called on test names annotated with the extensions from
  * [[ProtocolVersionCheckString]]
  */
trait OnlyRunWhenWordSpecStringWrapper[F[_], FUS[_], A] {
  def in(testFun: => F[A])(implicit pos: source.Position): Unit
  def inUS(testFun: => FUS[A])(implicit pos: source.Position): Unit
  def ignore(testFun: => F[A])(implicit pos: source.Position): Unit
}

object OnlyRunWhenWordSpecStringWrapper {
  class Impl[Verb, F[_], FUS[_], A](
      verbWrapper: VerbWrapper[Verb, F, FUS, A, ?],
      verb: Verb,
      condition: => Boolean,
  ) extends OnlyRunWhenWordSpecStringWrapper[F, FUS, A] {
    def in(testFun: => F[A])(implicit pos: source.Position): Unit =
      if (condition) verbWrapper.inVerb(verb, testFun) else verbWrapper.ignoreVerb(verb, testFun)

    def inUS(testFun: => FUS[A])(implicit pos: source.Position): Unit =
      if (condition) verbWrapper.inVerb(verb, verbWrapper.FUStoF(testFun))
      else verbWrapper.ignoreVerb(verb, verbWrapper.FUStoF(testFun))

    override def ignore(testFun: => F[A])(implicit pos: Position): Unit =
      verbWrapper.ignoreVerb(verb, testFun)
  }

  def apply[Verb, F[_], FUS[_], A](
      verbWrapper: VerbWrapper[Verb, F, FUS, A, ?],
      verb: Verb,
      condition: => Boolean,
  ): OnlyRunWhenWordSpecStringWrapper[F, FUS, A] =
    new Impl[Verb, F, FUS, A](verbWrapper, verb, condition)
}

/** Defines further the test case methods to be called on test names annotated with the extensions
  * from [[ProtocolVersionCheckString]]
  */
trait RichOnlyRunWhenWordSpecStringWrapper[F[_], FUS[_], A]
    extends OnlyRunWhenWordSpecStringWrapper[F, FUS, A] {
  def when(testFun: => Unit)(implicit pos: source.Position): Unit
}

object RichOnlyRunWhenWordSpecStringWrapper {
  class Impl[Verb, F[_], FUS[_], A](
      verbWrapper: RichVerbWrapper[Verb, F, FUS, A, ?],
      verb: Verb,
      condition: => Boolean,
  ) extends OnlyRunWhenWordSpecStringWrapper.Impl[Verb, F, FUS, A](verbWrapper, verb, condition)
      with RichOnlyRunWhenWordSpecStringWrapper[F, FUS, A] {
    override def when(testFun: => Unit)(implicit pos: Position): Unit =
      if (condition) verbWrapper.whenVerb(verb, testFun)
  }

  def apply[Verb, F[_], FUS[_], A](
      verbWrapper: RichVerbWrapper[Verb, F, FUS, A, ?],
      verb: Verb,
      condition: => Boolean,
  ): RichOnlyRunWhenWordSpecStringWrapper[F, FUS, A] =
    new Impl[Verb, F, FUS, A](verbWrapper, verb, condition)
}

/** Type class for possibly annotated test case names (`Verb`). */
trait VerbWrapper[Verb, F[_], FUS[_], A, Wrapper] {
  def inVerb(verb: Verb, testFun: => F[A])(implicit pos: source.Position): Unit
  def ignoreVerb(verb: Verb, testFun: => F[A])(implicit pos: source.Position): Unit
  def FUStoF(testFun: => FUS[A])(implicit pos: source.Position): F[A]
  def wrapper(verb: Verb, condition: => Boolean): Wrapper
}

trait RichVerbWrapper[Verb, F[_], FUS[_], A, Wrapper]
    extends VerbWrapper[Verb, F, FUS, A, Wrapper] {
  def whenVerb(verb: Verb, testFun: => Unit)(implicit pos: source.Position): Unit
}

trait VerbWrapper1[Verb, F[_], A, Wrapper] extends VerbWrapper[Verb, F, F, A, Wrapper] {
  override final def FUStoF(testFun: => F[A])(implicit pos: Position): F[A] = testFun
}

trait RichVerbWrapper1[Verb, F[_], A, Wrapper]
    extends VerbWrapper1[Verb, F, A, Wrapper]
    with RichVerbWrapper[Verb, F, F, A, Wrapper]

/** Adds utilities for ignoring tests based on a predicate in FixtureAnyWordSpec tests. For example:
  * {{{
  * class MyTest extends CommunityIntegrationTest with ProtocolVersionChecksFixtureAnyWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in { implicit env =>
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * @see
  *   com.digitalasset.canton.BaseTest.testedProtocolVersion on how to set the protocol version for
  *   tests at runtime.
  */
trait TestPredicateFiltersFixtureAnyWordSpec {
  this: TestEssentials & FixtureAnyWordSpecLike =>

  private type F[A] = FixtureParam => A
  private type Wrapper = OnlyRunWhenWordSpecStringWrapper[F, F, Any]
  private type RichWrapper = RichOnlyRunWhenWordSpecStringWrapper[F, F, Any]

  // Reason is not used but forces us to identify the reason which can be linked to a T O D O
  @nowarn("cat=unused")
  def onlyLocalParty(reason: UnsupportedExternalPartyTest): Boolean =
    partiesKind == PartyKind.Local

  private object FixtureAnyWordSpecLikeVerbWrapperString
      extends RichVerbWrapper1[WordSpecStringWrapper, F, Any, RichWrapper] {
    override def inVerb(verb: WordSpecStringWrapper, testFun: => F[Any])(implicit
        pos: Position
    ): Unit =
      verb.in(testFun)

    override def ignoreVerb(verb: WordSpecStringWrapper, testFun: => F[Any])(implicit
        pos: Position
    ): Unit = verb.ignore(testFun)

    override def whenVerb(verb: WordSpecStringWrapper, testFun: => Unit)(implicit
        pos: Position
    ): Unit = verb.when(testFun)

    override def wrapper(verb: WordSpecStringWrapper, condition: => Boolean): RichWrapper =
      RichOnlyRunWhenWordSpecStringWrapper(this, verb, condition)
  }

  implicit def protocolVersionPartyCheckString(
      verb: String
  ): ProtocolVersionCheckString[
    FixtureParam => *,
    FixtureParam => *,
    Any,
    RichOnlyRunWhenWordSpecStringWrapper[FixtureParam => *, FixtureParam => *, Any],
  ] =
    ProtocolVersionCheckString(FixtureAnyWordSpecLikeVerbWrapperString)(
      verb,
      testedProtocolVersion,
      partiesKind,
    )

  private object FixtureAnyWordSpecLikeVerbWrapperTagged
      extends VerbWrapper1[ResultOfTaggedAsInvocationOnString, F, Any, Wrapper] {
    override def inVerb(verb: ResultOfTaggedAsInvocationOnString, testFun: => F[Any])(implicit
        pos: Position
    ): Unit = verb.in(testFun)

    override def ignoreVerb(
        verb: ResultOfTaggedAsInvocationOnString,
        testFun: => F[Any],
    )(implicit pos: Position): Unit = verb.ignore(testFun)

    override def wrapper(verb: ResultOfTaggedAsInvocationOnString, condition: => Boolean): Wrapper =
      OnlyRunWhenWordSpecStringWrapper(this, verb, condition)
  }

  implicit def protocolVersionPartyCheckTagged(
      verb: ResultOfTaggedAsInvocationOnString
  ): ProtocolVersionCheckString[
    FixtureParam => *,
    FixtureParam => *,
    Any,
    OnlyRunWhenWordSpecStringWrapper[FixtureParam => *, FixtureParam => *, Any],
  ] =
    ProtocolVersionCheckString(FixtureAnyWordSpecLikeVerbWrapperTagged)(
      verb,
      testedProtocolVersion,
      partiesKind,
    )
}

/** Adds utilities for checking protocol versions in FixtureAsyncWordSpec tests. For example:
  * {{{
  * class MyTest extends FixtureAsyncWordSpec with ProtocolVersionChecksFixtureAsyncWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in { implicit env =>
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * @see
  *   com.digitalasset.canton.BaseTest.testedProtocolVersion on how to set the protocol version for
  *   tests at runtime.
  */
trait ProtocolVersionChecksFixtureAsyncWordSpec {
  this: TestEssentials & FixtureAsyncWordSpecLike =>

  private type F[A] = FixtureParam => Future[A]
  private type FUS[A] = FixtureParam => FutureUnlessShutdown[A]
  private type Wrapper = OnlyRunWhenWordSpecStringWrapper[F, FUS, Assertion]
  private type RichWrapper = RichOnlyRunWhenWordSpecStringWrapper[F, FUS, Assertion]

  private object FixtureAsyncWordSpecVerbWrapperString
      extends RichVerbWrapper[WordSpecStringWrapper, F, FUS, Assertion, RichWrapper] {
    override def inVerb(verb: WordSpecStringWrapper, testFun: => F[Assertion])(implicit
        pos: Position
    ): Unit = verb.in(testFun)

    override def ignoreVerb(
        verb: WordSpecStringWrapper,
        testFun: => F[Assertion],
    )(implicit pos: Position): Unit = verb.ignore(testFun)

    override def whenVerb(verb: WordSpecStringWrapper, testFun: => Unit)(implicit
        pos: Position
    ): Unit = verb.when(testFun)

    override def FUStoF(testFun: => FUS[Assertion])(implicit
        pos: Position
    ): F[Assertion] = fixture =>
      testFun(fixture).onShutdown(
        fail(s"Unexpected shutdown in protocol version checks inUS wrapper")
      )

    override def wrapper(verb: WordSpecStringWrapper, condition: => Boolean): RichWrapper =
      RichOnlyRunWhenWordSpecStringWrapper(this, verb, condition)
  }

  implicit def protocolVersionCheckString(
      verb: String
  ): ProtocolVersionCheckString[
    Lambda[x => FixtureParam => Future[x]],
    Lambda[x => FixtureParam => FutureUnlessShutdown[x]],
    Assertion,
    RichOnlyRunWhenWordSpecStringWrapper[
      Lambda[x => FixtureParam => Future[x]],
      Lambda[x => FixtureParam => FutureUnlessShutdown[x]],
      Assertion,
    ],
  ] =
    ProtocolVersionCheckString(FixtureAsyncWordSpecVerbWrapperString)(
      verb,
      testedProtocolVersion,
      partiesKind,
    )

  private object FixtureAsyncWordSpecVerbWrapperTaggedAs
      extends VerbWrapper[ResultOfTaggedAsInvocationOnString, F, FUS, Assertion, Wrapper] {
    override def inVerb(
        verb: ResultOfTaggedAsInvocationOnString,
        testFun: => F[Assertion],
    )(implicit pos: Position): Unit = verb.in(testFun)

    override def ignoreVerb(
        verb: ResultOfTaggedAsInvocationOnString,
        testFun: => F[Assertion],
    )(implicit pos: Position): Unit = verb.ignore(testFun)

    override def FUStoF(testFun: => FUS[Assertion])(implicit
        pos: Position
    ): F[Assertion] = fixture =>
      testFun(fixture).onShutdown(
        fail(s"Unexpected shutdown in protocol version checks inUS wrapper")
      )

    override def wrapper(verb: ResultOfTaggedAsInvocationOnString, condition: => Boolean): Wrapper =
      OnlyRunWhenWordSpecStringWrapper(this, verb, condition)
  }

  implicit def protocolVersionCheckTaggedAs(
      verb: ResultOfTaggedAsInvocationOnString
  ): ProtocolVersionCheckString[
    Lambda[x => FixtureParam => Future[x]],
    Lambda[x => FixtureParam => FutureUnlessShutdown[x]],
    Assertion,
    OnlyRunWhenWordSpecStringWrapper[
      Lambda[x => FixtureParam => Future[x]],
      Lambda[x => FixtureParam => FutureUnlessShutdown[x]],
      Assertion,
    ],
  ] =
    ProtocolVersionCheckString(FixtureAsyncWordSpecVerbWrapperTaggedAs)(
      verb,
      testedProtocolVersion,
      partiesKind,
    )
}

/** Adds utilities for checking protocol versions in AsyncWordSpec tests. For example:
  * {{{
  * class MyTest extends AsyncWordSpec with ProtocolVersionChecksAsyncWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in {
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     Future {
  *       testedProtocolVersion should be >= ProtocolVersion.v4
  *     }
  *   }
  * }
  * }}}
  *
  * @see
  *   com.digitalasset.canton.BaseTest.testedProtocolVersion on how to set the protocol version for
  *   tests at runtime.
  */
trait ProtocolVersionChecksAsyncWordSpec {
  this: TestEssentials & AsyncWordSpecLike =>

  private type Wrapper =
    RichOnlyRunWhenWordSpecStringWrapper[Future, FutureUnlessShutdown, Assertion]

  private object AsyncWordSpecLikeVerbWrapper
      extends RichVerbWrapper[
        WordSpecStringWrapper,
        Future,
        FutureUnlessShutdown,
        Assertion,
        Wrapper,
      ] {
    override def inVerb(verb: WordSpecStringWrapper, testFun: => Future[Assertion])(implicit
        pos: Position
    ): Unit =
      verb.in(testFun)

    override def ignoreVerb(verb: WordSpecStringWrapper, testFun: => Future[Assertion])(implicit
        pos: Position
    ): Unit =
      verb.ignore(testFun)

    override def whenVerb(verb: WordSpecStringWrapper, testFun: => Unit)(implicit
        pos: Position
    ): Unit = verb.when(testFun)

    override def FUStoF(
        testFun: => FutureUnlessShutdown[Assertion]
    )(implicit pos: Position): Future[Assertion] =
      testFun.onShutdown(
        fail(s"Unexpected shutdown in protocol version checks inUS wrapper")
      )

    override def wrapper(verb: WordSpecStringWrapper, condition: => Boolean): Wrapper =
      RichOnlyRunWhenWordSpecStringWrapper(this, verb, condition)
  }

  implicit def protocolVersionCheckString(
      verb: String
  ): ProtocolVersionCheckString[
    Future,
    FutureUnlessShutdown,
    Assertion,
    RichOnlyRunWhenWordSpecStringWrapper[Future, FutureUnlessShutdown, Assertion],
  ] =
    ProtocolVersionCheckString(AsyncWordSpecLikeVerbWrapper)(
      verb,
      testedProtocolVersion,
      partiesKind,
    )
}

/** Adds utilities for checking protocol versions in AnyWordSpec tests. For example:
  * {{{
  * class MyTest extends AnyWordSpec with ProtocolVersionChecksAnyWordSpec {
  *   "some feature" onlyRunWithOrGreaterThan ProtocolVersion.v4 in {
  *     // this test is run only if the protocol version is v4 or later.
  *     // otherwise scalatest reports this test as ignored.
  *     testedProtocolVersion should be >= ProtocolVersion.v4
  *   }
  * }
  * }}}
  *
  * @see
  *   com.digitalasset.canton.BaseTest.testedProtocolVersion on how to set the protocol version for
  *   tests at runtime.
  */
trait ProtocolVersionChecksAnyWordSpec {
  this: TestEssentials & AnyWordSpecLike =>

  type Wrapper = RichOnlyRunWhenWordSpecStringWrapper[Id, Id, Any]

  private object AnyWordSpecLikeVerbWrapper
      extends RichVerbWrapper1[WordSpecStringWrapper, Id, Any, Wrapper] {
    override def inVerb(verb: WordSpecStringWrapper, testFun: => Id[Any])(implicit
        pos: Position
    ): Unit =
      verb.in(testFun)

    override def ignoreVerb(verb: WordSpecStringWrapper, testFun: => Id[Any])(implicit
        pos: Position
    ): Unit =
      verb.ignore(testFun)

    override def whenVerb(verb: WordSpecStringWrapper, testFun: => Unit)(implicit
        pos: Position
    ): Unit =
      verb.when(testFun)

    override def wrapper(verb: WordSpecStringWrapper, condition: => Boolean): Wrapper =
      RichOnlyRunWhenWordSpecStringWrapper(this, verb, condition)
  }

  implicit def protocolCheckString(
      verb: String
  ): ProtocolVersionCheckString[Id, Id, Any, RichOnlyRunWhenWordSpecStringWrapper[Id, Id, Any]] =
    ProtocolVersionCheckString(AnyWordSpecLikeVerbWrapper)(verb, testedProtocolVersion, partiesKind)
}
