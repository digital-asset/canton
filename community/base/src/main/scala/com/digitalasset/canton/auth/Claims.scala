// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import cats.syntax.either.*
import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.LfLedgerString
import com.digitalasset.canton.tea.AccountId
import com.digitalasset.canton.util.MonadUtil.syntax.IterableSyntax
import com.digitalasset.daml.lf.data.Ref

import java.time.{Duration, Instant}

/** A claim is a single statement about what an authenticated user can do with the ledger API.
  *
  * Note: this ADT is expected to evolve in the future by adding new cases for more fine grained
  * claims. The existing cases should be treated as immutable in order to guarantee backwards
  * compatibility for [[AuthService]] implementations.
  */
sealed abstract class Claim

/** Authorized to use all admin services. Does not authorize to use non-admin services.
  */
case object ClaimAdmin extends Claim

/** Authorized to act as any party, including:
  *   - Reading all data for all parties
  *   - Creating contract on behalf of any party
  *   - Exercising choices on behalf of any party
  */
case object ClaimActAsAnyParty extends Claim

/** Authorized to act as the given party, including:
  *   - Reading all data for the given party
  *   - Creating contracts on behalf of the given party
  *   - Exercising choices on behalf of the given party
  */
final case class ClaimActAsParty(name: Ref.Party) extends Claim

/** Authorized to execute commands as any party, including:
  *   - Preparing an interactive command on behalf of any party
  *   - Executing an interactive command on behalf of any party It does not entitle to read data for
  *     any parties.
  */
case object ClaimExecuteAsAnyParty extends Claim

/** Authorized to act as the given party, including:
  *   - Preparing an interactive command on behalf of the given party
  *   - Executing an interactive command on behalf of the given party It does not entitle to read
  *     data for any parties.
  */
final case class ClaimExecuteAsParty(name: Ref.Party) extends Claim

/** Authorized to read all data for the given party.
  *
  * Does NOT authorize to issue commands.
  */
final case class ClaimReadAsParty(name: Ref.Party) extends Claim

/** Authorized to read all data as any party on the participant.
  *
  * Does NOT authorize to issue commands.
  */
case object ClaimReadAsAnyParty extends Claim

/** Authorized to debit traffic from any account. Does not imply any party claim. */
case object ClaimDebitTrafficFromAnyAccount extends Claim

/** Authorized to debit traffic from the given account. Does not imply any party claim. */
final case class ClaimDebitTrafficFromAccount(accountId: AccountId) extends Claim

/** Claims that handle surface areas of Ledger API functionality.
  *
  * These always combine an Area (e.g. ``parties``) together with a Verb (e.g. ``read``). Verbs may
  * imply other verbs (e.g. ``write`` implies ``read``).
  *
  * In addition, ``ledger:read`` implies ``base:read``.
  */
final case class ClaimApiArea(area: ClaimApiArea.Area) extends Claim

object ClaimApiArea {
  trait Verb {
    def toString: String
  }

  sealed trait ReadWrite extends Verb

  object ReadWrite {
    case object Read extends ReadWrite { override def toString = "read" }
    case object Write extends ReadWrite { override def toString = "write" }
  }

  /** For cases where we support a widely scoped read and write, and additionally a finer-scoped
    * self-read.
    */
  sealed trait SelfReadWrite extends Verb

  object SelfReadWrite {
    case object Self extends SelfReadWrite { override def toString = "self" }
    case object Read extends SelfReadWrite { override def toString = "read" }
    case object Write extends SelfReadWrite { override def toString = "write" }
  }

  /* Some API areas currently only have Read permissions, in particular [[Base]].
   */
  case object Read extends Verb {
    override def toString = "read"
  }

  /* Some API areas currently only have Write permissions, because their Read permissions
   * are folded into Ledger(Read).
   */
  case object Write extends Verb {
    override def toString = "write"
  }

  sealed trait Area {
    def resource: String
    def verb: Verb

    /** List of implied other verbs for this API surface area. Note that this is only invoked once,
      * so it must contain the entire transitive closure.
      */
    def implies: List[Area] = Nil

    def toJwtScope: String = s"$resource:${verb.toString}"
  }

  /** Public info available from the Ledger API to all authenticated clients, such as version
    * metadata, and connection status.
    */
  final case class Base(verb: Read.type) extends Area {
    override def resource = "base"
  }

  sealed trait LedgerVerb extends Verb

  /** The ability to interact with the ledger. Except for when accessing public data like the vetted
    * packages on the participant, you will typically also need the permissions to operate as the
    * parties mentioned in the request.
    *
    *   - `ledger:read`: A broad permission that allows one to read ledger state, such as contracts,
    *     topology, and global state (e.g. ledger offset).
    *   - `ledger:act`: Allows taking actions that modify the ledger state, on behalf of parties
    *     controlled by the caller. Implies `ledger:read`.
    *   - `ledger:execute`: Allows submission for external keys. Does not imply `ledger:read`.
    */
  final case class Ledger(verb: LedgerVerb) extends Area {
    override def resource = "ledger"

    override def implies = verb match {
      case Ledger.Read => List(Base(Read))
      // Does not imply Read, just as CanExecuteAs does not imply CanReadAs.
      case Ledger.Execute => List()
      case Ledger.Act => List[Area](Base(Read), Ledger(Ledger.Read), Ledger(Ledger.Execute))
    }
  }

  object Ledger {
    case object Read extends LedgerVerb { override def toString = "read" }
    case object Execute extends LedgerVerb { override def toString = "execute" }
    case object Act extends LedgerVerb { override def toString = "act" }
  }

  /** The ability to upload DAML packages to the participant. */
  final case class Packages(verb: Write.type) extends Area {
    override def resource = "packages"
  }

  /** The ability to change participant-wide vetting state. */
  final case class Vetting(verb: Write.type) extends Area {
    override def resource = "vetting"
  }

  sealed trait PartiesVerb extends Verb

  /** The ability to interact with allocated parties.
    *
    *   - `parties:allocate`: self-allocate parties.
    *   - `parties:self`: self-read, implicitly granted to any user by default.
    *   - `parties:read`: list and query parties within the IDP.
    *   - `parties:write`: create, update and delete parties within the IDP.
    *
    * Note that a user is always able to read their own parties.
    *
    * To read or update parties accross IDPs, you also need either `idps:read` or `idps:write`.
    */
  final case class Parties(verb: PartiesVerb) extends Area {
    override def resource = "parties"

    override def implies = verb match {
      case Parties.Self => List()
      case Parties.Allocate => List(Parties(Parties.Self))
      case Parties.Read => List(Parties(Parties.Self))
      case Parties.Write => List(Parties(Parties.Read), Parties(Parties.Self))
    }
  }

  object Parties {
    case object Self extends PartiesVerb { override def toString = "self" }
    case object Allocate extends PartiesVerb { override def toString = "allocate" }
    case object Read extends PartiesVerb { override def toString = "read" }
    case object Write extends PartiesVerb { override def toString = "write" }
  }

  /** The ability to interact with users.
    *
    *   - `users:self`: query the caller's own user.
    *   - `users:read`: list and query users within the IDP.
    *   - `users:write`: create, update and delete users within the IDP.
    *
    * To read or update users accross IDPs, you also need either `idps:read` or `idps:write`.
    */
  final case class Users(verb: SelfReadWrite) extends Area {
    override def resource = "users"

    override def implies = verb match {
      case SelfReadWrite.Self => List()
      case SelfReadWrite.Read => List(Users(SelfReadWrite.Self))
      case SelfReadWrite.Write => List(Users(SelfReadWrite.Self), Users(SelfReadWrite.Read))
    }
  }

  /** The ability to act accross IDPs.
    *
    *   - `idps:read`: list IDP configurations, and read data from other IDPs.
    *   - `idps:write`: update, create and delete IDP configurations, and make modifications to
    *     resources in other IDPs.
    */
  final case class Idps(verb: ReadWrite) extends Area {
    override def resource = "idps"

    override def implies = verb match {
      case ReadWrite.Read => List()
      case ReadWrite.Write => List(Idps(ReadWrite.Read))
    }
  }

  /** The ability to change ledger time.  Only used in tests. */
  final case class Time(verb: Write.type) extends Area {
    override def resource = "time"
  }

  /** The ability to interact with traffic balances. Note that you also need a corresponding
    * [[ClaimDebitTrafficFromAccount]] or [[ClaimDebitTrafficFromAnyAccount]] for the account.
    *
    *   - `traffic:self`: retrieve your own account balance.
    *   - `traffic:read`: retrieve account balances on the participant node.
    *   - `traffic:write`: an admin-like permission to write traffic balances or prune state.
    */
  final case class Traffic(verb: SelfReadWrite) extends Area {
    override def resource = "traffic"

    override def implies = verb match {
      case SelfReadWrite.Self => List()
      case SelfReadWrite.Read => List(Traffic(SelfReadWrite.Self))
      case SelfReadWrite.Write => List(Traffic(SelfReadWrite.Self), Traffic(SelfReadWrite.Read))
    }
  }

  /** The default list of Ledger API claims for an admin, which is literally all of them. */
  def admin: List[Claim] = List(
    ClaimApiArea(Base(Read)),
    ClaimApiArea(Ledger(Ledger.Act)),
    ClaimApiArea(Packages(Write)),
    ClaimApiArea(Vetting(Write)),
    ClaimApiArea(Parties(Parties.Allocate)),
    ClaimApiArea(Parties(Parties.Write)),
    ClaimApiArea(Users(SelfReadWrite.Write)),
    ClaimApiArea(Idps(ReadWrite.Write)),
    ClaimApiArea(Time(Write)),
    ClaimApiArea(Traffic(SelfReadWrite.Write)),
  )

  /** The default list of Ledger API claims for an IDP admin, backwards-compatible with before we
    * had the concept of Ledger API claims.
    */
  def idpAdmin: List[Claim] = List(
    ClaimApiArea(Base(Read)),
    ClaimApiArea(Ledger(Ledger.Act)),
    ClaimApiArea(Parties(Parties.Allocate)),
    ClaimApiArea(Parties(Parties.Write)),
    ClaimApiArea(Users(SelfReadWrite.Write)),
    ClaimApiArea(Traffic(SelfReadWrite.Self)),
  )

  /** The default list of Ledger API claims for a user, backwards-compatible with before we had the
    * concept of Ledger API claims.
    */
  def user = List[Claim](
    ClaimApiArea(Base(Read)),
    ClaimApiArea(Ledger(Ledger.Act)),
    ClaimApiArea(Parties(Parties.Allocate)),
    ClaimApiArea(Users(SelfReadWrite.Self)),
    ClaimApiArea(Traffic(SelfReadWrite.Self)),
  )

  // Shorthands
  def baseRead = ClaimApiArea(Base(Read))
  def ledgerRead = ClaimApiArea(Ledger(Ledger.Read))
  def ledgerExecute = ClaimApiArea(Ledger(Ledger.Execute))
  def ledgerAct = ClaimApiArea(Ledger(Ledger.Act))
  def packagesWrite = ClaimApiArea(Packages(Write))
  def vettingWrite = ClaimApiArea(Vetting(Write))
  def partiesSelf = ClaimApiArea(Parties(Parties.Self))
  def partiesAllocate = ClaimApiArea(Parties(Parties.Allocate))
  def partiesRead = ClaimApiArea(Parties(Parties.Read))
  def partiesWrite = ClaimApiArea(Parties(Parties.Write))
  def usersSelf = ClaimApiArea(Users(SelfReadWrite.Self))
  def usersRead = ClaimApiArea(Users(SelfReadWrite.Read))
  def usersWrite = ClaimApiArea(Users(SelfReadWrite.Write))
  def idpsRead = ClaimApiArea(Idps(ReadWrite.Read))
  def idpsWrite = ClaimApiArea(Idps(ReadWrite.Write))
  def timeWrite = ClaimApiArea(Time(Write))
  def trafficSelf = ClaimApiArea(Traffic(SelfReadWrite.Self))
  def trafficRead = ClaimApiArea(Traffic(SelfReadWrite.Read))
  def trafficWrite = ClaimApiArea(Traffic(SelfReadWrite.Write))
}

object Claim {

  /** Parses the scope from A JWT, which contains a space-separated list of claims. Note that this
    * ignores unknown claims, since those could be intended for other applications, or future
    * versions of Canton. Sharing JWTs accross applications is frowned upon but not illegal.
    */
  def parseJwtScope(scope: String): Seq[Claim] =
    scope.split(" ").toSeq.flatMap(parseClaim)

  private def parseClaim(str: String): Option[Claim] =
    str.split(":", 2) match {
      case Array("base", vs) =>
        parseRead(vs).map(v => ClaimApiArea(ClaimApiArea.Base(v)))
      case Array("ledger", vs) =>
        parseLedgerVerb(vs).map(v => ClaimApiArea(ClaimApiArea.Ledger(v)))
      case Array("packages", vs) =>
        parseWrite(vs).map(v => ClaimApiArea(ClaimApiArea.Packages(v)))
      case Array("vetting", vs) =>
        parseWrite(vs).map(v => ClaimApiArea(ClaimApiArea.Vetting(v)))
      case Array("parties", vs) =>
        parsePartiesVerb(vs).map(v => ClaimApiArea(ClaimApiArea.Parties(v)))
      case Array("users", vs) =>
        parseSelfReadWrite(vs).map(v => ClaimApiArea(ClaimApiArea.Users(v)))
      case Array("idps", vs) =>
        parseReadWrite(vs).map(v => ClaimApiArea(ClaimApiArea.Idps(v)))
      case Array("time", vs) =>
        parseWrite(vs).map(v => ClaimApiArea(ClaimApiArea.Time(v)))
      case Array("traffic", vs) =>
        parseSelfReadWrite(vs).map(v => ClaimApiArea(ClaimApiArea.Traffic(v)))
      case _ => None
    }

  private def parseReadWrite(str: String): Option[ClaimApiArea.ReadWrite] =
    str match {
      case "read" => Some(ClaimApiArea.ReadWrite.Read)
      case "write" => Some(ClaimApiArea.ReadWrite.Write)
      case _ => None
    }

  private def parseSelfReadWrite(str: String): Option[ClaimApiArea.SelfReadWrite] =
    str match {
      case "self" => Some(ClaimApiArea.SelfReadWrite.Self)
      case "read" => Some(ClaimApiArea.SelfReadWrite.Read)
      case "write" => Some(ClaimApiArea.SelfReadWrite.Write)
      case _ => None
    }

  private def parseRead(str: String): Option[ClaimApiArea.Read.type] =
    str match {
      case "read" => Some(ClaimApiArea.Read)
      case _ => None
    }

  private def parseWrite(str: String): Option[ClaimApiArea.Write.type] =
    str match {
      case "write" => Some(ClaimApiArea.Write)
      case _ => None
    }

  private def parseLedgerVerb(str: String): Option[ClaimApiArea.LedgerVerb] =
    str match {
      case "read" => Some(ClaimApiArea.Ledger.Read)
      case "execute" => Some(ClaimApiArea.Ledger.Execute)
      case "act" => Some(ClaimApiArea.Ledger.Act)
      case _ => None
    }

  private def parsePartiesVerb(str: String): Option[ClaimApiArea.PartiesVerb] =
    str match {
      case "allocate" => Some(ClaimApiArea.Parties.Allocate)
      case "self" => Some(ClaimApiArea.Parties.Self)
      case "read" => Some(ClaimApiArea.Parties.Read)
      case "write" => Some(ClaimApiArea.Parties.Write)
      case _ => None
    }

  /** Given an initial list of claims (typically associated with a user), restrict this to only a
    * subset of those claims. This will throw an an error if the restriction is not a subset of the
    * initial claim, as that could indicate someone is trying to obtain more permissions than they
    * should.
    */
  def restrict(
      assigned: Seq[Claim],
      restrictions: Seq[Claim],
  ): Either[AuthorizationError, Seq[Claim]] = {
    val areaRestrictions = restrictions.collect { case ClaimApiArea(area) => area }
    if (areaRestrictions.isEmpty) {
      Right(assigned) // Unrestricted
    } else {
      // Create a set of the assigned areas.
      val assignedAreas = assigned
        .foldLeft(Set.empty[ClaimApiArea.Area]) {
          case (acc, ClaimApiArea(area)) => acc.union((area :: area.implies).toSet)
          case (acc, _) => acc
        }

      areaRestrictions
        .sequentialTraverse_ { area =>
          if (assignedAreas.contains(area))
            Either.right[AuthorizationError, Unit](())
          else
            Either.left[AuthorizationError, Unit](AuthorizationError.InvalidScopeRestriction(area))
        }
        .map { _ =>
          // Replace all API area claims from the assigned set by the newly
          // restricted ones.
          areaRestrictions.map(ClaimApiArea(_)) ++
            assigned.filter {
              case ClaimApiArea(_) => false
              case ClaimAdmin => false // Remove ClaimAdmin if there are any restrictions
              case _ => true
            }
        }
    }
  }
}

sealed trait ClaimSet

object ClaimSet {

  val DefaultIdentityProviderId: String = ""

  object Unauthenticated extends ClaimSet

  /** [[Claims]] define what actions an authenticated user can perform on the Ledger API.
    *
    * They also optionally specify an expiration epoch time that statically specifies the time on or
    * after which the token will no longer be considered valid by the Ledger API.
    *
    * The precise authorization rules are documented in "//docs/source/app-dev/authorization.rst".
    * Please use that file when writing or reviewing tests; and keep it up to date when adding new
    * endpoints.
    *
    * @param claims
    *   List of [[Claim]]s describing the authorization this object describes.
    * @param participantId
    *   If set, the claims will only be valid on the given participant identifier.
    * @param userId
    *   If set, the claims will only be valid on the given user identifier.
    * @param expiration
    *   If set, the claims will cease to be valid at the given time.
    * @param resolvedFromUser
    *   If set, then the claims were resolved from a user in the user management service.
    * @param identityProviderId
    *   If set, the claims will only be valid on the given Identity Provider configuration.
    * @param appliedRestriction
    *   The restriction that was applied to these claims. Note that these restrictions have already
    *   been applied to the [[claims]] field. We additionally carry them here in case we need to
    *   recompute the claims, as is the case for ongoing stream authorization.
    */
  final case class Claims(
      claims: Seq[Claim],
      participantId: Option[String],
      userId: Option[String],
      expiration: Option[Instant],
      identityProviderId: Option[LfLedgerString],
      resolvedFromUser: Boolean,
      appliedRestriction: Seq[Claim] = Seq.empty,
  ) extends ClaimSet {

    def validForParticipant(id: String): Either[AuthorizationError, Unit] =
      participantId match {
        case Some(p) if p != id => Left(AuthorizationError.InvalidParticipant(p, id))
        case _ => Right(())
      }

    def validForUser(id: String): Either[AuthorizationError, Unit] =
      userId match {
        case Some(a) if a != id => Left(AuthorizationError.InvalidUser(a, id))
        case _ => Right(())
      }

    /** Returns false if the expiration timestamp exists and is greater than or equal to the current
      * time
      */
    def notExpired(
        now: Instant,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        tokenExpiryGracePeriodForStreams: Option[Duration] = None,
    ): Either[AuthorizationError, Unit] = {
      val subtrahends =
        jwtTimestampLeeway
          .flatMap(l => l.expiresAt.orElse(l.default))
          .map(Duration.ofSeconds)
          .toList ++
          tokenExpiryGracePeriodForStreams.toList
      val relaxedNow = subtrahends.foldLeft(now)((acc, subtrahend) => acc.minus(subtrahend))
      expiration match {
        case Some(e) if !relaxedNow.isBefore(e) => Left(AuthorizationError.Expired(e, now))
        case _ => Right(())
      }
    }

    /** Returns true if the set of claims authorizes the user to use admin services, unless the
      * claims expired
      */
    def isAdmin: Either[AuthorizationError, Unit] =
      Either.cond(claims.contains(ClaimAdmin), (), AuthorizationError.MissingAdminClaim)

    /** Returns true if the set of claims authorizes the user to act as the given party, unless the
      * claims expired
      */
    def canActAs(party: String): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimActAsParty(p) if p == party => true
          case _ => false
        },
        (),
        AuthorizationError.MissingActClaim(party),
      )

    /** Returns true if the set of claims authorizes the user to read data for the given party,
      * unless the claims expired
      */
    def canReadAs(party: String): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimReadAsAnyParty => true
          case ClaimActAsParty(p) if p == party => true
          case ClaimReadAsParty(p) if p == party => true
          case _ => false
        },
        (),
        AuthorizationError.MissingReadClaim(party),
      )

    /** Returns true if the set of claims authorizes the user to read data as any party, unless the
      * claims expired
      */
    def canReadAsAnyParty: Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimReadAsAnyParty => true
          case _ => false
        },
        (),
        AuthorizationError.MissingReadAsAnyPartyClaim,
      )

    /** Returns true if the set of claims authorizes the user to execute interactive commands for
      * the given party, unless the claims expired.
      */
    def canExecuteAs(party: String): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimExecuteAsAnyParty => true
          case ClaimActAsParty(p) if p == party => true
          case ClaimExecuteAsParty(p) if p == party => true
          case _ => false
        },
        (),
        AuthorizationError.MissingExecuteClaim(party),
      )

    /** Returns true if the set of claims authorizes the user to debit traffic from the given
      * account, unless the claims expired. With `acceptExecuteRights`, the right to execute as a
      * party with the same id also authorizes it.
      */
    def canDebitTrafficFrom(
        accountId: String,
        acceptExecuteRights: Boolean,
    ): Either[AuthorizationError, Unit] = {
      val hasDebitClaim = claims.exists {
        case ClaimDebitTrafficFromAnyAccount => true
        case ClaimDebitTrafficFromAccount(a) if a.unwrap == accountId => true
        case _ => false
      }
      Either.cond(
        hasDebitClaim || (acceptExecuteRights && canExecuteAs(accountId).isRight),
        (),
        AuthorizationError.MissingDebitClaim(accountId),
      )
    }

    /** Returns true if the set of claims authorize the user to list local traffic accounts on the
      * participant. The user needs Traffic(Read) rights, or CanDebitTrafficFromAnyAccount rights,
      * or additionally, when acceptExecuteRights is true, also CanExecuteAsAnyParty or
      * CanActAsAnyParty are supported.
      */
    def canListTrafficAccounts(acceptExecuteRights: Boolean): Either[AuthorizationError, Unit] =
      canAccessApiArea(ClaimApiArea.trafficRead.area) match {
        case Right(_) => Right(())
        case Left(_) =>
          Either.cond(
            claims.contains(ClaimDebitTrafficFromAnyAccount)
              || (acceptExecuteRights && canExecuteAsAnyParty.isRight),
            (),
            AuthorizationError.MissingListAccountsClaims,
          )
      }

    /** Returns true if the set of claims authorizes the user to execute interactive commands as any
      * party, unless the claims expired.
      */
    def canExecuteAsAnyParty: Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimExecuteAsAnyParty => true
          case _ => false
        },
        (),
        AuthorizationError.MissingExecuteAsAnyPartyClaim,
      )

    /** Returns true if the set of claims authorizes the user to read data for the given party,
      * unless the claims expired
      */
    def canOperateAs(party: String): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimReadAsAnyParty => true
          case ClaimExecuteAsAnyParty => true
          case ClaimActAsParty(p) if p == party => true
          case ClaimReadAsParty(p) if p == party => true
          case ClaimExecuteAsParty(p) if p == party => true
          case _ => false
        },
        (),
        AuthorizationError.MissingOperationClaim(party),
      )

    /** Asserts that the claims authorize access to the specified group of Ledger API methods. */
    def canAccessApiArea(want: ClaimApiArea.Area): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimApiArea(allowedArea) =>
            allowedArea == want || allowedArea.implies.contains(want)
          case _ => false
        },
        (),
        AuthorizationError.MissingLedgerApiClaim(want),
      )
  }

  /** The representation of a user that was authenticated, but whose [[Claims]] have not yet been
    * resolved.
    *
    * @param restriction
    *   An explicit of restrictions set on the token. The user rights should restricted to only
    *   those mentioned here.
    */
  final case class AuthenticatedUser(
      identityProviderId: Option[LfLedgerString],
      userId: String,
      participantId: Option[String],
      expiration: Option[Instant],
      restriction: Seq[Claim],
  ) extends ClaimSet

  object Claims {

    /** A set of [[Claims]] that does not have any authorization */
    val Empty: Claims = Claims(
      claims = List.empty[Claim],
      participantId = None,
      userId = None,
      expiration = None,
      resolvedFromUser = false,
      identityProviderId = None,
    )

    /** A set of [[Claims]] that has all possible authorizations */
    val Wildcard: Claims =
      Empty.copy(claims = List[Claim](ClaimAdmin, ClaimActAsAnyParty) ++ ClaimApiArea.admin)

    /** A set of [[Claims]] that has all admin authorizations but doesn't authorize to act as
      * parties
      */
    val Admin: Claims =
      Empty.copy(claims = ClaimAdmin :: ClaimApiArea.admin)

  }

}
