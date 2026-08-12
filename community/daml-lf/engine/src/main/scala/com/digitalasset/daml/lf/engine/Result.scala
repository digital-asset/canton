// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey, NeedKeyProgression}
import com.digitalasset.daml.lf.value.Value.*

object Result extends data.Freer.Companion {

  type F[A] = Need[A]
  type E = Error

  sealed abstract class Need[A] extends Product with Serializable

  object Need {

    /** Requests the package `packageId`. Resume with `Some(pkg)` if available, `None` otherwise. */
    final case class Package(packageId: PackageId) extends Need[Option[language.Ast.Package]]

    /** Requests the contract `coid`. Resume with [[Contract.Found]], [[Contract.NotFound]], or
      * [[Contract.UnsupportedIdVersion]].
      *
      * For [[Contract.Found]], the caller provides the fat contract instance previously associated
      * with `coid` by the engine, the `expectedHashingMethod` the engine must use, and an
      * `idValidator` that authenticates the contract from the engine-computed hash.
      */
    final case class Contract(coid: ContractId) extends Need[Contract.Response]
    object Contract {
      sealed trait Response
      final case class Found(
          contractInstance: FatContractInstance,
          expectedHashingMethod: Hash.HashingMethod,
          idValidator: Hash => Boolean,
      ) extends Response
      final case object NotFound extends Response
      final case object UnsupportedIdVersion extends Response
    }

    /** Requests contracts whose key matches `key`. Resume with a page of entries and a
      * [[transaction.NeedKeyProgression.HasStarted]] token:
      * [[transaction.NeedKeyProgression.Finished]] when done, or
      * [[transaction.NeedKeyProgression.InProgress]] to be called again with the token as
      * `continuationToken`. `limit` is a preferred page size; returning more is allowed and the
      * engine buffers the overflow.
      *
      * We look up only the key, not key + maintainers: indexing by maintainers could turn an
      * upgrade error into a negative lookup when an invalid upgrade changes the maintainers
      * expression.
      */
    final case class Key(
        key: GlobalKey,
        limit: Int,
        continuationToken: NeedKeyProgression.CanContinue,
    ) extends Need[Key.Response]

    object Key {

      object Response {

        /** An entry in a [[Response]] result: either an authenticable contract or an error. */
        sealed trait ContractEntry extends Product with Serializable

        /** A fat contract instance matching the key, with the `expectedHashingMethod` the engine
          * must use and an `idValidator` that authenticates it from the engine-computed hash.
          */
        final case class AuthenticableFatContractInstance(
            contractInstance: FatContractInstance,
            expectedHashingMethod: Hash.HashingMethod,
            idValidator: Hash => Boolean,
        ) extends ContractEntry

        /** Indicates that the contract ID uses an unsupported version. */
        final case class UnsupportedContractIdVersion(contractId: ContractId) extends ContractEntry
      }

      /** The response to the [[Key]] question: contract `entries` and a `hasStarted` token
        * ([[transaction.NeedKeyProgression.Finished]] when done, else
        * [[transaction.NeedKeyProgression.InProgress]]).
        */
      final case class Response(
          contracts: Vector[Response.ContractEntry],
          hasStarted: NeedKeyProgression.HasStarted,
      )
    }

    /** Requests an external-call result. Fields use canonical lowercase hex. Resume with
      * `Right(output)` (same encoding) on success or `Left(error)` on failure.
      */
    final case class ExternalCall(extId: String, funcId: String, configHash: String, input: String)
        extends Need[Either[ExternalCall.Error, String]]
    object ExternalCall {

      /** Error information from external call failures */
      final case class Error(message: String)
    }

    /** Hints that the interpretation will likely resolve these contract ids and keys. The caller
      * may prefetch them in parallel but need not. Each key maps to the max number of contracts to
      * prefetch for it.
      */
    final case class Prefetch(contractIds: Seq[ContractId], keys: Map[GlobalKey, Int])
        extends Need[Unit]

    final case class Interruption(abort: () => Option[String]) extends Need[Unit]
  }

  def done[A](x: A): Result[A] = pure[A](x)
  def error(err: Error): Result[Nothing] = raise(err)
  def error(packageError: Error.Package.Error): Result[Nothing] =
    error(Error.Package(packageError))
  def error(preprocessingError: Error.Preprocessing.Error): Result[Nothing] =
    error(Error.Preprocessing(preprocessingError))
  def error(
      interpretationError: Error.Interpretation.Error,
      details: Option[String] = scala.None,
  ): Result[Nothing] =
    error(Error.Interpretation(interpretationError, details))
  def error(validationError: Error.Validation.Error): Result[Nothing] =
    error(Error.Validation(validationError))

  private def need[X](n: Need[X]): Result[X] = {
    // Single-shot guard: the continuation potentially shares the mutable `interpreter`,
    // so a second resume fails fast rather than corrupt state.
    val used = new java.util.concurrent.atomic.AtomicBoolean(false)
    lift(n).flatMap { x =>
      if (used.compareAndSet(false, true)) done(x)
      else throw new IllegalStateException(s"Result $n continuation already resumed")
    }
  }

  private[lf] def needPackage(packageId: PackageId): Result[Option[language.Ast.Package]] =
    need(Need.Package(packageId))

  private[lf] def needContract(
      acoid: ContractId
  ): Result[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)] =
    need(Need.Contract(acoid)).flatMap {
      case Need.Contract.Found(contractInstance, expectedHashingMethod, authenticator) =>
        done((contractInstance, expectedHashingMethod, authenticator))
      case Need.Contract.NotFound =>
        error(Error.Interpretation.DamlException(interpretation.Error.ContractNotFound(acoid)))
      case Need.Contract.UnsupportedIdVersion =>
        error(Error.Interpretation.DamlException(interpretation.Error.UnsupportedContractId(acoid)))
    }

  def needKey(
      key: GlobalKey,
      limit: Int,
      continuationToken: NeedKeyProgression.CanContinue,
  ): Result[Need.Key.Response] =
    need(Need.Key(key, limit, continuationToken))

  def needExternalCall(
      extId: String,
      funcId: String,
      configHash: String,
      input: String,
  ): Result[Either[Need.ExternalCall.Error, String]] =
    need(Need.ExternalCall(extId, funcId, configHash, input))

  def needPrefetch(contractIds: Seq[ContractId], keys: Map[GlobalKey, Int]): Result[Unit] =
    need(Need.Prefetch(contractIds, keys))

  def needInterruption(abort: () => Option[String]): Result[Unit] =
    need(Need.Interruption(abort))

  /** Build a handler that answers each [[Need]] from the supplied lookups, for use with
    * [[data.Freer.consume]]. External calls cannot be serviced and short-circuit with an error.
    */
  def lookupHandler(
      pcs: PartialFunction[ContractId, FatContractInstance] = PartialFunction.empty,
      pkgs: PartialFunction[PackageId, language.Ast.Package] = PartialFunction.empty,
      keys: PartialFunction[GlobalKey, Vector[FatContractInstance]] = PartialFunction.empty,
      hashingMethod: ContractId => Hash.HashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
      idValidator: (ContractId, Hash) => Boolean = (_, _) => true,
  ): Handler =
    new Handler {
      def apply[X](need: Need[X]): Either[Error, X] =
        need match {
          case Need.Package(packageId) =>
            Right(pkgs.lift(packageId))
          case Need.Contract(acoid) =>
            Right(pcs.lift(acoid) match {
              case Some(coInst) =>
                Need.Contract.Found(coInst, hashingMethod(acoid), idValidator(acoid, _))
              case _ => Need.Contract.NotFound
            })
          case Need.Key(key, _, _) =>
            Right(
              Need.Key.Response(
                keys
                  .lift(key)
                  .getOrElse(Vector.empty)
                  .map(fci =>
                    Need.Key.Response.AuthenticableFatContractInstance(
                      fci,
                      hashingMethod(fci.contractId),
                      hash => idValidator(fci.contractId, hash),
                    )
                  ),
                NeedKeyProgression.Finished,
              )
            )
          case Need.Prefetch(_, _) =>
            Right(())
          case Need.Interruption(_) =>
            Right(())
          case Need.ExternalCall(extId, funcId, _, _) =>
            Left(
              Error.Interpretation(
                Error.Interpretation.Internal(
                  "Result.lookupHandler",
                  s"Result.lookupHandler cannot handle Need.ExternalCall " +
                    s"(extensionId=$extId, functionId=$funcId)",
                  scala.None,
                ),
                scala.None,
              )
            )
        }
    }

}
